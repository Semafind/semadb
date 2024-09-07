package shard

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index"
	"github.com/semafind/semadb/utils"
	"github.com/vmihailenco/msgpack/v5"
)

type Shard struct {
	dbFile     string
	db         diskstore.DiskStore
	collection models.Collection
	// ---------------------------
	cacheManager *cache.Manager
	logger       zerolog.Logger
}

// ---------------------------
/* Points store the actual data points, graphIndex stores the similarity graph
 * and internal stores the shard metadata such as point count. We partition like
 * this because graph traversal is read-heavy operation, if everything is bundled
 * together, the disk cache pulls in more pages. It's also logically easier to
 * manage. */
const POINTSBUCKETKEY = "points"
const INTERNALBUCKETKEY = "internal"

// ---------------------------
// Internal bucket keys
var POINTCOUNTKEY = []byte("pointCount")
var FREENODEIDSKEY = []byte("freeNodeIds")
var NEXTFREENODEIDKEY = []byte("nextFreeNodeId")

// ---------------------------
const DELETEVALUE = "_delete"

// ---------------------------

func NewShard(dbFile string, collection models.Collection, cacheManager *cache.Manager) (*Shard, error) {
	// ---------------------------
	db, err := diskstore.Open(dbFile)
	if err != nil {
		return nil, fmt.Errorf("could not open shard db: %w", err)
	}
	// ---------------------------
	if cacheManager == nil {
		// 0 means no cache, every operation will get blank cache and discard it
		cacheManager = cache.NewManager(0)
	}
	// ---------------------------
	shard := &Shard{
		dbFile:       dbFile, // An alternative could be db.Path()
		db:           db,
		collection:   collection,
		cacheManager: cacheManager,
		logger:       log.With().Str("component", "shard").Str("name", dbFile).Logger(),
	}
	return shard, nil
}

func (s *Shard) Close() error {
	s.cacheManager.Release(s.dbFile)
	return s.db.Close()
}

func (s *Shard) Backup(backupFrequency, backupCount int) error {
	return utils.BackupBBolt(s.db, backupFrequency, backupCount)
}

// ---------------------------

func changePointCount(bucket diskstore.Bucket, change int) error {
	// ---------------------------
	countBytes := bucket.Get(POINTCOUNTKEY)
	var count uint64
	if countBytes != nil {
		count = conversion.BytesToUint64(countBytes)
	}
	// ---------------------------
	newCount := int(count) + change
	if newCount < 0 {
		return fmt.Errorf("point count cannot be negative")
	}
	// ---------------------------
	countBytes = conversion.Uint64ToBytes(uint64(newCount))
	if err := bucket.Put(POINTCOUNTKEY, countBytes); err != nil {
		return fmt.Errorf("could not change point count: %w", err)
	}
	return nil
}

type shardInfo struct {
	PointCount uint64
	Size       int64 // Size of the shard database file
}

func (s *Shard) Info() (si shardInfo, err error) {
	// ---------------------------
	dbSize, err := s.db.SizeInBytes()
	if err != nil {
		return
	}
	si.Size = dbSize
	// ---------------------------
	err = s.db.Read(func(bm diskstore.BucketManager) error {
		b, err := bm.Get(INTERNALBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not read internal bucket: %w", err)
		}
		// ---------------------------
		// The reason we use a point count is because a single point has
		// multiple key value pairs in the points bucket. This is easier to
		// manage than counting the number of keys in the points bucket which
		// may change over time.
		countBytes := b.Get(POINTCOUNTKEY)
		if countBytes != nil {
			si.PointCount = conversion.BytesToUint64(countBytes)
		}
		// ---------------------------
		return nil
	})
	return
}

// ---------------------------

func (s *Shard) InsertPoints(points []models.Point) error {
	// ---------------------------
	s.logger.Debug().Int("count", len(points)).Msg("InsertPoints")
	// ---------------------------
	// Check for duplicate ids
	ids := make(map[uuid.UUID]struct{}, len(points))
	for _, point := range points {
		if _, ok := ids[point.Id]; ok {
			return fmt.Errorf("duplicate point id: %s", point.Id.String())
		}
		ids[point.Id] = struct{}{}
	}
	// ---------------------------
	// Insert points
	// Remember, Bolt allows only one read-write transaction at a time
	var txTime time.Time
	cacheTx := s.cacheManager.NewTransaction()
	err := s.db.Write(func(bm diskstore.BucketManager) error {
		bPoints, err := bm.Get(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not write points bucket: %w", err)
		}
		bInternal, err := bm.Get(INTERNALBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not write internal bucket: %w", err)
		}
		// ---------------------------
		nodeCounter, err := NewIdCounter(bInternal, FREENODEIDSKEY, NEXTFREENODEIDKEY)
		if err != nil {
			return fmt.Errorf("could not create id counter: %w", err)
		}
		// ---------------------------
		// Kick off index dispatcher
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// ---------------------------
		pointsQ := utils.ProduceWithContext(ctx, points)
		indexQ, indexQErrC := utils.TransformWithContext(ctx, pointsQ, func(point models.Point) (ipc index.IndexPointChange, skip bool, err error) {
			// ---------------------------
			/* If the point exists, we can't re-insert it. This is actually an
			 * error because the edges will be wrong in the graph. It needs to be
			 * updated instead. We can potentially do it here (do an update
			 * instead of insert) but the API design migh be inconsistent as it
			 * will then depend whether a point is re-assigned to the same shard
			 * during insertion when there are multiple shards. We are returning
			 * an error here to force the user to update the point instead which
			 * handles the multiple shard case. */
			var exists bool
			if exists, err = CheckPointExists(bPoints, point.Id); err != nil {
				err = fmt.Errorf("could not check point existence: %w", err)
				return
			}
			if exists {
				err = fmt.Errorf("point already exists: %s", point.Id.String())
				return
			}
			sp := ShardPoint{Point: point, NodeId: nodeCounter.NextId()}
			if err = SetPoint(bPoints, sp); err != nil {
				err = fmt.Errorf("could not set point: %w", err)
				return
			}
			ipc.NodeId = sp.NodeId
			ipc.NewData = point.Data
			return
		})
		im := index.NewIndexManager(bm, cacheTx, s.dbFile, s.collection.IndexSchema)
		dispatchErrC := im.Dispatch(ctx, indexQ)
		// ---------------------------
		mergedErrC := utils.MergeErrorsWithContext(ctx, indexQErrC, dispatchErrC)
		// At this point concurrent stuff is over, we can check for errors
		if err := <-mergedErrC; err != nil {
			return fmt.Errorf("could not complete insert: %w", err)
		}
		// ---------------------------
		// Update point count accordingly
		if err := changePointCount(bInternal, len(points)); err != nil {
			return fmt.Errorf("could not update point count for insertion: %w", err)
		}
		// ---------------------------
		if err := nodeCounter.Flush(); err != nil {
			return fmt.Errorf("could not flush id counter: %w", err)
		}
		txTime = time.Now()
		return nil
	})
	s.logger.Debug().Str("duration", time.Since(txTime).String()).Msg("InsertPoints - Transaction Done")
	if err != nil {
		cacheTx.Commit(true)
		s.logger.Error().Err(err).Msg("could not insert points")
		return fmt.Errorf("could not insert points: %w", err)
	}
	cacheTx.Commit(false)
	// ---------------------------
	return nil
}

// ---------------------------

func (s *Shard) UpdatePoints(points []models.Point) ([]uuid.UUID, error) {
	s.logger.Debug().Int("count", len(points)).Msg("UpdatePoints")
	// ---------------------------
	// Note that some points may not exist, so we need to take care of that
	// throughout this function
	updatedIds := make([]uuid.UUID, 0, len(points))
	// ---------------------------
	cacheTx := s.cacheManager.NewTransaction()
	err := s.db.Write(func(bm diskstore.BucketManager) error {
		pointsBucket, err := bm.Get(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get write points bucket: %w", err)
		}
		// ---------------------------
		// Kick off index dispatcher
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// ---------------------------
		pointsQ := utils.ProduceWithContext(ctx, points)
		indexQ, indexQErrC := utils.TransformWithContext(ctx, pointsQ, func(point models.Point) (ipc index.IndexPointChange, skip bool, err error) {
			// ---------------------------
			sp, err := GetPointByUUID(pointsBucket, point.Id)
			if err == ErrPointDoesNotExist {
				// Point does not exist, we can skip it, it may reside in
				// another shard. Updating non-existing points is a no-op.
				skip = true
				return
			}
			if err != nil {
				err = fmt.Errorf("could not get point by id: %w", err)
				return
			}
			// ---------------------------
			// Merge data on update
			var existingData models.PointAsMap
			var incomingData models.PointAsMap
			if err = msgpack.Unmarshal(sp.Data, &existingData); err != nil {
				err = fmt.Errorf("could not unmarshal old data: %w", err)
				return
			}
			if err = msgpack.Unmarshal(point.Data, &incomingData); err != nil {
				err = fmt.Errorf("could not unmarshal new data: %w", err)
				return
			}
			for k, v := range incomingData {
				if vs, ok := v.(string); ok && vs == DELETEVALUE {
					delete(existingData, k)
				} else {
					existingData[k] = v
				}
			}
			finalNewData, err := msgpack.Marshal(existingData)
			if err != nil {
				err = fmt.Errorf("could not marshal final new data: %w", err)
				return
			}
			// ---------------------------
			// Check if the user is making a point too large
			if len(finalNewData) > s.collection.UserPlan.MaxPointSize {
				err = fmt.Errorf("point size exceeds limit: %d", s.collection.UserPlan.MaxPointSize)
				return
			}
			// ---------------------------
			point.Data = finalNewData
			if err = SetPoint(pointsBucket, ShardPoint{Point: point, NodeId: sp.NodeId}); err != nil {
				err = fmt.Errorf("could not set updated point: %w", err)
				return
			}
			ipc.NodeId = sp.NodeId
			ipc.PreviousData = sp.Data
			ipc.NewData = finalNewData
			// ---------------------------
			updatedIds = append(updatedIds, point.Id)
			// ---------------------------
			return
		})
		im := index.NewIndexManager(bm, cacheTx, s.dbFile, s.collection.IndexSchema)
		dispatchErrC := im.Dispatch(ctx, indexQ)
		// ---------------------------
		mergedErrC := utils.MergeErrorsWithContext(ctx, indexQErrC, dispatchErrC)
		// At this point concurrent stuff is over, we can check for errors
		if err := <-mergedErrC; err != nil {
			return fmt.Errorf("could not complete update: %w", err)
		}
		return nil
	})
	if err != nil {
		cacheTx.Commit(true)
		s.logger.Debug().Err(err).Msg("could not update points")
		return nil, fmt.Errorf("could not update points: %w", err)
	}
	cacheTx.Commit(false)
	// ---------------------------
	return updatedIds, nil
}

// ---------------------------

func (s *Shard) SearchPoints(searchRequest models.SearchRequest) ([]models.SearchResult, error) {
	// ---------------------------
	/* rSet contains all the points to return, results contains any ordered
	 * search results. For example a basic integer equals search pops up in
	 * rSet, a vector search pops up in rSet and results. */
	var finalResults []models.SearchResult
	// ---------------------------
	cacheTx := s.cacheManager.NewTransaction()
	err := s.db.Read(func(bm diskstore.BucketManager) error {
		// ---------------------------
		bPoints, err := bm.Get(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get points bucket: %w", err)
		}
		// ---------------------------
		im := index.NewIndexManager(bm, cacheTx, s.dbFile, s.collection.IndexSchema)
		rSet, results, err := im.Search(context.Background(), searchRequest.Query)
		if err != nil {
			return fmt.Errorf("could not perform search: %w", err)
		}
		// ---------------------------
		// Backfill point UUID and data
		for _, r := range results {
			sp, err := GetPointByNodeId(bPoints, r.NodeId, len(searchRequest.Select) > 0)
			if err != nil {
				return fmt.Errorf("could not get point by node id %d: %w", r.NodeId, err)
			}
			r.Point = sp.Point
			rSet.Remove(r.NodeId)
			finalResults = append(finalResults, r)
		}
		// If any points are missing in the results from rSet, we need to append them
		it := rSet.Iterator()
		for it.HasNext() {
			nodeId := it.Next()
			sp, err := GetPointByNodeId(bPoints, nodeId, len(searchRequest.Select) > 0)
			if err != nil {
				return fmt.Errorf("could not get point by node id %d: %w", nodeId, err)
			}
			finalResults = append(finalResults, models.SearchResult{NodeId: nodeId, Point: sp.Point})
		}
		// ---------------------------
		return nil
	})
	if err != nil {
		cacheTx.Commit(true)
		return nil, fmt.Errorf("search failed: %w", err)
	}
	cacheTx.Commit(false)
	// ---------------------------
	/* Select and sort, if we have * (star) then we don't need to do anything and
	 * let upstream handle decoding the whole data point. Otherwise we need to
	 * selectively decode the required properties. Note that we are allowing
	 * sorting after selecting star. So if there is something to sort even if
	 * select star is given we need to decode the lot. */
	if (len(searchRequest.Select) > 0 && searchRequest.Select[0] != "*") || len(searchRequest.Sort) > 0 {
		selectSortStart := time.Now()
		/* We are selecting only a subset of the point data. We need to partial
		 * decode and re-encode the point data. */
		dec := msgpack.NewDecoder(nil)
		for i, r := range finalResults {
			// This fills with selected properties {"name": ...}
			finalResults[i].DecodedData = make(models.PointAsMap)
			if len(r.Point.Data) == 0 {
				// No data to select from
				continue
			}
			// E.g. ["name", "age"]
			for _, p := range searchRequest.Select {
				// E.g. p = "name" or "*" (star)
				dec.Reset(bytes.NewReader(r.Point.Data))
				if p == "*" {
					if err := dec.Decode(&finalResults[i].DecodedData); err != nil {
						return nil, fmt.Errorf("could not decode all point data: %w", err)
					}
					break
				}
				res, err := dec.Query(p)
				if err != nil {
					return nil, fmt.Errorf("could not select point data, %s: %w", p, err)
				}
				if len(res) == 0 {
					// Didn't find anything for this property
					continue
				}
				// ---------------------------
				/* We originally implemented nested fields to create nested maps
				 * and populate accordingly but it adds extra for loops and
				 * complexity. It also entangles the sorting code below as well.
				 * For now, a select field such as "nested.field" will comes
				 * back flattened, e.g. {"nested.field": value} as opposed to
				 * {"nested": {"field": value}}.
				 *
				 * UPDATE: We have decided to implemented the nested fields as it
				 * is more consistent with how the data is inputted. That is, the
				 * user gives us nested fields but upon retrieval we used to
				 * flatten it. This was confusing and we had implemented it as
				 * expanding nested fields originally, so we are going back to
				 * how things were. */
				// ---------------------------
				// Assign the value to final decoded data. This makes
				// {"property": value} e.g. {"name": "james"}
				segments := strings.Split(p, ".")
				// e.g. segments = ["nested", "field"] or ["name"]
				current := finalResults[i].DecodedData
				for j, s := range segments {
					if j == len(segments)-1 {
						current[s] = res[0]
						break
					}
					// If the nested field does not exist, we create it
					if _, ok := current[s]; !ok {
						current[s] = make(map[string]any)
					}
					var ok bool
					current, ok = current[s].(map[string]any)
					if !ok {
						return nil, fmt.Errorf("could not access nested property when selecting: %s", p)
					}
				}
			}
			// We erase data information as it is not needed any more, saves us
			// from transmitting it
			finalResults[i].Data = nil
		}
		// ---------------------------
		// Time to sort, the tricky bit here is that the type of values is any.
		if len(searchRequest.Sort) > 0 {
			utils.SortSearchResults(finalResults, searchRequest.Sort)
		}
		// ---------------------------
		s.logger.Debug().Str("duration", time.Since(selectSortStart).String()).Msg("Search - Select Sort")
	}
	/* End of select sort, if we skipped it then the encoded data is transmitted,
	 * otherwise DecodedData is populated and sent instead. */
	// ---------------------------
	// Offset and limit
	if searchRequest.Limit == 0 {
		searchRequest.Limit = len(finalResults)
	}
	finalResults = finalResults[min(searchRequest.Offset, len(finalResults)):min(searchRequest.Offset+searchRequest.Limit, len(finalResults))]
	// ---------------------------
	return finalResults, nil
}

// ---------------------------

func (s *Shard) DeletePoints(deleteSet map[uuid.UUID]struct{}) ([]uuid.UUID, error) {
	// ---------------------------
	deletedIds := make([]uuid.UUID, 0, len(deleteSet))
	// ---------------------------
	cacheTx := s.cacheManager.NewTransaction()
	err := s.db.Write(func(bm diskstore.BucketManager) error {
		bPoints, err := bm.Get(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get write points bucket: %w", err)
		}
		bInternal, err := bm.Get(INTERNALBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get write internal bucket: %w", err)
		}
		// ---------------------------
		nodeCounter, err := NewIdCounter(bInternal, FREENODEIDSKEY, NEXTFREENODEIDKEY)
		if err != nil {
			return fmt.Errorf("could not create id counter: %w", err)
		}
		// ---------------------------
		// Kick off index dispatcher
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// ---------------------------
		pointsQ := utils.ProduceWithContextMapKeys(ctx, deleteSet)
		indexQ, indexQErrC := utils.TransformWithContext(ctx, pointsQ, func(pointId uuid.UUID) (ipc index.IndexPointChange, skip bool, err error) {
			sp, err := GetPointByUUID(bPoints, pointId)
			if err == ErrPointDoesNotExist {
				// Deleting a non-existing point is a no-op
				skip = true
				return
			}
			if err != nil {
				err = fmt.Errorf("could not get point for deletion: %w", err)
				return
			}
			deletedIds = append(deletedIds, pointId)
			nodeCounter.FreeId(sp.NodeId)
			// ---------------------------
			if err = DeletePoint(bPoints, pointId, sp.NodeId); err != nil {
				err = fmt.Errorf("could not delete point %s: %w", pointId, err)
				return
			}
			// ---------------------------
			ipc.NodeId = sp.NodeId
			ipc.PreviousData = sp.Data
			return
		})
		im := index.NewIndexManager(bm, cacheTx, s.dbFile, s.collection.IndexSchema)
		dispatchErrC := im.Dispatch(ctx, indexQ)
		// ---------------------------
		mergedErrC := utils.MergeErrorsWithContext(ctx, indexQErrC, dispatchErrC)
		// At this point concurrent stuff is over, we can check for errors
		if err := <-mergedErrC; err != nil {
			return fmt.Errorf("could not complete insert: %w", err)
		}
		// ---------------------------
		// Update point count accordingly
		if err := changePointCount(bInternal, -len(deletedIds)); err != nil {
			return fmt.Errorf("could not change point count for deletion: %w", err)
		}
		// ---------------------------
		if err := nodeCounter.Flush(); err != nil {
			return fmt.Errorf("could not flush id counter: %w", err)
		}
		// ---------------------------
		return nil
	})
	if err != nil {
		cacheTx.Commit(true)
		return nil, fmt.Errorf("could not delete points: %w", err)
	}
	cacheTx.Commit(false)
	return deletedIds, nil
}

// ---------------------------
