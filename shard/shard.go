package shard

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
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
	// Maximum node id used in the shard. This is actually used for visit sets
	// to determine the size of the bitset or fallback to a map. It is not the
	// counter from which new Ids are generated. That is handled by the id
	// counter. We store this here to avoid having to read the id counter
	// potentially from disk every time we need to create a new visit set. It
	// doesn't need to be exact either, bitsets can resize if we get it wrong
	// but we keep it in sync anyway.
	maxNodeId atomic.Uint64
	// ---------------------------
	cacheManager *cache.Manager
	logger       zerolog.Logger
}

// ---------------------------
const CURRENTSHARDVERSION = 1

/* Points store the actual data points, graphIndex stores the similarity graph
 * and internal stores the shard metadata such as point count. We partition like
 * this because graph traversal is read-heavy operation, if everything is bundled
 * together, the disk cache pulls in more pages. It's also logically easier to
 * manage. */
const POINTSBUCKETKEY = "points"
const INTERNALBUCKETKEY = "internal"

// ---------------------------
var STARTIDKEY = []byte("startId")
var POINTCOUNTKEY = []byte("pointCount")

var FREENODEIDSKEY = []byte("freeNodeIds")
var NEXTFREENODEIDKEY = []byte("nextFreeNodeId")
var SHARDVERSIONKEY = []byte("shardVersion")

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
	var maxNodeId uint64
	err = db.Write(func(bm diskstore.BucketManager) error {
		// ---------------------------
		// Setup buckets
		bInternal, err := bm.Get(INTERNALBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not write internal bucket: %w", err)
		}
		// ---------------------------
		nodeCounter, err := NewIdCounter(bInternal, FREENODEIDSKEY, NEXTFREENODEIDKEY)
		if err != nil {
			return fmt.Errorf("could not create id counter: %w", err)
		}
		maxNodeId = nodeCounter.MaxId()
		// ---------------------------
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not initialise shard: %w", err)
	}
	// ---------------------------
	shard := &Shard{
		dbFile:       dbFile, // An alternative could be db.Path()
		db:           db,
		collection:   collection,
		cacheManager: cacheManager,
		logger:       log.With().Str("component", "shard").Str("name", dbFile).Logger(),
	}
	shard.maxNodeId.Store(maxNodeId)
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
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		indexQ := make(chan index.IndexPointChange)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			im := index.NewIndexManager(bm, s.cacheManager, s.dbFile, s.collection.IndexSchema, s.maxNodeId.Load())
			err := im.Dispatch(ctx, indexQ)
			if err != nil {
				cancel(fmt.Errorf("could not dispatch index: %w", err))
			}
		}()
		// ---------------------------
		for _, point := range points {
			// ---------------------------
			/* If the point exists, we can't re-insert it. This is actually an
			 * error because the edges will be wrong in the graph. It needs to be
			 * updated instead. We can potentially do it here (do an update
			 * instead of insert) but the API design migh be inconsistent as it
			 * will then depend whether a point is re-assigned to the same shard
			 * during insertion when there are multiple shards. We are returning
			 * an error here to force the user to update the point instead which
			 * handles the multiple shard case. */
			if exists, err := CheckPointExists(bPoints, point.Id); err != nil {
				return fmt.Errorf("could not check point existence: %w", err)
			} else if exists {
				return fmt.Errorf("point already exists: %s", point.Id.String())
			}
			sp := ShardPoint{Point: point, NodeId: nodeCounter.NextId()}
			if err := SetPoint(bPoints, sp); err != nil {
				return fmt.Errorf("could not set point: %w", err)
			}
			// ---------------------------
			select {
			case <-ctx.Done():
				return fmt.Errorf("context interrupt for shard insert: %w", context.Cause(ctx))
			case indexQ <- index.IndexPointChange{NodeId: sp.NodeId, PreviousData: nil, NewData: point.Data}:
			}
		}
		close(indexQ)
		wg.Wait()
		// At this point concurrent stuff is over, we can check for errors
		if err := context.Cause(ctx); err != nil {
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
		s.maxNodeId.Store(nodeCounter.MaxId())
		txTime = time.Now()
		return nil
	})
	s.logger.Debug().Str("duration", time.Since(txTime).String()).Msg("InsertPoints - Transaction Done")
	if err != nil {
		s.logger.Error().Err(err).Msg("could not insert points")
		return fmt.Errorf("could not insert points: %w", err)
	}
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
	err := s.db.Write(func(bm diskstore.BucketManager) error {
		pointsBucket, err := bm.Get(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get write points bucket: %w", err)
		}
		// ---------------------------
		// Kick off index dispatcher
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		indexQ := make(chan index.IndexPointChange)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			im := index.NewIndexManager(bm, s.cacheManager, s.dbFile, s.collection.IndexSchema, s.maxNodeId.Load())
			err := im.Dispatch(ctx, indexQ)
			if err != nil {
				cancel(fmt.Errorf("could not dispatch index: %w", err))
			}
		}()
		// ---------------------------
		for _, point := range points {
			sp, err := GetPointByUUID(pointsBucket, point.Id)
			if err == ErrPointDoesNotExist {
				// Point does not exist, we can skip it, it may reside in
				// another shard. Updating non-existing points is a no-op.
				continue
			}
			if err != nil {
				return fmt.Errorf("could not get point node id: %w", err)
			}
			// Merge data on update
			var existingData models.PointAsMap
			var incomingData models.PointAsMap
			if err := msgpack.Unmarshal(sp.Data, &existingData); err != nil {
				return fmt.Errorf("could not unmarshal old data: %w", err)
			}
			if err := msgpack.Unmarshal(point.Data, &incomingData); err != nil {
				return fmt.Errorf("could not unmarshal new data: %w", err)
			}
			// TODO: add tests for this merging and deleting of values
			for k, v := range incomingData {
				if vs, ok := v.(string); ok && vs == DELETEVALUE {
					delete(existingData, k)
				} else {
					existingData[k] = v
				}
			}
			finalNewData, err := msgpack.Marshal(existingData)
			if err != nil {
				return fmt.Errorf("could not marshal new data: %w", err)
			}
			// Check if the user is making a point too large
			// TODO: Add tests for this check
			if len(finalNewData) > s.collection.UserPlan.MaxMetadataSize {
				return fmt.Errorf("point size exceeds limit: %d", s.collection.UserPlan.MaxMetadataSize)
			}
			// ---------------------------
			point.Data = finalNewData
			if err := SetPoint(pointsBucket, ShardPoint{Point: point, NodeId: sp.NodeId}); err != nil {
				return fmt.Errorf("could not set updated point: %w", err)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("context interrupt for shard update: %w", context.Cause(ctx))
			case indexQ <- index.IndexPointChange{NodeId: sp.NodeId, PreviousData: sp.Data, NewData: finalNewData}:
			}
			// ---------------------------
			updatedIds = append(updatedIds, point.Id)
		}
		close(indexQ)
		wg.Wait()
		// At this point concurrent stuff is over, we can check for errors
		if err := context.Cause(ctx); err != nil {
			return fmt.Errorf("could not complete insert: %w", err)
		}
		return nil
	})
	if err != nil {
		s.logger.Debug().Err(err).Msg("could not update points")
		return nil, fmt.Errorf("could not update points: %w", err)
	}
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
	err := s.db.Read(func(bm diskstore.BucketManager) error {
		// ---------------------------
		bPoints, err := bm.Get(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get points bucket: %w", err)
		}
		// ---------------------------
		im := index.NewIndexManager(bm, s.cacheManager, s.dbFile, s.collection.IndexSchema, s.maxNodeId.Load())
		rSet, results, err := im.Search(context.Background(), searchRequest.Query)
		if err != nil {
			return fmt.Errorf("could not perform search: %w", err)
		}
		// ---------------------------
		// Backfill point UUID and data
		for _, r := range results {
			sp, err := GetPointByNodeId(bPoints, r.NodeId)
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
			sp, err := GetPointByNodeId(bPoints, nodeId)
			if err != nil {
				return fmt.Errorf("could not get point by node id %d: %w", nodeId, err)
			}
			finalResults = append(finalResults, models.SearchResult{NodeId: nodeId, Point: sp.Point})
		}
		// ---------------------------
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}
	// ---------------------------
	// Select and sort
	if len(searchRequest.Select) > 0 {
		selectSortStart := time.Now()
		/* We are selecting only a subset of the point data. We need to partial
		 * decode and re-encode the point data. */
		tempPoints := make([]models.PointAsMap, len(finalResults))
		dec := msgpack.NewDecoder(nil)
		for i, r := range finalResults {
			tp := make(models.PointAsMap)
			for _, p := range searchRequest.Select {
				// E.g. p = "name"
				dec.Reset(bytes.NewReader(r.Point.Data))
				res, err := dec.Query(p)
				if err != nil {
					return nil, fmt.Errorf("could not select point data, %s: %w", p, err)
				}
				if len(res) == 0 {
					// Didn't find anything for this property
					continue
				}
				tp[p] = res[0]
			}
			tempPoints[i] = tp
		}
		// ---------------------------
		// Time to sort, the tricky bit here is that the type of values is any.
		if len(searchRequest.Sort) > 0 {
			/* Because we don't know the type of the values, this may be a costly
			 * operation to undertake. We should monitor how this performs. */
			slices.SortFunc(tempPoints, func(a models.PointAsMap, b models.PointAsMap) int {
				for _, s := range searchRequest.Sort {
					// E.g. s = "age"
					av, ok := a[s.Property]
					if !ok {
						// If the property is missing, we need to decide what to do
						// here. We can either put it at the top or bottom. We put it
						// at the bottom for now.
						return 1
					}
					bv, ok := b[s.Property]
					if !ok {
						return -1
					}
					var res int
					if s.Descending {
						res = utils.CompareAny(bv, av)
					} else {
						res = utils.CompareAny(av, bv)
					}
					if res != 0 {
						return res
					}
				}
				return 0
			})
		}
		// ---------------------------
		// Re-encode the point data
		for i, tp := range tempPoints {
			b, err := msgpack.Marshal(tp)
			if err != nil {
				return nil, fmt.Errorf("could not re-encode point data after select: %w", err)
			}
			finalResults[i].Point.Data = b
		}
		s.logger.Debug().Str("duration", time.Since(selectSortStart).String()).Msg("Search - Select Sort")
	}
	// ---------------------------
	// Offset and limit
	finalResults = finalResults[min(searchRequest.Offset, len(finalResults)):min(searchRequest.Offset+searchRequest.Limit, len(finalResults))]
	// ---------------------------
	return finalResults, nil
}

// ---------------------------

func (s *Shard) DeletePoints(deleteSet map[uuid.UUID]struct{}) ([]uuid.UUID, error) {
	// ---------------------------
	deletedIds := make([]uuid.UUID, 0, len(deleteSet))
	// ---------------------------
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
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		indexQ := make(chan index.IndexPointChange)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			im := index.NewIndexManager(bm, s.cacheManager, s.dbFile, s.collection.IndexSchema, s.maxNodeId.Load())
			err := im.Dispatch(ctx, indexQ)
			if err != nil {
				cancel(fmt.Errorf("could not dispatch index: %w", err))
			}
		}()
		// ---------------------------
		for pointId := range deleteSet {
			sp, err := GetPointByUUID(bPoints, pointId)
			if err == ErrPointDoesNotExist {
				// Deleting a non-existing point is a no-op
				continue
			}
			if err != nil {
				return fmt.Errorf("could not get point for deletion: %w", err)
			}
			deletedIds = append(deletedIds, pointId)
			nodeCounter.FreeId(sp.NodeId)
			select {
			case <-ctx.Done():
				return fmt.Errorf("context interrupt for shard delete: %w", context.Cause(ctx))
			case indexQ <- index.IndexPointChange{NodeId: sp.NodeId, PreviousData: sp.Data, NewData: nil}:
			}
			if err := DeletePoint(bPoints, pointId, sp.NodeId); err != nil {
				return fmt.Errorf("could not delete point %s: %w", pointId, err)
			}
		}
		close(indexQ)
		wg.Wait()
		// At this point concurrent stuff is over, we can check for errors
		if err := context.Cause(ctx); err != nil {
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
		return nil, fmt.Errorf("could not delete points: %w", err)
	}
	return deletedIds, nil
}

// ---------------------------
