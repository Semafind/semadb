package shard

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index"
	"github.com/semafind/semadb/shard/index/vamana"
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
	// TODO: add shard logger
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
		bInternal, err := bm.WriteBucket(INTERNALBUCKETKEY)
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
	err = s.db.Read(func(bm diskstore.ReadOnlyBucketManager) error {
		b, err := bm.ReadBucket(INTERNALBUCKETKEY)
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
	log.Debug().Str("component", "shard").Int("count", len(points)).Msg("InsertPoints")
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
		bPoints, err := bm.WriteBucket(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not write points bucket: %w", err)
		}
		bInternal, err := bm.WriteBucket(INTERNALBUCKETKEY)
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
		indexQ := make(chan index.IndexPointChange)
		waitIndex := make(chan error)
		go func() {
			err := index.Dispatch(ctx, cancel, bm, s.cacheManager, s.dbFile, s.collection.IndexSchema, uint(s.maxNodeId.Load()), indexQ)
			waitIndex <- err
		}()
		// ---------------------------
		for _, point := range points {
			// ---------------------------
			if ctx.Err() != nil {
				break
			}
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
				cancel(fmt.Errorf("could not check point existence: %w", err))
				break
			} else if exists {
				cancel(fmt.Errorf("point already exists: %s", point.Id.String()))
				break
			}
			sp := ShardPoint{Point: point, NodeId: nodeCounter.NextId()}
			if err := SetPoint(bPoints, sp); err != nil {
				cancel(fmt.Errorf("could not set point: %w", err))
				break
			}
			// ---------------------------
			indexQ <- index.IndexPointChange{NodeId: sp.NodeId, PreviousData: nil, NewData: point.Data}
		}
		close(indexQ)
		if err := <-waitIndex; err != nil {
			cancel(fmt.Errorf("could not index points: %w", err))
		}
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
	log.Debug().Str("component", "shard").Str("duration", time.Since(txTime).String()).Msg("InsertPoints - Transaction Done")
	if err != nil {
		log.Error().Err(err).Msg("could not insert points")
		return fmt.Errorf("could not insert points: %w", err)
	}
	// ---------------------------
	return nil
}

// ---------------------------

func (s *Shard) UpdatePoints(points []models.Point) ([]uuid.UUID, error) {
	log.Debug().Str("component", "shard").Int("count", len(points)).Msg("UpdatePoints")
	// ---------------------------
	// Note that some points may not exist, so we need to take care of that
	// throughout this function
	updatedIds := make([]uuid.UUID, 0, len(points))
	// ---------------------------
	err := s.db.Write(func(bm diskstore.BucketManager) error {
		pointsBucket, err := bm.WriteBucket(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get write points bucket: %w", err)
		}
		// ---------------------------
		// Kick off index dispatcher
		ctx, cancel := context.WithCancelCause(context.Background())
		indexQ := make(chan index.IndexPointChange)
		waitIndex := make(chan error)
		go func() {
			err := index.Dispatch(ctx, cancel, bm, s.cacheManager, s.dbFile, s.collection.IndexSchema, uint(s.maxNodeId.Load()), indexQ)
			waitIndex <- err
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
				cancel(fmt.Errorf("could not get point node id: %w", err))
				break
			}
			// Merge data on update
			var existingData models.PointAsMap
			var incomingData models.PointAsMap
			if err := msgpack.Unmarshal(sp.Data, &existingData); err != nil {
				cancel(fmt.Errorf("could not unmarshal old data: %w", err))
				break
			}
			if err := msgpack.Unmarshal(point.Data, &incomingData); err != nil {
				cancel(fmt.Errorf("could not unmarshal new data: %w", err))
				break
			}
			// TODO: add tests for this merging and deleting of values
			for k, v := range incomingData {
				if v == DELETEVALUE {
					delete(existingData, k)
				} else {
					existingData[k] = v
				}
			}
			finalNewData, err := msgpack.Marshal(existingData)
			if err != nil {
				cancel(fmt.Errorf("could not marshal new data: %w", err))
				break
			}
			// Check if the user is making a point too large
			// TODO: Add tests for this check
			if len(finalNewData) > s.collection.UserPlan.MaxMetadataSize {
				cancel(fmt.Errorf("point size exceeds limit: %d", s.collection.UserPlan.MaxMetadataSize))
				break
			}
			indexQ <- index.IndexPointChange{NodeId: sp.NodeId, PreviousData: sp.Data, NewData: finalNewData}
			// ---------------------------
			if err := SetPoint(pointsBucket, ShardPoint{Point: point, NodeId: sp.NodeId}); err != nil {
				cancel(fmt.Errorf("could not set updated point: %w", err))
				break
			}
			updatedIds = append(updatedIds, point.Id)
		}
		close(indexQ)
		if err := <-waitIndex; err != nil {
			cancel(fmt.Errorf("could not index points: %w", err))
		}
		// At this point concurrent stuff is over, we can check for errors
		if err := context.Cause(ctx); err != nil {
			return fmt.Errorf("could not complete insert: %w", err)
		}
		return nil
	})
	if err != nil {
		log.Debug().Err(err).Msg("could not update points")
		return nil, fmt.Errorf("could not update points: %w", err)
	}
	// ---------------------------
	return updatedIds, nil
}

// ---------------------------

type SearchPoint struct {
	Point    models.Point
	Distance float32
}

func (s *Shard) SearchPoints(query []float32, k int) ([]SearchPoint, error) {
	// ---------------------------
	var results []SearchPoint
	err := s.db.Read(func(bm diskstore.ReadOnlyBucketManager) error {
		// ---------------------------
		bPoints, err := bm.ReadBucket(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get points bucket: %w", err)
		}
		// ---------------------------
		bucketName := "index/vectorVamana/vector"
		bGraph, err := bm.ReadBucket(bucketName)
		if err != nil {
			return fmt.Errorf("could not get graph bucket: %w", err)
		}
		vIndex, err := vamana.NewIndexVamana(s.dbFile+"/"+bucketName, s.collection.IndexSchema.VectorVamana["vector"], s.cacheManager, uint(s.maxNodeId.Load()))
		if err != nil {
			return fmt.Errorf("could not create vamana index: %w", err)
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		// TODO: Switch to dispatch search for complex queries
		searchResults, err := vIndex.Search(ctx, cancel, bGraph, query, k)
		if err != nil {
			cancel(fmt.Errorf("could not perform search: %w", err))
		}
		if err := context.Cause(ctx); err != nil {
			return fmt.Errorf("could not complete search: %w", err)
		}
		results = make([]SearchPoint, len(searchResults))
		for i, sr := range searchResults {
			sp, err := GetPointByNodeId(bPoints, sr.NodeId)
			if err != nil {
				return fmt.Errorf("could not get point by node id: %w", err)
			}
			results[i] = SearchPoint{Point: sp.Point, Distance: sr.Distance}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not perform search: %w", err)
	}
	// ---------------------------
	return results, nil
}

// ---------------------------

func (s *Shard) DeletePoints(deleteSet map[uuid.UUID]struct{}) ([]uuid.UUID, error) {
	// ---------------------------
	deletedIds := make([]uuid.UUID, 0, len(deleteSet))
	// ---------------------------
	err := s.db.Write(func(bm diskstore.BucketManager) error {
		bPoints, err := bm.WriteBucket(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get write points bucket: %w", err)
		}
		bInternal, err := bm.WriteBucket(INTERNALBUCKETKEY)
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
		indexQ := make(chan index.IndexPointChange)
		waitIndex := make(chan error)
		go func() {
			err := index.Dispatch(ctx, cancel, bm, s.cacheManager, s.dbFile, s.collection.IndexSchema, uint(s.maxNodeId.Load()), indexQ)
			waitIndex <- err
		}()
		// ---------------------------
		for pointId := range deleteSet {
			nodeId, err := GetPointNodeIdByUUID(bPoints, pointId)
			if err == ErrPointDoesNotExist || nodeId == 0 {
				// Deleting a non-existing point is a no-op
				continue
			} else if err != nil {
				return fmt.Errorf("could not get point node id for deletion: %w", err)
			}
			deletedIds = append(deletedIds, pointId)
			nodeCounter.FreeId(nodeId)
			indexQ <- index.IndexPointChange{NodeId: nodeId, PreviousData: nil, NewData: nil}
			if err := DeletePoint(bPoints, pointId, nodeId); err != nil {
				cancel(fmt.Errorf("could not delete point %s: %w", pointId, err))
				break
			}
		}
		close(indexQ)
		if err := <-waitIndex; err != nil {
			cancel(fmt.Errorf("could not index points: %w", err))
		}
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
