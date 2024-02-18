package shard

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/utils"
	"go.etcd.io/bbolt"
)

type Shard struct {
	dbFile     string
	db         *bbolt.DB
	collection models.Collection
	distFn     distance.DistFunc
	// Start point is the entry point to the graph. It is used to bootstrap the
	// search and is excluded from results
	startId uint64
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
}

// ---------------------------
const CURRENTSHARDVERSION = 1

var POINTSBUCKETKEY = []byte("points")
var INTERNALBUCKETKEY = []byte("internal")

// ---------------------------
var STARTIDKEY = []byte("startId")
var POINTCOUNTKEY = []byte("pointCount")

var FREENODEIDSKEY = []byte("freeNodeIds")
var NEXTFREENODEIDKEY = []byte("nextFreeNodeId")
var SHARDVERSIONKEY = []byte("shardVersion")

// ---------------------------

func NewShard(dbFile string, collection models.Collection, cacheManager *cache.Manager) (*Shard, error) {
	// ---------------------------
	db, err := bbolt.Open(dbFile, 0644, &bbolt.Options{Timeout: 1 * time.Minute})
	if err != nil {
		return nil, fmt.Errorf("could not open shard db: %w", err)
	}
	// ---------------------------
	if cacheManager == nil {
		// 0 means no cache, every operation will get blank cache and discard it
		cacheManager = cache.NewManager(0)
	}
	var startId uint64
	var maxNodeId uint64
	err = db.Update(func(tx *bbolt.Tx) error {
		// ---------------------------
		// Setup buckets
		bPoints, err := tx.CreateBucketIfNotExists(POINTSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		bInternal, err := tx.CreateBucketIfNotExists(INTERNALBUCKETKEY)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		// ---------------------------
		// Get or set shard version
		sv := bInternal.Get(SHARDVERSIONKEY)
		if sv == nil {
			// There is no shard version, we'll create one
			if err := bInternal.Put(SHARDVERSIONKEY, cache.Uint64ToBytes(CURRENTSHARDVERSION)); err != nil {
				return fmt.Errorf("could not set shard version: %w", err)
			}
		} else {
			// Check if the shard version is compatible
			shardVersion := cache.BytesToUint64(sv)
			if shardVersion != CURRENTSHARDVERSION {
				// In the future we can implement a migration process here
				return fmt.Errorf("shard version mismatch: %d != %d", shardVersion, CURRENTSHARDVERSION)
			}
		}
		// ---------------------------
		nodeCounter, err := NewIdCounter(bInternal, FREENODEIDSKEY, NEXTFREENODEIDKEY)
		if err != nil {
			return fmt.Errorf("could not create id counter: %w", err)
		}
		// ---------------------------
		// Setup start point
		sid := bInternal.Get(STARTIDKEY)
		if sid == nil {
			// There is no start point, we'll create one
			// Create random unit vector of size n
			randVector := make([]float32, collection.VectorSize)
			sum := float32(0)
			for i := range randVector {
				randVector[i] = rand.Float32()*2 - 1
				sum += randVector[i] * randVector[i]
			}
			// Normalise the vector
			norm := 1 / float32(math.Sqrt(float64(sum)))
			for i := range randVector {
				randVector[i] *= norm
			}
			startId = nodeCounter.NextId()
			if err := nodeCounter.Flush(); err != nil {
				return fmt.Errorf("could not flush id counter: %w", err)
			}
			// ---------------------------
			// Create start point
			randPoint := cache.ShardPoint{
				NodeId: startId,
				Point: models.Point{
					Id:     uuid.New(),
					Vector: randVector,
				},
			}
			err := cacheManager.With(dbFile, bPoints, func(pc cache.ReadWriteCache) error {
				log.Debug().Uint64("id", startId).Str("UUID", randPoint.Id.String()).Msg("creating start point")
				pc.SetPoint(randPoint)
				if err := pc.Flush(); err != nil {
					return fmt.Errorf("could not flush start point: %w", err)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("could not create start point: %w", err)
			}
			if err := bInternal.Put(STARTIDKEY, cache.Uint64ToBytes(randPoint.NodeId)); err != nil {
				return fmt.Errorf("could not set start point id: %w", err)
			}
			// ---------------------------
		} else {
			startId = cache.BytesToUint64(sid)
			log.Debug().Uint64("id", startId).Msg("found start point")
		}
		// ---------------------------
		maxNodeId = nodeCounter.MaxId()
		// ---------------------------
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not initialise shard: %w", err)
	}
	// ---------------------------
	distFn, err := distance.GetDistanceFn(collection.DistMetric)
	if err != nil {
		return nil, fmt.Errorf("could not get distance function: %w", err)
	}
	// ---------------------------
	shard := &Shard{
		dbFile:       dbFile, // An alternative could be db.Path()
		db:           db,
		collection:   collection,
		distFn:       distFn,
		startId:      startId,
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

func changePointCount(tx *bbolt.Tx, change int) error {
	bInternal := tx.Bucket(INTERNALBUCKETKEY)
	// ---------------------------
	countBytes := bInternal.Get(POINTCOUNTKEY)
	var count uint64
	if countBytes != nil {
		count = cache.BytesToUint64(countBytes)
	}
	// ---------------------------
	newCount := int(count) + change
	if newCount < 0 {
		return fmt.Errorf("point count cannot be negative")
	}
	// ---------------------------
	countBytes = cache.Uint64ToBytes(uint64(newCount))
	if err := bInternal.Put(POINTCOUNTKEY, countBytes); err != nil {
		return fmt.Errorf("could not change point count: %w", err)
	}
	return nil
}

type shardInfo struct {
	PointCount uint64
	Size       int64 // Size of the shard database file
}

func (s *Shard) Info() (si shardInfo, err error) {
	err = s.db.View(func(tx *bbolt.Tx) error {
		bInternal := tx.Bucket(INTERNALBUCKETKEY)
		// ---------------------------
		// The reason we use a point count is because a single point has
		// multiple key value pairs in the points bucket. This is easier to
		// manage than counting the number of keys in the points bucket which
		// may change over time.
		countBytes := bInternal.Get(POINTCOUNTKEY)
		if countBytes != nil {
			si.PointCount = cache.BytesToUint64(countBytes)
		}
		// ---------------------------
		si.Size = tx.Size()
		return nil
	})
	return
}

// ---------------------------

func (s *Shard) insertSinglePoint(pc cache.ReadWriteCache, startPointId uint64, shardPoint cache.ShardPoint) error {
	// ---------------------------
	point, err := pc.SetPoint(shardPoint)
	if err != nil {
		return fmt.Errorf("could not set point: %w", err)
	}
	// ---------------------------
	_, visitedSet, err := s.greedySearch(pc, startPointId, point.Vector, 1, s.collection.Parameters.SearchSize)
	if err != nil {
		return fmt.Errorf("could not greedy search: %w", err)
	}
	// ---------------------------
	// We don't need to lock the point here because it does not yet have inbound
	// edges that other goroutines might use to visit this node.
	s.robustPrune(point, visitedSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
	// ---------------------------
	// Add the bi-directional edges, suppose A is being added and has A -> B and
	// A -> C. Then we attempt to add edges from B and C back to A. point.Edges
	// is A -> B and A -> C.
	err = pc.WithPointNeighbours(point, true, func(nnA []*cache.CachePoint) error {
		for _, n := range nnA {
			// So here n = B or C as the example goes
			// While we are adding the bi-directional edges, we need exclusive
			// access to ensure other goroutines don't modify the edges while we are
			// dealing with them. That is what WithPointNeighbours is for.
			err = pc.WithPointNeighbours(n, false, func(nnB []*cache.CachePoint) error {
				if len(nnB)+1 > s.collection.Parameters.DegreeBound {
					// We need to prune the neighbour as well to keep the degree bound
					candidateSet := NewDistSet(n.Vector, len(nnB)+1, 0, s.distFn)
					candidateSet.AddPoint(nnB...)
					candidateSet.AddPoint(point) // Here we are asking B or C to add A
					candidateSet.Sort()
					s.robustPrune(n, candidateSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
				} else {
					// ---------------------------
					// Add the edge
					n.AddNeighbour(point)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("could not get neighbour point neighbours for bi-directional edges: %w", err)
			}
		}
		return nil
	})
	// ---------------------------
	return nil
}

// ---------------------------

func (s *Shard) insertWorker(ctx context.Context, pc cache.ReadWriteCache, jobQueue <-chan cache.ShardPoint, wg *sync.WaitGroup, cancel context.CancelCauseFunc) {
	defer wg.Done()
	for point := range jobQueue {
		// Check if we've been cancelled
		select {
		case <-ctx.Done():
			return
		default:
		}
		/* If the point exists, we can't re-insert it. This is actually an error
		 * because the edges will be wrong in the graph. It needs to be updated
		 * instead. We can potentially do it here (do an update instead of
		 * insert) but the API design migh be inconsistent as it will then depend
		 * whether a point is re-assigned to the same shard during insertion when
		 * there are multiple shards. We are returning an error here to force the
		 * user to update the point instead which handles the multiple shard
		 * case. */
		if _, err := pc.GetPointByUUID(point.Id); err == nil {
			cancel(fmt.Errorf("point already exists: %s", point.Id.String()))
			return
		}
		// Insert the point
		if err := s.insertSinglePoint(pc, s.startId, point); err != nil {
			cancel(err)
			return
		}
	}
}

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
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSBUCKETKEY)
		// ---------------------------
		nodeCounter, err := NewIdCounter(tx.Bucket(INTERNALBUCKETKEY), FREENODEIDSKEY, NEXTFREENODEIDKEY)
		if err != nil {
			return fmt.Errorf("could not create id counter: %w", err)
		}
		// ---------------------------
		// Setup parallel insert routine
		jobQueue := make(chan cache.ShardPoint, len(points))
		// Submit the jobs
		for point := range points {
			jobQueue <- cache.ShardPoint{Point: points[point], NodeId: nodeCounter.NextId()}
		}
		close(jobQueue)
		/* The reason we need to setup all the jobs before starting as opposed to
		 * starting the workers and submitting jobs is because the workers shared
		 * the same cache. Notice how the cache operation waits for all workers
		 * to finish to determine whether there was a failure and then we report
		 * that failure to bbolt to abort the transaction. If we start workers
		 * first, we can synchronise on the final error, in other words we can't
		 * check the error before submitting the jobs. */
		err = s.cacheManager.With(s.dbFile, b, func(pc cache.ReadWriteCache) error {
			/* In this funky case there are actually multiple goroutines
			 * operating on the same cache. As opposed to multiple requests
			 * queuing to get access to the shared cache. */
			// ---------------------------
			var wg sync.WaitGroup
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			numWorkers := runtime.NumCPU() * 3 / 4 // We leave some cores for other tasks
			startTime := time.Now()
			// ---------------------------
			for i := 0; i < numWorkers; i++ {
				wg.Add(1)
				go s.insertWorker(ctx, pc, jobQueue, &wg, cancel)
			}
			wg.Wait()
			log.Debug().Int("points", len(points)).Str("duration", time.Since(startTime).String()).Msg("InsertPoints - Insert")
			// ---------------------------
			if err := context.Cause(ctx); err != nil {
				return fmt.Errorf("could not run insert procedure: %w", err)
			}
			// ---------------------------
			startTime = time.Now()
			if err := pc.Flush(); err != nil {
				return fmt.Errorf("could not flush point cache: %w", err)
			}
			// ---------------------------
			log.Debug().Str("component", "shard").Str("duration", time.Since(startTime).String()).Msg("InsertPoints - Flush")
			return nil
		})
		if err != nil {
			return fmt.Errorf("insertion failed: %w", err)
		}
		// ---------------------------
		// Update point count accordingly
		if err := changePointCount(tx, len(points)); err != nil {
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
		log.Debug().Err(err).Msg("could not insert points")
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
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSBUCKETKEY)
		// ---------------------------
		err := s.cacheManager.With(s.dbFile, b, func(pc cache.ReadWriteCache) error {
			updateSet := make(map[uuid.UUID]*cache.CachePoint, len(points))
			updateNodeIds := make(map[uint64]struct{}, len(points))
			// ---------------------------
			// First we check if the points exist
			for _, point := range points {
				if cp, err := pc.GetPointByUUID(point.Id); err != nil {
					// Point does not exist, or we can't access it at the moment
					continue
				} else {
					updatedIds = append(updatedIds, point.Id)
					cp.Point = point // Update the point, we will re-insert the cache point later
					updateSet[point.Id] = cp
					updateNodeIds[cp.NodeId] = struct{}{}
				}
			}
			if len(updateSet) == 0 {
				// No points to update, we skip the process
				return nil
			}
			// ---------------------------
			// We assume the updated points have their vectors changed. We can in
			// the future handle metadata updates specifically so all this
			// re-indexing doesn't happen.
			if err := s.removeInboundEdges(pc, updateNodeIds); err != nil {
				return fmt.Errorf("could not remove inbound edges: %w", err)
			}
			// ---------------------------
			// Now the pruning is complete, we can re-insert the points again
			for _, cp := range updateSet {
				if err := s.insertSinglePoint(pc, s.startId, cp.ShardPoint); err != nil {
					return fmt.Errorf("could not re-insert point: %w", err)
				}
			}
			// ---------------------------
			return pc.Flush()
		})
		return err
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
	// Perform search, we add 1 to k because the start point is included in the
	// search set. Recall that the start point is only used to bootstrap the
	// search, and is not included in the results.
	results := make([]SearchPoint, 0, k+1)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSBUCKETKEY)
		err := s.cacheManager.WithReadOnly(s.dbFile, b, func(pc cache.ReadOnlyCache) error {
			startTime := time.Now()
			searchSet, _, err := s.greedySearch(pc, s.startId, query, k, s.collection.Parameters.SearchSize)
			log.Debug().Str("component", "shard").Str("duration", time.Since(startTime).String()).Msg("SearchPoints - GreedySearch")
			if err != nil {
				return fmt.Errorf("could not perform graph search: %w", err)
			}
			// ---------------------------
			// Clean up results and backfill metadata
			for _, distElem := range searchSet.items {
				point := distElem.point
				// We skip the start point
				if point.NodeId == s.startId {
					continue
				}
				if len(results) >= k {
					break
				}
				mdata, err := pc.GetMetadata(point.NodeId)
				if err != nil {
					return fmt.Errorf("could not get point metadata: %w", err)
				}
				point.Metadata = mdata
				sp := SearchPoint{
					Point:    point.Point,
					Distance: distElem.distance}
				results = append(results, sp)
			}
			return nil
		})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("could not perform search: %w", err)
	}
	// ---------------------------
	return results, nil
}

// ---------------------------

// This point has a neighbour that is being deleted. We need to pool together
// the deleted neighbour neighbours and prune the point.
func (s *Shard) pruneDeleteNeighbour(pc cache.ReadWriteCache, id uint64, deleteSet map[uint64]struct{}) error {
	// ---------------------------
	point, err := pc.GetPoint(id)
	if err != nil {
		return fmt.Errorf("could not get point: %w", err)
	}
	// ---------------------------
	// We are going to build a new candidate list of neighbours and then robust
	// prune it. What is happening here is A -> B -> C, and B is being deleted.
	// So we add existing neighbours of B to the candidate list. We go / expand
	// only one level deep.
	deletedNothing := true
	err = pc.WithPointNeighbours(point, false, func(nnA []*cache.CachePoint) error {
		// These are the neighbours of A
		candidateSet := NewDistSet(point.Vector, len(nnA)*2, 0, s.distFn)
		for _, neighbour := range nnA {
			// Is B deleted?
			if _, ok := deleteSet[neighbour.NodeId]; ok {
				// Pull the neighbours of the deleted neighbour (B), and add them to the candidate set
				err := pc.WithPointNeighbours(neighbour, true, func(nnB []*cache.CachePoint) error {
					// Check if those neighbours are in the delete set, if not add them
					// to the candidate set. We do this check in case our neighbour has
					// neighbours that are being deleted too.
					for _, deletedPointNeighbour := range nnB {
						// Is C a valid candidate or is it deleted too?
						if _, ok := deleteSet[deletedPointNeighbour.NodeId]; !ok {
							candidateSet.AddPoint(deletedPointNeighbour)
						}
					}
					deletedNothing = false
					return nil
				})
				if err != nil {
					return fmt.Errorf("could not get deleted neighbour point neighbours: %w", err)
				}
			} else {
				// Nope, B is not deleted so we just add it to the candidate set
				candidateSet.AddPoint(neighbour)
			}
		}
		candidateSet.Sort()
		// ---------------------------
		s.robustPrune(point, candidateSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not get point neighbours: %w", err)
	}
	if deletedNothing {
		// No neighbours are to be deleted, something's up
		return fmt.Errorf("no neighbours to be deleted for point: %s", point.Id.String())
	}
	// ---------------------------
	return nil
}

// ---------------------------

// Attempts to remove the edges of the deleted points. This is done by scanning
// all the edges and removing the ones that point to a deleted point.
func (s *Shard) removeInboundEdges(pc cache.ReadWriteCache, deleteSet map[uint64]struct{}) error {
	// The scanning may not be efficient but it is correct. We can optimise this
	// in the future.
	// ---------------------------
	startTime := time.Now()
	toPrune, err := pc.EdgeScan(deleteSet)
	if err != nil {
		return fmt.Errorf("could not scan edges: %w", err)
	}
	log.Debug().Str("component", "shard").Int("deleteSetSize", len(deleteSet)).Str("duration", time.Since(startTime).String()).Msg("EdgeScan")
	// ---------------------------
	startTime = time.Now()
	for _, pointId := range toPrune {
		if err := s.pruneDeleteNeighbour(pc, pointId, deleteSet); err != nil {
			return fmt.Errorf("could not prune delete neighbour: %w", err)
		}
	}
	log.Debug().Str("component", "shard").Int("toPruneCount", len(toPrune)).Str("duration", time.Since(startTime).String()).Msg("PruneDeleteNeighbour")
	// ---------------------------
	return nil
}

// ---------------------------

func (s *Shard) DeletePoints(deleteSet map[uuid.UUID]struct{}) ([]uuid.UUID, error) {
	// ---------------------------
	deletedIds := make([]uuid.UUID, 0, len(deleteSet))
	deletedNodeIds := make(map[uint64]struct{}, len(deleteSet))
	// ---------------------------
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSBUCKETKEY)
		// ---------------------------
		nodeCounter, err := NewIdCounter(tx.Bucket(INTERNALBUCKETKEY), FREENODEIDSKEY, NEXTFREENODEIDKEY)
		if err != nil {
			return fmt.Errorf("could not create id counter: %w", err)
		}
		err = s.cacheManager.With(s.dbFile, b, func(pc cache.ReadWriteCache) error {
			// ---------------------------
			for pointId := range deleteSet {
				point, err := pc.GetPointByUUID(pointId)
				if err != nil {
					// If the point doesn't exist, we can skip it
					log.Debug().Err(err).Msg("could not get point for deletion")
					continue
				}
				point.Delete()
				deletedIds = append(deletedIds, pointId)
				deletedNodeIds[point.NodeId] = struct{}{}
				nodeCounter.FreeId(point.NodeId)
			}
			if len(deletedIds) == 0 {
				// No points to delete, we skip scanning
				return nil
			}
			// ---------------------------
			// Collect all the neighbours of the points to be deleted
			/* Initially we doubled downed on the assumption that more often than not
			 * there would be bidirectional edges between points. This is, however,
			 * not the case which leads to active edges to points that do not exist.
			 * During search that throws an error. There are three approaches:
				1. Scan all edges and delete the ones that point to a deleted point
				2. Prune optimistically of the neighbours of the deleted points,
				then during getPointNeighbours to check if the neighbour exists
				3. A midway where we mark the deleted points, ignore them
				during search and only do a full prune when it reaches say 10% of
				total size.
			   We are going with 1 for now to achieve correctness. The sharding
			   process means no single shard will be too large to cause a huge
			   performance hit. Each shard scan can be done in parallel too.  In the
			   future we can implement 3 by keeping track of the number of deleted
			   points and only doing a full prune when it reaches a certain
			   threshold. */
			if err := s.removeInboundEdges(pc, deletedNodeIds); err != nil {
				return fmt.Errorf("could not remove inbound edges: %w", err)
			}
			return pc.Flush()
		})
		if err != nil {
			return fmt.Errorf("could not run delete procedure: %w", err)
		}
		// ---------------------------
		// Update point count accordingly
		if err := changePointCount(tx, -len(deletedIds)); err != nil {
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
