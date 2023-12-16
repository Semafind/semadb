package shard

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/utils"
	"go.etcd.io/bbolt"
)

type Shard struct {
	db         *bbolt.DB
	collection models.Collection
	distFn     distance.DistFunc
	startId    uuid.UUID
}

var POINTSKEY = []byte("points")
var INTERNALKEY = []byte("internal")
var STARTIDKEY = []byte("startId")
var POINTCOUNTKEY = []byte("pointCount")

func NewShard(dbfile string, collection models.Collection) (*Shard, error) {
	// ---------------------------
	db, err := bbolt.Open(dbfile, 0644, &bbolt.Options{Timeout: 1 * time.Minute})
	if err != nil {
		return nil, fmt.Errorf("could not open shard db: %w", err)
	}
	// ---------------------------
	var startId uuid.UUID
	err = db.Update(func(tx *bbolt.Tx) error {
		// ---------------------------
		// Setup buckets
		bPoints, err := tx.CreateBucketIfNotExists(POINTSKEY)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		bInternal, err := tx.CreateBucketIfNotExists(INTERNALKEY)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
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
			// ---------------------------
			// Create start point
			randPoint := ShardPoint{
				Point: models.Point{
					Id:     uuid.New(),
					Vector: randVector,
				},
			}
			log.Debug().Str("id", randPoint.Id.String()).Msg("creating start point")
			if err := setPoint(bPoints, randPoint); err != nil {
				return fmt.Errorf("could not set start point: %w", err)
			}
			if err := bInternal.Put(STARTIDKEY, randPoint.Id[:]); err != nil {
				return fmt.Errorf("could not set start point id: %w", err)
			}
			startId = randPoint.Id
			// ---------------------------
		} else {
			startId, err = uuid.FromBytes(sid)
			if err != nil {
				return fmt.Errorf("could not parse start point: %w", err)
			}
			log.Debug().Str("id", startId.String()).Msg("found start point")
		}
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
	return &Shard{
		db:         db,
		collection: collection,
		distFn:     distFn,
		startId:    startId,
	}, nil
}

func (s *Shard) Close() error {
	return s.db.Close()
}

func (s *Shard) Backup(backupFrequency, backupCount int) error {
	return utils.BackupBBolt(s.db, backupFrequency, backupCount)
}

// ---------------------------

func changePointCount(tx *bbolt.Tx, change int64) error {
	bInternal := tx.Bucket(INTERNALKEY)
	// ---------------------------
	countBytes := bInternal.Get(POINTCOUNTKEY)
	var count int64
	if countBytes != nil {
		count = bytesToInt64(countBytes)
	}
	// ---------------------------
	count += change
	// ---------------------------
	countBytes = int64ToBytes(count)
	if err := bInternal.Put(POINTCOUNTKEY, countBytes); err != nil {
		return fmt.Errorf("could not change point count: %w", err)
	}
	return nil
}

type shardInfo struct {
	PointCount int64
	Allocated  int64 // Bytes allocated for points bucket
	InUse      int64 // Bytes in use for points bucket
}

func (s *Shard) Info() (si shardInfo, err error) {
	err = s.db.View(func(tx *bbolt.Tx) error {
		bInternal := tx.Bucket(INTERNALKEY)
		// ---------------------------
		countBytes := bInternal.Get(POINTCOUNTKEY)
		if countBytes != nil {
			si.PointCount = bytesToInt64(countBytes)
		}
		// ---------------------------
		pStats := tx.Bucket(POINTSKEY).Stats()
		si.Allocated = int64(pStats.BranchAlloc + pStats.LeafAlloc)
		si.InUse = int64(pStats.BranchInuse + pStats.LeafInuse + pStats.InlineBucketInuse)
		return nil
	})
	return
}

// ---------------------------

func (s *Shard) insertSinglePoint(pc *PointCache, startPointId uuid.UUID, shardPoint ShardPoint) error {
	// ---------------------------
	point := pc.SetPoint(shardPoint)
	// ---------------------------
	_, visitedSet, err := s.greedySearch(pc, startPointId, point.Vector, 1, s.collection.Parameters.SearchSize)
	if err != nil {
		return fmt.Errorf("could not greedy search: %w", err)
	}
	// ---------------------------
	s.robustPrune(point, visitedSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
	// ---------------------------
	// Add the bi-directional edges
	for _, nId := range point.Edges {
		n, err := pc.GetPoint(nId)
		if err != nil {
			return fmt.Errorf("could not get neighbour point: %w", err)
		}
		// While we are adding the bi-directional edges, we need exclusive
		// access to ensure other goroutines don't modify the edges while we are
		// dealing with them.
		n.mu.Lock()
		if len(n.Edges)+1 > s.collection.Parameters.DegreeBound {
			// ---------------------------
			nn, err := pc.GetPointNeighbours(n)
			if err != nil {
				n.mu.Unlock()
				return fmt.Errorf("could not get neighbour neighbours: %w", err)
			}
			// We need to prune the neighbour as well to keep the degree bound
			candidateSet := NewDistSet(n.Vector, len(n.Edges)+1, s.distFn)
			candidateSet.AddPoint(nn...)
			candidateSet.AddPoint(point)
			candidateSet.Sort()
			s.robustPrune(n, candidateSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
		} else {
			// ---------------------------
			// Add the edge
			n.AddNeighbour(point)
		}
		n.mu.Unlock()
	}
	// ---------------------------
	return nil
}

// ---------------------------

func (s *Shard) InsertPoints(points []models.Point) error {
	// ---------------------------
	// profileFile, _ := os.Create("dump/cpu.prof")
	// defer profileFile.Close()
	// pprof.StartCPUProfile(profileFile)
	// defer pprof.StopCPUProfile()
	// ---------------------------
	log.Debug().Str("component", "shard").Int("count", len(points)).Msg("InsertPoints")
	// ---------------------------
	// Insert points
	// bar := progressbar.Default(int64(len(points)))
	startTime := time.Now()
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSKEY)
		pc := NewPointCache(b)
		// ---------------------------
		// Setup parallel insert routine
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		jobQueue := make(chan ShardPoint, len(points))
		// Start the workers and start inserting points.
		numWorkers := runtime.NumCPU() * 3 / 4 // We leave some cores for other tasks
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for point := range jobQueue {
					// Check if we've been cancelled
					select {
					case <-ctx.Done():
						return
					default:
					}
					// If the point exists, we can't re-insert it. This is
					// actually an error because the edges will be wrong in the
					// graph. It needs to be updated instead. We can potentially
					// do it here (do an update instead of insert) but the API
					// design migh be inconsistent as it will then depend
					// whether a point is re-assigned to the same shard during
					// insertion when there are multiple shards. We are
					// returning an error here to force the user to update the
					// point instead which handles the multiple shard case.
					if _, err := pc.GetPoint(point.Id); err == nil {
						cancel(fmt.Errorf("point already exists: %s", point.Id.String()))
						return
					}
					// Insert the point
					if err := s.insertSinglePoint(pc, s.startId, point); err != nil {
						cancel(err)
						return
					}
				}
			}()
		}
		// Submit the jobs
		for point := range points {
			jobQueue <- ShardPoint{Point: points[point]}
		}
		close(jobQueue)
		wg.Wait()
		finalErr := context.Cause(ctx)
		if finalErr != nil {
			return finalErr
		}
		log.Debug().Int("points", len(points)).Float64("perf(p/s)", float64(len(points))/time.Since(startTime).Seconds()).Msg("InsertPoints Points / Second")
		// ---------------------------
		// Update point count accordingly
		if err := changePointCount(tx, int64(len(points))); err != nil {
			return fmt.Errorf("could not update point count for insertion: %w", err)
		}
		// ---------------------------
		startTime = time.Now()
		err := pc.Flush()
		log.Debug().Str("component", "shard").Str("duration", time.Since(startTime).String()).Msg("InsertPoints - Flush")
		return err
	})
	if err != nil {
		log.Debug().Err(err).Msg("could not insert points")
		return fmt.Errorf("could not insert points: %w", err)
	}
	// ---------------------------
	// bar.Close()
	// ---------------------------
	return nil
}

// ---------------------------

func (s *Shard) UpdatePoints(points []models.Point) ([]uuid.UUID, error) {
	log.Debug().Str("component", "shard").Int("count", len(points)).Msg("UpdatePoints")
	// ---------------------------
	// Note that some points may not exist, so we need to take care of that
	// throughout this function
	updateSet := make(map[uuid.UUID]struct{}, len(points))
	// ---------------------------
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSKEY)
		pc := NewPointCache(b)
		// ---------------------------
		// First we check if the points exist
		for _, point := range points {
			if _, err := pc.GetPoint(point.Id); err != nil {
				// Point does not exist, or we can't access it at the moment
				continue
			}
			updateSet[point.Id] = struct{}{}
		}
		if len(updateSet) == 0 {
			// No points to update, we skip the process
			return nil
		}
		// ---------------------------
		// We assume the updated points have their vectors changed. We can in
		// the future handle metadata updates specifically so all this
		// re-indexing doesn't happen.
		if err := s.removeInboundEdges(pc, updateSet); err != nil {
			return fmt.Errorf("could not remove inbound edges: %w", err)
		}
		// ---------------------------
		// Now the pruning is complete, we can re-insert the points again
		for _, point := range points {
			// Check it is in the updateSet
			if _, ok := updateSet[point.Id]; !ok {
				continue
			}
			if err := s.insertSinglePoint(pc, s.startId, ShardPoint{Point: point}); err != nil {
				return fmt.Errorf("could not re-insert point: %w", err)
			}
		}
		// ---------------------------
		return pc.Flush()
	})
	if err != nil {
		log.Debug().Err(err).Msg("could not update points")
		return nil, fmt.Errorf("could not update points: %w", err)
	}
	// ---------------------------
	updatedIds := make([]uuid.UUID, len(updateSet))
	i := 0
	for pointId := range updateSet {
		updatedIds[i] = pointId
		i++
	}
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
		b := tx.Bucket(POINTSKEY)
		pc := NewPointCache(b)
		searchSet, _, err := s.greedySearch(pc, s.startId, query, k, s.collection.Parameters.SearchSize)
		if err != nil {
			return fmt.Errorf("could not perform graph search: %w", err)
		}
		// ---------------------------
		// Clean up results and backfill metadata
		searchSet.KeepFirstK(k)
		for _, distElem := range searchSet.items {
			point := distElem.point.Point
			// We skip the start point
			if point.Id == s.startId {
				continue
			}
			if len(results) >= k {
				break
			}
			mdata, err := getPointMetadata(b, point.Id)
			if err != nil {
				return fmt.Errorf("could not get point metadata: %w", err)
			}
			// We copy here because the byte slice is only valid for the
			// lifetime of the transaction
			if mdata != nil {
				point.Metadata = make([]byte, len(mdata))
				copy(point.Metadata, mdata)
			}
			sp := SearchPoint{
				Point:    point,
				Distance: distElem.distance}
			results = append(results, sp)
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

func (s *Shard) pruneDeleteNeighbour(pc *PointCache, id uuid.UUID, deleteSet map[uuid.UUID]struct{}) error {
	// ---------------------------
	point, err := pc.GetPoint(id)
	if err != nil {
		return fmt.Errorf("could not get point: %w", err)
	}
	// ---------------------------
	pointNeighbours, err := pc.GetPointNeighbours(point)
	if err != nil {
		return fmt.Errorf("could not get point neighbours: %w", err)
	}
	// ---------------------------
	// We are going to build a new candidate list of neighbours and then robust
	// prune it
	candidateSet := NewDistSet(point.Vector, len(point.Edges)*2, s.distFn)
	deletedNothing := true
	for _, neighbour := range pointNeighbours {
		if _, ok := deleteSet[neighbour.Id]; ok {
			// Pull the neighbours of the deleted neighbour, and add them to the candidate set
			deletedPointNeighbours, err := pc.GetPointNeighbours(neighbour)
			if err != nil {
				return fmt.Errorf("could not get deleted point neighbours: %w", err)
			}
			// Check if those neighbours are in the delete set, if not add them
			// to the candidate set. We do this check in case our neighbour has
			// neighbours that are being deleted too.
			for _, deletedPointNeighbour := range deletedPointNeighbours {
				if _, ok := deleteSet[deletedPointNeighbour.Id]; !ok {
					candidateSet.AddPoint(deletedPointNeighbour)
				}
			}
			deletedNothing = false
		} else {
			candidateSet.AddPoint(neighbour)
		}
	}
	if deletedNothing {
		// No neighbours are to be deleted, something's up
		return fmt.Errorf("no neighbours to be deleted for point: %s", point.Id.String())
	}
	candidateSet.Sort()
	// ---------------------------
	s.robustPrune(point, candidateSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
	// ---------------------------
	return nil
}

// ---------------------------

// Attempts to remove the edges of the deleted points. This is done by scanning
// all the edges and removing the ones that point to a deleted point.
func (s *Shard) removeInboundEdges(pc *PointCache, deleteSet map[uuid.UUID]struct{}) error {
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
	// We don't expect to delete all the points because some may be in other
	// shards. So we start with a lower capacity for the array.
	deletedIds := make([]uuid.UUID, 0, len(deleteSet)/2)
	// ---------------------------
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSKEY)
		pc := NewPointCache(b)
		// ---------------------------
		for pointId := range deleteSet {
			point, err := pc.GetPoint(pointId)
			if err != nil {
				// If the point doesn't exist, we can skip it
				log.Debug().Err(err).Msg("could not get point for deletion")
				continue
			}
			point.isDeleted = true
			deletedIds = append(deletedIds, pointId)
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
		if err := s.removeInboundEdges(pc, deleteSet); err != nil {
			return fmt.Errorf("could not remove inbound edges: %w", err)
		}
		// ---------------------------
		// Update point count accordingly
		if err := changePointCount(tx, -int64(len(deletedIds))); err != nil {
			return fmt.Errorf("could not change point count for deletion: %w", err)
		}
		// ---------------------------
		return pc.Flush()
	})
	if err != nil {
		return nil, fmt.Errorf("could not delete points: %w", err)
	}
	return deletedIds, nil
}

// ---------------------------
