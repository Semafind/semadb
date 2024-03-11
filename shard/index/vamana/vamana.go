package vamana

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
)

// ---------------------------

// We set start ID to 1 in order to avoid the default zero value of uint64. For
// example, if we forget to set the nodeId, we don't want it to suddenly become
// the start node.
const STARTID = 1

// ---------------------------

type indexVamana struct {
	cacheName    string
	distFn       distance.DistFunc
	parameters   models.IndexVectorVamanaParameters
	cacheManager *cache.Manager
	maxNodeId    uint
	logger       zerolog.Logger
}

func NewIndexVamana(cacheName string, parameters models.IndexVectorVamanaParameters, cacheManager *cache.Manager, maxNodeId uint) (*indexVamana, error) {
	distFn, err := distance.GetDistanceFn(parameters.DistanceMetric)
	if err != nil {
		return nil, fmt.Errorf("could not get distance function: %w", err)
	}
	index := &indexVamana{
		cacheName:    cacheName,
		distFn:       distFn,
		parameters:   parameters,
		cacheManager: cacheManager,
		maxNodeId:    maxNodeId,
		logger:       log.With().Str("component", "IndexVamana").Str("name", cacheName).Logger(),
	}
	return index, nil
}

func (v *indexVamana) setupStartNode(pc cache.ReadWriteCache) error {
	// ---------------------------
	if _, err := pc.GetPoint(STARTID); err == nil {
		return nil
	}
	// ---------------------------
	// Create random unit vector of size n
	randVector := make([]float32, v.parameters.VectorSize)
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
	// Create start point
	randPoint := cache.ShardPoint{
		NodeId: STARTID,
		Vector: randVector,
	}
	if _, err := pc.SetPoint(randPoint); err != nil {
		return fmt.Errorf("could not set start point: %w", err)
	}
	return nil
}

func (v *indexVamana) Insert(ctx context.Context, cancel context.CancelCauseFunc, bucket diskstore.Bucket, pointQueue <-chan cache.ShardPoint) error {
	return v.cacheManager.With(v.cacheName, bucket, func(pc cache.ReadWriteCache) error {
		// ---------------------------
		if err := v.setupStartNode(pc); err != nil {
			return fmt.Errorf("could not setup start node: %w", err)
		}
		// ---------------------------
		/* In this funky case there are actually multiple goroutines operating on
		 * the same cache. As opposed to multiple requests queuing to get access
		 * to the shared cache. Internal concurrency (workers) vs external
		 * concurrency (user requests). */
		// ---------------------------
		var wg sync.WaitGroup
		numWorkers := runtime.NumCPU() * 3 / 4 // We leave some cores for other tasks
		startTime := time.Now()
		// ---------------------------
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go v.insertWorker(ctx, pc, pointQueue, &wg, cancel)
		}
		wg.Wait()
		v.logger.Debug().Str("duration", time.Since(startTime).String()).Msg("IndexVamana- Insert")
		// ---------------------------
		if ctx.Err() != nil {
			v.logger.Debug().Msg("IndexVamana - Insert - Context Done")
			// We are returning nil here because it is not our error, the context
			// was cancelled. We are just checking to avoid further work here.
			return nil
		}
		// ---------------------------
		startTime = time.Now()
		if err := pc.Flush(); err != nil {
			return fmt.Errorf("could not flush point cache: %w", err)
		}
		// ---------------------------
		v.logger.Debug().Str("duration", time.Since(startTime).String()).Msg("IndexVamana - Flush")
		// ---------------------------
		return nil
	})
}

// Updates the index by first pruning the inbound edges of the updated points and
// then re-inserting the updated points.
func (v *indexVamana) Update(ctx context.Context, cancel context.CancelCauseFunc, bucket diskstore.Bucket, pointQueue <-chan cache.ShardPoint) error {
	return v.cacheManager.With(v.cacheName, bucket, func(pc cache.ReadWriteCache) error {
		// ---------------------------
		updatedPoints := make([]cache.ShardPoint, 0, 100)
		updatedNodeIds := make(map[uint64]struct{})
		/* We collect all updates from the channel so we can perform prune in one
		 * go across all the updated points. */
		for point := range pointQueue {
			if ctx.Err() != nil {
				return nil
			}
			if _, ok := updatedNodeIds[point.NodeId]; !ok {
				updatedNodeIds[point.NodeId] = struct{}{}
				updatedPoints = append(updatedPoints, point)
			}
		}
		// ---------------------------
		// This is a lot more efficient than pruning each point as it arrives as
		// it scans the graph once.
		if err := v.removeInboundEdges(pc, updatedNodeIds); err != nil {
			return fmt.Errorf("could not remove inbound edges: %w", err)
		}
		// ---------------------------
		// Now the pruning is complete, we can re-insert the updated points
		for _, point := range updatedPoints {
			if ctx.Err() != nil {
				return nil
			}
			if err := v.insertSinglePoint(pc, point); err != nil {
				return fmt.Errorf("could not re-insert updated point: %w", err)
			}
		}
		// ---------------------------
		return pc.Flush()
	})
}

func (v *indexVamana) Delete(ctx context.Context, cancel context.CancelCauseFunc, bucket diskstore.Bucket, pointQueue <-chan cache.ShardPoint) error {
	return v.cacheManager.With(v.cacheName, bucket, func(pc cache.ReadWriteCache) error {
		deletedNodeIds := make(map[uint64]struct{})
		/* We collect all ids from the channel so we can perform prune in one
		 * go across all the deleted points. */
		for point := range pointQueue {
			if ctx.Err() != nil {
				return nil
			}
			deletedNodeIds[point.NodeId] = struct{}{}
			cp, err := pc.GetPoint(point.NodeId)
			if err != nil {
				return fmt.Errorf("could not get point from cache for deletion: %w", err)
			}
			// This deletes the point from the cache. When the cache is flushed
			// it is deleted from the diskstore.
			cp.Delete()
		}
		// ---------------------------
		// Collect all the neighbours of the points to be deleted
		/* Initially we doubled downed on the assumption that more often than not
		 * there would be bidirectional edges between points. This is, however,
		 * not the case which leads to active edges to points that do not exist.
		 * During search that throws an error. There are three approaches:
		 *
		 * 1. Scan all edges and delete the ones that point to a deleted point
		 * 2. Prune optimistically of the neighbours of the deleted points,
		 *    then during getPointNeighbours to check if the neighbour exists
		 * 3. A midway where we mark the deleted points, ignore them
		 *    during search and only do a full prune when it reaches say 10% of
		 *    total size.
		 *
		 * We are going with 1 for now to achieve correctness. The sharding process
		 * means no single shard will be too large to cause a huge performance hit.
		 * Each shard scan can be done in parallel too.  In the future we can
		 * implement 3 by keeping track of the number of deleted points and only
		 * doing a full prune when it reaches a certain threshold.
		 */
		if err := v.removeInboundEdges(pc, deletedNodeIds); err != nil {
			return fmt.Errorf("could not remove inbound edges: %w", err)
		}
		return pc.Flush()
	})
}

type SearchResult struct {
	NodeId   uint64
	Distance float32
}

func (v *indexVamana) Search(ctx context.Context, cancel context.CancelCauseFunc, bucket diskstore.ReadOnlyBucket, query []float32, limit int) ([]SearchResult, error) {
	var results []SearchResult
	err := v.cacheManager.WithReadOnly(v.cacheName, bucket, func(pc cache.ReadOnlyCache) error {
		startTime := time.Now()
		searchSet, _, err := greedySearch(pc, query, limit, v.parameters.SearchSize, v.distFn, v.maxNodeId)
		v.logger.Debug().Str("component", "shard").Str("duration", time.Since(startTime).String()).Msg("SearchPoints - GreedySearch")
		if err != nil {
			return fmt.Errorf("could not perform graph search: %w", err)
		}
		results = make([]SearchResult, 0, min(len(searchSet.items), limit))
		for _, elem := range searchSet.items {
			if elem.point.NodeId == STARTID {
				continue
			}
			if len(results) >= limit {
				break
			}
			results = append(results, SearchResult{NodeId: elem.point.NodeId, Distance: elem.distance})
		}
		return nil
	})
	return results, err
}
