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

type IndexVamana struct {
	cacheName    string
	distFn       distance.DistFunc
	parameters   models.IndexVectorVamanaParameters
	maxNodeId    uint64
	cacheManager *cache.Manager
	bucket       diskstore.Bucket
	logger       zerolog.Logger
}

func NewIndexVamana(cacheName string, cacheManager *cache.Manager, bucket diskstore.Bucket, parameters models.IndexVectorVamanaParameters, maxNodeId uint64) (*IndexVamana, error) {
	distFn, err := distance.GetDistanceFn(parameters.DistanceMetric)
	if err != nil {
		return nil, fmt.Errorf("could not get distance function: %w", err)
	}
	index := &IndexVamana{
		cacheName:    cacheName,
		distFn:       distFn,
		parameters:   parameters,
		maxNodeId:    maxNodeId,
		cacheManager: cacheManager,
		bucket:       bucket,
		logger:       log.With().Str("component", "IndexVamana").Str("name", cacheName).Logger(),
	}
	return index, nil
}

func (v *IndexVamana) setupStartNode(pc cache.ReadWriteCache) error {
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
	randPoint := cache.GraphNode{
		NodeId: STARTID,
		Vector: randVector,
	}
	if _, err := pc.SetPoint(randPoint); err != nil {
		return fmt.Errorf("could not set start point: %w", err)
	}
	return nil
}

func (v *IndexVamana) Insert(ctx context.Context, points <-chan cache.GraphNode) error {
	return v.cacheManager.With(v.cacheName, v.bucket, func(pc cache.ReadWriteCache) error {
		v.setupStartNode(pc)
		return v.insert(ctx, pc, points)
	})
}

func (v *IndexVamana) Update(ctx context.Context, points <-chan cache.GraphNode) error {
	return v.cacheManager.With(v.cacheName, v.bucket, func(pc cache.ReadWriteCache) error {
		v.setupStartNode(pc)
		return v.update(ctx, pc, points)
	})
}

func (v *IndexVamana) Delete(ctx context.Context, points <-chan cache.GraphNode) error {
	return v.cacheManager.With(v.cacheName, v.bucket, func(pc cache.ReadWriteCache) error {
		return v.delete(ctx, pc, points)
	})
}

func (v *IndexVamana) insert(ctx context.Context, pc cache.ReadWriteCache, pointQueue <-chan cache.GraphNode) error {
	/* We create our context for the workers. The way contexts work in go
	 * language means if the parent context is cancelled all work here is
	 * stopped, if a worker has an error than we cancel our context. It may very
	 * well be that the parent context is eventually cancelled due to an error
	 * here but we make no assumptions about that. */
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// ---------------------------
	/* In this funky case there are actually multiple goroutines operating on
	 * the same cache. As opposed to multiple requests queuing to get access
	 * to the shared cache. Internal concurrency (workers) vs external
	 * concurrency (user requests). */
	// ---------------------------
	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU() - 1 // We leave 1 core for the main thread
	startTime := time.Now()
	// ---------------------------
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := v.insertWorker(ctx, pc, pointQueue); err != nil {
				cancel(err)
			}
		}()
	}
	wg.Wait()
	v.logger.Debug().Str("duration", time.Since(startTime).String()).Msg("IndexVamana- Insert")
	// ---------------------------
	if err := context.Cause(ctx); err != nil {
		v.logger.Debug().Msg("IndexVamana - Insert - Context Done")
		return fmt.Errorf("insert context error: %w", err)
	}
	// ---------------------------
	return nil
}

// Updates the index by first pruning the inbound edges of the updated points and
// then re-inserting the updated points.
func (v *IndexVamana) update(ctx context.Context, pc cache.ReadWriteCache, pointQueue <-chan cache.GraphNode) error {
	// ---------------------------
	updatedPoints := make([]cache.GraphNode, 0, 100)
	updatedNodeIds := make(map[uint64]struct{})
	/* We collect all updates from the channel so we can perform prune in one
	 * go across all the updated points. */
outer:
	for {
		select {
		case <-ctx.Done():
			// We are returning nil here because we haven't done anything to
			// affect the index yet
			return nil
		case point, ok := <-pointQueue:
			if !ok {
				break outer
			}
			if _, ok := updatedNodeIds[point.NodeId]; !ok {
				updatedNodeIds[point.NodeId] = struct{}{}
				updatedPoints = append(updatedPoints, point)
			}
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
	return nil
}

func (v *IndexVamana) delete(ctx context.Context, pc cache.ReadWriteCache, pointQueue <-chan cache.GraphNode) error {
	deletedNodeIds := make(map[uint64]struct{})
	/* We collect all ids from the channel so we can perform prune in one
	 * go across all the deleted points. */
outer:
	for {
		select {
		case <-ctx.Done():
			// We return an error because we've been cp.Delete()ing the
			// points, so the cache could end up in an inconsistent state.
			// By returning error we signal to our caller that we did not
			// complete the operation and as such the cache manager may
			// scrap the cache.
			return fmt.Errorf("context done while deleting points: %w", context.Cause(ctx))
		case point, ok := <-pointQueue:
			if !ok {
				// Channel is closed
				break outer
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
	}
	if len(deletedNodeIds) == 0 {
		return nil
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
	return nil
}

func (v *IndexVamana) Search(ctx context.Context, query []float32, limit int) ([]models.SearchResult, error) {
	var results []models.SearchResult
	err := v.cacheManager.WithReadOnly(v.cacheName, v.bucket, func(pc cache.ReadOnlyCache) error {
		startTime := time.Now()
		searchSet, _, err := greedySearch(pc, query, limit, v.parameters.SearchSize, v.distFn, v.maxNodeId)
		if err != nil {
			return fmt.Errorf("could not perform graph search: %w", err)
		}
		v.logger.Debug().Str("component", "shard").Str("duration", time.Since(startTime).String()).Msg("SearchPoints - GreedySearch")
		results = make([]models.SearchResult, 0, min(len(searchSet.items), limit))
		for _, elem := range searchSet.items {
			if elem.point.NodeId == STARTID {
				continue
			}
			if len(results) >= limit {
				break
			}
			sr := models.SearchResult{
				NodeId:   elem.point.NodeId,
				Distance: &elem.distance,
			}
			results = append(results, sr)
		}
		return nil
	})
	return results, err
}
