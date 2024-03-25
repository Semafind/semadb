package vamana

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/utils"
)

// ---------------------------

// We set start ID to 1 in order to avoid the default zero value of uint64. For
// example, if we forget to set the nodeId, we don't want it to suddenly become
// the start node.
const STARTID = 1

// ---------------------------

type IndexVamana struct {
	distFn     distance.DistFunc
	parameters models.IndexVectorVamanaParameters
	pointCache cache.SharedPointCache
	maxNodeId  uint64
	logger     zerolog.Logger
}

func NewIndexVamana(name string, pc cache.SharedPointCache, params models.IndexVectorVamanaParameters, maxNodeId uint64) (*IndexVamana, error) {
	distFn, err := distance.GetDistanceFn(params.DistanceMetric)
	if err != nil {
		return nil, fmt.Errorf("could not get distance function: %w", err)
	}
	index := &IndexVamana{
		distFn:     distFn,
		parameters: params,
		pointCache: pc,
		maxNodeId:  maxNodeId,
		logger:     log.With().Str("component", "IndexVamana").Str("name", name).Logger(),
	}
	if err := index.setupStartNode(); err != nil {
		return nil, fmt.Errorf("could not setup start node: %w", err)
	}
	return index, nil
}

func (v *IndexVamana) setupStartNode() error {
	// ---------------------------
	if _, err := v.pointCache.GetPoint(STARTID); err == nil {
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
	if _, err := v.pointCache.SetPoint(randPoint); err != nil {
		return fmt.Errorf("could not set start point: %w", err)
	}
	return nil
}

func (v *IndexVamana) InsertUpdateDelete(ctx context.Context, points <-chan cache.GraphNode) <-chan error {
	errC := make(chan error, 1)
	go func() {
		errC <- v.insertUpdateDelete(ctx, v.pointCache, points)
		close(errC)
	}()
	return errC
}

func (v *IndexVamana) insertUpdateDelete(ctx context.Context, pc cache.SharedPointCache, pointQueue <-chan cache.GraphNode) error {
	// ---------------------------
	/* In this funky case there are actually multiple goroutines operating on
	 * the same cache. As opposed to multiple requests queuing to get access
	 * to the shared cache. Internal concurrency (workers) vs external
	 * concurrency (user requests). */
	numWorkers := runtime.NumCPU() - 1 // We leave 1 core for the main thread
	startTime := time.Now()
	insertQ := make(chan cache.GraphNode)
	errCs := make([]<-chan error, numWorkers)
	// ---------------------------
	for i := 0; i < numWorkers; i++ {
		errCs[i] = v.insertWorker(ctx, pc, insertQ)
	}
	insertErrC := utils.MergeErrorsWithContext(ctx, errCs...)
	// ---------------------------
	/* Update and delete operations do a full scan to prune nodes correctly.
	 * There is an approximate version we can implement, i.e. prune locally but
	 * on smaller graphs this may lead to disconnected nodes. We opt for going
	 * correctness initially. So to prune all the inbound edges to remove these
	 * nodes from the graph, we collect them and do a single scan. */
	updatedPoints := make([]cache.GraphNode, 0)
	deletedPoints := make([]*cache.CachePoint, 0)
	toRemoveInBoundNodeIds := make(map[uint64]struct{})
	// ---------------------------
	errC := utils.SinkWithContext(ctx, pointQueue, func(point cache.GraphNode) error {
		if point.NodeId == STARTID {
			return fmt.Errorf("cannot modify point with start id: %d", STARTID)
		}
		// What operation is this?
		cp, err := pc.GetPoint(point.NodeId)
		switch {
		case err == cache.ErrNotFound:
			// Insert
			insertQ <- point
		case err == nil && point.Vector != nil:
			// Update
			updatedPoints = append(updatedPoints, point)
			toRemoveInBoundNodeIds[point.NodeId] = struct{}{}
		case err == nil && point.Vector == nil:
			// Delete
			deletedPoints = append(deletedPoints, cp)
			toRemoveInBoundNodeIds[point.NodeId] = struct{}{}
		default:
			return err
		}
		return nil
	})
	if err := <-errC; err != nil {
		return fmt.Errorf("could not process point: %w", err)
	}
	// ---------------------------
	/* We don't want to interleave inbound edge pruning for update and delete
	 * while insert is happening. This may again lead to disconnected graphs. */
	close(insertQ)
	if err := <-insertErrC; err != nil {
		return fmt.Errorf("could not insert point: %w", err)
	}
	// ---------------------------
	/* Initially we doubled downed on the assumption that more often than not
	 * there would be bidirectional edges between points. This is, however,
	 * not the case which leads to active edges to points that do not exist.
	 * During search that throws an error and adds overhead to always check
	 * whether the node you are traversing to is valid, if it isn't maybe remove
	 * it. There are three approaches:
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
	if err := v.removeInboundEdges(pc, toRemoveInBoundNodeIds); err != nil {
		return fmt.Errorf("could not remove inbound edges: %w", err)
	}
	for _, cp := range deletedPoints {
		/* Mark as deleted. We do this here after the inbound edges have been
		 * removed because we don't want to remove nodes while insertion is
		 * potentially happening. Again we don't expect to have large number of
		 * deletions so this is single threaded. When a node is marked, it then
		 * gets deleted during a flush. */
		cp.Delete()
	}
	// ---------------------------
	/* The updated nodes are now re-inserted into the graph. We do this under the
	 * assumption that change the vector of a point will change its neighbours so
	 * it has to be recalculated by re-inserting.
	 *
	 * We also assume there won't be to many updates in the system so this part
	 * is single threaded for ease of implementation. One can re-use the insert
	 * workers above by implementing an idle check, i.e. instead of waiting for
	 * the workers to finish draining the insert channel and returning, you wait
	 * until they have drained but are idle. */
	for _, point := range updatedPoints {
		if err := v.insertSinglePoint(pc, point); err != nil {
			return fmt.Errorf("could not re-insert updated point: %w", err)
		}
	}
	// ---------------------------
	v.logger.Debug().Str("duration", time.Since(startTime).String()).Msg("IndexVamana- Write")
	// ---------------------------
	if err := context.Cause(ctx); err != nil {
		v.logger.Debug().Msg("IndexVamana - Insert - Context Done")
		return fmt.Errorf("insert context error: %w", err)
	}
	// ---------------------------
	return nil
}

func (v *IndexVamana) Search(ctx context.Context, query []float32, limit int) ([]models.SearchResult, error) {
	startTime := time.Now()
	searchSet, _, err := greedySearch(v.pointCache, query, limit, v.parameters.SearchSize, v.distFn, v.maxNodeId)
	if err != nil {
		return nil, fmt.Errorf("could not perform graph search: %w", err)
	}
	v.logger.Debug().Str("component", "shard").Str("duration", time.Since(startTime).String()).Msg("SearchPoints - GreedySearch")
	results := make([]models.SearchResult, 0, min(len(searchSet.items), limit))
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
	return results, err
}
