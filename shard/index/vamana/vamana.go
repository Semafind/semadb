package vamana

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/vectorstore"
	"github.com/semafind/semadb/utils"
)

// ---------------------------

// We set start ID to 1 in order to avoid the default zero value of uint64. For
// example, if we forget to set the nodeId, we don't want it to suddenly become
// the start node.
const STARTID = 1

const (
	MAXNODEIDKEY = "_vamanaMaxNodeId"
)

// ---------------------------

type IndexVamana struct {
	parameters models.IndexVectorVamanaParameters
	// ---------------------------
	vecStore  vectorstore.VectorStore
	nodeStore *cache.ItemCache[uint64, *graphNode]
	/* Maximum node id used in the index. This is actually used for visit sets to
	 * determine the size of the bitset or fallback to a map. It is not the
	 * counter from which new Ids are generated. That is handled by the id
	 * counter in the shard. We store this here to avoid having to read from disk
	 * every time we need to create a new visit set. It doesn't need to be exact
	 * either, bitsets can resize if we get it wrong but we try to keep it in
	 * sync anyway. */
	maxNodeId atomic.Uint64
	// ---------------------------
	bucket diskstore.Bucket
	logger zerolog.Logger
}

func NewIndexVamana(name string, params models.IndexVectorVamanaParameters, bucket diskstore.Bucket) (*IndexVamana, error) {
	logger := log.With().Str("component", "IndexVamana").Str("name", name).Logger()
	// ---------------------------
	index := &IndexVamana{
		parameters: params,
		nodeStore:  cache.NewItemCache[uint64, *graphNode](bucket),
		bucket:     bucket,
		logger:     logger,
	}
	// ---------------------------
	vstore, err := vectorstore.New(params.Quantizer, bucket, params.DistanceMetric, int(params.VectorSize))
	if err != nil {
		return nil, fmt.Errorf("could not create vector store: %w", err)
	}
	index.vecStore = vstore
	// ---------------------------
	if err := index.setupStartNode(); err != nil {
		return nil, fmt.Errorf("could not setup start node: %w", err)
	}
	// ---------------------------
	// Max node id from bucket
	if maxNodeIdVal := bucket.Get([]byte(MAXNODEIDKEY)); maxNodeIdVal != nil {
		index.maxNodeId.Store(conversion.BytesToUint64(maxNodeIdVal))
	}
	logger.Debug().Uint64("maxNodeId", index.maxNodeId.Load()).Msg("IndexVamana- New")
	// ---------------------------
	return index, nil
}

func (v *IndexVamana) SizeInMemory() int64 {
	return v.vecStore.SizeInMemory() + v.nodeStore.SizeInMemory()
}

func (v *IndexVamana) UpdateBucket(bucket diskstore.Bucket) {
	v.bucket = bucket
	v.vecStore.UpdateBucket(bucket)
	v.nodeStore.UpdateBucket(bucket)
}

func (v *IndexVamana) setupStartNode() error {
	// ---------------------------
	if v.vecStore.Exists(STARTID) {
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
	if _, err := v.vecStore.Set(STARTID, randVector); err != nil {
		return fmt.Errorf("could not set start point: %w", err)
	}
	startNode := &graphNode{
		Id: STARTID,
	}
	v.nodeStore.Put(STARTID, startNode)
	return nil
}

type IndexVectorChange struct {
	Id     uint64
	Vector []float32
}

func (v *IndexVamana) InsertUpdateDelete(ctx context.Context, points <-chan IndexVectorChange) <-chan error {
	errC := make(chan error, 1)
	go func() {
		errC <- v.insertUpdateDelete(ctx, points)
		close(errC)
	}()
	return errC
}

func (v *IndexVamana) insertUpdateDelete(ctx context.Context, pointQueue <-chan IndexVectorChange) error {
	// ---------------------------
	startTime := time.Now()
	// ---------------------------
	/* Update and delete operations do a full scan to prune nodes correctly.
	 * There is an approximate version we can implement, i.e. prune locally but
	 * on smaller graphs this may lead to disconnected nodes. We opt for going
	 * correctness initially. So to prune all the inbound edges to remove these
	 * nodes from the graph, we collect them and do a single scan. */
	updatedPoints := make([]IndexVectorChange, 0)
	deletedPointsIds := make([]uint64, 0)
	toRemoveInBoundNodeIds := make(map[uint64]struct{})
	// ---------------------------
	insertQ, distributeErrC := utils.TransformWithContext(ctx, pointQueue, func(point IndexVectorChange) (out IndexVectorChange, skip bool, err error) {
		if point.Id == STARTID {
			err = fmt.Errorf("cannot modify point with start id: %d", STARTID)
			return
		}
		if point.Id == 0 {
			err = fmt.Errorf("invalid point id: %d", point.Id)
			return
		}
		// What operation is this?
		exists := v.vecStore.Exists(point.Id)
		switch {
		case !exists && point.Vector == nil:
			// Skip, nothing to do
			skip = true
		case !exists && point.Vector != nil:
			// Insert
			if point.Id > v.maxNodeId.Load() {
				v.maxNodeId.Store(point.Id)
			}
			skip = false
			out = point
		case exists && point.Vector != nil:
			// Update
			updatedPoints = append(updatedPoints, point)
			toRemoveInBoundNodeIds[point.Id] = struct{}{}
			skip = true
		case exists && point.Vector == nil:
			// Delete
			deletedPointsIds = append(deletedPointsIds, point.Id)
			toRemoveInBoundNodeIds[point.Id] = struct{}{}
			skip = true
		default:
			err = fmt.Errorf("unknown operation for point: %d", point.Id)
		}
		return
	})
	/* In this funky case there are actually multiple goroutines operating on
	 * the same cache. As opposed to multiple requests queuing to get access
	 * to the shared cache. Internal concurrency (workers) vs external
	 * concurrency (user requests). */
	numWorkers := runtime.NumCPU() - 1 // We leave 1 core for the main thread
	errCs := make([]<-chan error, numWorkers+1)
	// ---------------------------
	for i := 0; i < numWorkers; i++ {
		errCs[i] = v.insertWorker(ctx, insertQ)
	}
	errCs[numWorkers] = distributeErrC
	/* We don't want to interleave inbound edge pruning for update and delete
	 * while insert is happening. This may again lead to disconnected graphs. */
	if err := <-utils.MergeErrorsWithContext(ctx, errCs...); err != nil {
		return fmt.Errorf("could not distribute or insert points: %w", err)
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
	if len(toRemoveInBoundNodeIds) > 0 {
		if err := v.removeInboundEdges(toRemoveInBoundNodeIds); err != nil {
			return fmt.Errorf("could not remove inbound edges: %w", err)
		}
	}
	/* Mark as deleted. We do this here after the inbound edges have been
	 * removed because we don't want to remove nodes while insertion is
	 * potentially happening. Again we don't expect to have large number of
	 * deletions so this is single threaded. When a node is marked, it then
	 * gets deleted during a flush. */
	if err := v.vecStore.Delete(deletedPointsIds...); err != nil {
		return fmt.Errorf("could not delete points from vector store: %w", err)
	}
	if err := v.nodeStore.Delete(deletedPointsIds...); err != nil {
		return fmt.Errorf("could not delete points from node store: %w", err)
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
		if err := v.insertSinglePoint(point); err != nil {
			return fmt.Errorf("could not re-insert updated point: %w", err)
		}
	}
	// ---------------------------
	v.logger.Debug().Str("duration", time.Since(startTime).String()).Msg("IndexVamana- Write")
	// ---------------------------
	// Check vector store optimisation, this may include quantisation etc.
	if err := v.vecStore.Fit(); err != nil {
		return fmt.Errorf("could not fit vector store: %w", err)
	}
	// ---------------------------
	return v.flush()
}

func (v *IndexVamana) flush() error {
	if err := v.vecStore.Flush(); err != nil {
		return fmt.Errorf("could not flush vector store: %w", err)
	}
	if err := v.nodeStore.Flush(); err != nil {
		return fmt.Errorf("could not flush node store: %w", err)
	}
	if err := v.bucket.Put([]byte(MAXNODEIDKEY), conversion.Uint64ToBytes(v.maxNodeId.Load())); err != nil {
		return fmt.Errorf("could not set max node id: %w", err)
	}
	return nil
}

func (v *IndexVamana) Search(ctx context.Context, query models.SearchVectorVamanaOptions, filter *roaring64.Bitmap) (*roaring64.Bitmap, []models.SearchResult, error) {
	startTime := time.Now()
	searchSet, _, err := v.greedySearch(query.Vector, query.Limit, query.SearchSize, filter)
	if err != nil {
		return nil, nil, fmt.Errorf("could not perform graph search: %w", err)
	}
	v.logger.Debug().Str("component", "shard").Str("duration", time.Since(startTime).String()).Msg("SearchPoints - GreedySearch")
	results := make([]models.SearchResult, 0, min(len(searchSet.items), query.Limit))
	resultSet := roaring64.New()
	// ---------------------------
	weight := float32(1)
	if query.Weight != nil {
		weight = *query.Weight
	}
	// ---------------------------
	for _, elem := range searchSet.items {
		if elem.Point.Id() == STARTID {
			continue
		}
		if len(results) >= query.Limit {
			break
		}
		sr := models.SearchResult{
			NodeId:      elem.Point.Id(),
			Distance:    &elem.Distance,
			HybridScore: (-1 * elem.Distance * weight),
		}
		results = append(results, sr)
		resultSet.Add(elem.Point.Id())
	}
	// ---------------------------
	return resultSet, results, err
}
