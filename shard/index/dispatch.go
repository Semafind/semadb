package index

import (
	"context"
	"fmt"
	"sync"

	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index/inverted"
	"github.com/semafind/semadb/shard/index/vamana"
	"github.com/semafind/semadb/utils"
	"github.com/vmihailenco/msgpack/v5"
)

type IndexPointChange struct {
	NodeId       uint64
	PreviousData []byte
	NewData      []byte
}

type decodedPointChange struct {
	nodeId  uint64
	oldData any
	newData any
}

// ---------------------------

// Dispatch is a function that dispatches the new data to the appropriate index
func (im indexManager) Dispatch(
	ctx context.Context,
	changes <-chan IndexPointChange,
) error {
	// ---------------------------
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	dec := msgpack.NewDecoder(nil)
	// ---------------------------
	decodedQ := make(map[string]chan decodedPointChange)
	/* We drain the original changes channel to multiplex into the appropriate
	 * index queues which become their own pipelines. */
	err := utils.SinkWithContext(ctx, changes, func(change IndexPointChange) error {
		for propName, params := range im.indexSchema {
			prev, current, op, err := getOperation(dec, propName, change.PreviousData, change.NewData)
			if err != nil {
				return fmt.Errorf("could not get operation for property %s: %w", propName, err)
			}
			// This property has not changed or is not present
			if op == opSkip {
				continue
			}
			// e.g. index/vamana/myvector
			bucketName := fmt.Sprintf("index/%s/%s", params.Type, propName)
			queue, ok := decodedQ[bucketName]
			if !ok {
				queue = make(chan decodedPointChange)
				decodedQ[bucketName] = queue
				if err := im.getDrainFn(ctx, cancel, &wg, bucketName, params, queue); err != nil {
					return fmt.Errorf("could not setup drain for %s: %w", bucketName, err)
				}
			}
			// ---------------------------
			// Submit job to the queue
			select {
			case queue <- decodedPointChange{nodeId: change.NodeId, oldData: prev, newData: current}:
			case <-ctx.Done():
				return fmt.Errorf("context done while dispatching to %s: %w", bucketName, context.Cause(ctx))
			}
		}
		return nil
	})
	if err != nil {
		cancel(fmt.Errorf("could not dispatch index: %w", err))
	}
	// ---------------------------
	// Close any queues
	for _, queue := range decodedQ {
		close(queue)
	}
	// ---------------------------
	// Wait for any indexing to finish
	wg.Wait()
	return context.Cause(ctx)
}

func (im indexManager) getDrainFn(ctx context.Context, cancel context.CancelCauseFunc, wg *sync.WaitGroup, bucketName string, params models.IndexSchemaValue, in <-chan decodedPointChange) error {
	// ---------------------------
	bucket, err := im.bm.Get(bucketName)
	if err != nil {
		return fmt.Errorf("could not get write bucket %s: %w", bucketName, err)
	}
	// ---------------------------
	switch params.Type {
	case models.IndexTypeVectorVamana:
		cacheName := im.cacheRoot + "/" + bucketName
		vamanaIndex, err := vamana.NewIndexVamana(cacheName, im.cx, bucket, *params.VectorVamana, im.maxNodeId)
		if err != nil {
			return fmt.Errorf("could not create vamana index: %w", err)
		}
		// Transform
		setupDrain(ctx, cancel, wg, in, preProcessVamana, vamanaIndex.InsertUpdateDelete)
		// ---------------------------
	case models.IndexTypeInteger:
		intIndex := inverted.NewIndexInverted[int64](bucket)
		setupDrain(ctx, cancel, wg, in, preProcessInverted[int64], intIndex.InsertUpdateDelete)
	default:
		return fmt.Errorf("unsupported index property type: %s", params.Type)
	} // End of property type switch
	return nil
}

func setupDrain[T any](ctx context.Context, cancel context.CancelCauseFunc, wg *sync.WaitGroup, in <-chan decodedPointChange, preProcess func(decodedPointChange) (T, error), endpoint func(context.Context, <-chan T) error) {
	out := make(chan T)
	// ---------------------------
	// Kick off the pre-processing
	wg.Add(1)
	go func() {
		if err := utils.TransformWithContext(ctx, in, out, preProcess); err != nil {
			cancel(fmt.Errorf("could not transform: %w", err))
		}
		close(out)
		wg.Done()
	}()
	// ---------------------------
	// Start drain
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := endpoint(ctx, out); err != nil {
			cancel(fmt.Errorf("could not drain: %w", err))
		}
	}()
	// ---------------------------
}

func preProcessInverted[T inverted.Invertable](change decodedPointChange) (invChange inverted.IndexChange[T], err error) {
	// ---------------------------
	invChange.Id = change.nodeId
	if change.oldData != nil {
		prevValue, ok := change.oldData.(T)
		if !ok {
			err = fmt.Errorf("could not cast old data: %v", change.oldData)
			return
		}
		invChange.PreviousData = &prevValue
	}
	if change.newData != nil {
		currentValue, ok := change.newData.(T)
		if !ok {
			err = fmt.Errorf("could not cast new data: %v", change.newData)
			return
		}
		invChange.CurrentData = &currentValue
	}
	return
}

func preProcessVamana(change decodedPointChange) (gn cache.GraphNode, err error) {
	// ---------------------------
	gn.NodeId = change.nodeId
	if change.newData != nil {
		gn.Vector, err = castDataToVector(change.newData)
	}
	return
}
