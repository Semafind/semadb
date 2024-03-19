package index

import (
	"context"
	"fmt"
	"sync"

	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index/vamana"
	"github.com/vmihailenco/msgpack/v5"
)

type IndexPointChange struct {
	NodeId       uint64
	PreviousData []byte
	NewData      []byte
}

type decodedPointChange struct {
	nodeId  uint64
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
	// ---------------------------
outer:
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context interrupt for index dispatch: %w", context.Cause(ctx))
		// For every change, we go through every indexable property and dispatch
		case change, ok := <-changes:
			if !ok {
				break outer
			}
			for propName, params := range im.indexSchema {
				_, current, op, err := getOperation(dec, propName, change.PreviousData, change.NewData)
				if err != nil {
					return fmt.Errorf("could not get operation for property %s: %w", propName, err)
				}
				// This property has not changed or is not present
				if op == opSkip {
					continue
				}
				// ---------------------------
				// e.g. index/vamana/myvector
				bucketName := fmt.Sprintf("index/%s/%s", params.Type, propName)
				queue, ok := decodedQ[bucketName]
				if !ok {
					queue = make(chan decodedPointChange)
					decodedQ[bucketName] = queue
					drainFn, err := im.getDrainFn(bucketName, params)
					if err != nil {
						return fmt.Errorf("could not get drain function for %s: %w", bucketName, err)
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						if err := drainFn(ctx, queue); err != nil {
							cancel(fmt.Errorf("could not drain index for %s: %w", bucketName, err))
						}
					}()
				}
				// ---------------------------
				// Submit job to the queue
				select {
				case queue <- decodedPointChange{nodeId: change.NodeId, newData: current}:
				case <-ctx.Done():
					return fmt.Errorf("context done while dispatching to %s: %w", bucketName, context.Cause(ctx))
				}
				// ---------------------------
			} // End of properties loop
		}
	}
	// ---------------------------
	// Close any queues
	for _, queue := range decodedQ {
		close(queue)
	}
	// ---------------------------
	// Wait for any indexing to finish
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return fmt.Errorf("index dispatch context error: %w", err)
	}
	return nil
}

type drainFunction func(ctx context.Context, queue <-chan decodedPointChange) error

func (im indexManager) getDrainFn(bucketName string, params models.IndexSchemaValue) (drainFunction, error) {
	// ---------------------------
	bucket, err := im.bm.Get(bucketName)
	if err != nil {
		return nil, fmt.Errorf("could not get write bucket %s: %w", bucketName, err)
	}
	// ---------------------------
	switch params.Type {
	case models.IndexTypeVectorVamana:
		// ---------------------------
		cacheName := im.cacheRoot + "/" + bucketName
		vamanaIndex, err := vamana.NewIndexVamana(cacheName, im.cx, bucket, *params.VectorVamana, im.maxNodeId)
		if err != nil {
			return nil, fmt.Errorf("could not create vamana index: %w", err)
		}
		vDrain := func(ctx context.Context, queue <-chan decodedPointChange) error {
			return drainVamana(ctx, queue, vamanaIndex)
		}
		return vDrain, nil
	default:
		return nil, fmt.Errorf("unsupported index property type: %s", params.Type)
	} // End of property type switch
}

func drainVamana(ctx context.Context, in <-chan decodedPointChange, vamanaIndex *vamana.IndexVamana) error {
	// ---------------------------
	out := make(chan cache.GraphNode)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	// ---------------------------
	// Transform the incoming changes
	go func() {
		defer close(out)
		for change := range in {
			var vector []float32
			if change.newData != nil {
				var err error
				vector, err = castDataToVector(change.newData)
				if err != nil {
					cancel(fmt.Errorf("could not cast data to vector: %w", err))
					return
				}
			}
			select {
			case out <- cache.GraphNode{NodeId: change.nodeId, Vector: vector}:
			case <-ctx.Done():
				return
			}
		}
	}()
	// ---------------------------
	// Write the transformed changes to the index
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := vamanaIndex.InsertUpdateDelete(ctx, out); err != nil {
			cancel(fmt.Errorf("could not write to vamana index: %w", err))
		}
	}()
	// ---------------------------
	wg.Wait()
	return context.Cause(ctx)
}
