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
	indexMap := make(map[string]any)
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
				// e.g. index/vamana/myvector/insert
				qName := bucketName + "/" + op
				queue, ok := decodedQ[qName]
				if !ok {
					queue = make(chan decodedPointChange)
					decodedQ[qName] = queue
					drainFn, err := im.getDrainFn(indexMap, params, bucketName, op)
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
					return fmt.Errorf("context done while dispatching to %s: %w", qName, context.Cause(ctx))
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

func (im indexManager) getDrainFn(
	indexMap map[string]any,
	params models.IndexSchemaValue,
	bucketName string,
	operation string,
) (drainFunction, error) {
	switch params.Type {
	case models.IndexTypeVectorVamana:
		// ---------------------------
		vamanaIndexAny, ok := indexMap[bucketName]
		if !ok {
			cacheName := im.cacheRoot + "/" + bucketName
			bucket, err := im.bm.Get(bucketName)
			if err != nil {
				return nil, fmt.Errorf("could not get write bucket %s: %w", bucketName, err)
			}
			vamanaIndexAny, err = vamana.NewIndexVamana(cacheName, im.cx, bucket, *params.VectorVamana, im.maxNodeId)
			if err != nil {
				return nil, fmt.Errorf("could not create vamana index: %w", err)
			}
			indexMap[bucketName] = vamanaIndexAny
		}
		vamanaIndex := vamanaIndexAny.(*vamana.IndexVamana)
		vDrain := func(ctx context.Context, queue <-chan decodedPointChange) error {
			return drainVamana(ctx, queue, vamanaIndex, operation)
		}
		return vDrain, nil
	default:
		return nil, fmt.Errorf("unsupported index property type: %s", params.Type)
	} // End of property type switch
}

func drainVamana(
	ctx context.Context,
	in <-chan decodedPointChange,
	vamanaIndex *vamana.IndexVamana,
	operation string,
) error {
	// ---------------------------
	out := make(chan cache.GraphNode)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	// ---------------------------
	wg.Add(1)
	go func() {
		defer wg.Done()
		switch operation {
		case opInsert:
			if err := vamanaIndex.Insert(ctx, out); err != nil {
				cancel(fmt.Errorf("could not insert into vamana index: %w", err))
			}
		case opUpdate:
			if err := vamanaIndex.Update(ctx, out); err != nil {
				cancel(fmt.Errorf("could not update vamana index: %w", err))
			}
		case opDelete:
			if err := vamanaIndex.Delete(ctx, out); err != nil {
				cancel(fmt.Errorf("could not delete from vamana index: %w", err))
			}
		}
	}()
	// ---------------------------
	// Transform the incoming changes
outer:
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context interrupt while draining vamana: %w", context.Cause(ctx))
		case change, ok := <-in:
			if !ok {
				break outer
			}
			vector, err := castDataToVector(change.newData)
			if err != nil {
				return fmt.Errorf("could not cast data to vector: %w", err)
			}
			select {
			case out <- cache.GraphNode{NodeId: change.nodeId, Vector: vector}:
			case <-ctx.Done():
				return fmt.Errorf("context interrupt while draining vamana: %w", context.Cause(ctx))
			}
		}
	}
	close(out)
	wg.Wait()
	return context.Cause(ctx)
}
