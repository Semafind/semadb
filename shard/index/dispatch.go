package index

import (
	"context"
	"fmt"

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
) <-chan error {
	// ---------------------------
	distributeErrC := make(chan error, 1)
	// ---------------------------
	/* We drain the original changes channel to multiplex into the appropriate
	 * index queues which become their own pipelines. */
	go func() {
		errCs := make([]<-chan error, 0)
		decodedQ := make(map[string]chan decodedPointChange)
		dec := msgpack.NewDecoder(nil)
		errC := utils.SinkWithContext(ctx, changes, func(change IndexPointChange) error {
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
					df, err := im.getDrainFn(bucketName, params)
					if err != nil {
						return fmt.Errorf("could not setup drain function for %s: %w", bucketName, err)
					}
					errCs = append(errCs, df(ctx, queue))
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
		err := <-errC
		// Close any queues
		for _, queue := range decodedQ {
			close(queue)
		}
		if err != nil {
			distributeErrC <- fmt.Errorf("error distributing changes: %w", err)
		} else {
			distributeErrC <- <-utils.MergeErrorsWithContext(ctx, errCs...)
		}
		close(distributeErrC)
	}()
	// ---------------------------
	return distributeErrC
}

type DrainFn func(ctx context.Context, in <-chan decodedPointChange) <-chan error

func (im indexManager) getDrainFn(bucketName string, params models.IndexSchemaValue) (DrainFn, error) {
	// ---------------------------
	bucket, err := im.bm.Get(bucketName)
	if err != nil {
		return nil, fmt.Errorf("could not get write bucket %s: %w", bucketName, err)
	}
	// ---------------------------
	var drainFn DrainFn
	switch params.Type {
	case models.IndexTypeVectorVamana:
		cacheName := im.cacheRoot + "/" + bucketName
		// Transform
		drainFn = func(ctx context.Context, in <-chan decodedPointChange) <-chan error {
			out, transformErrC := utils.TransformWithContext(ctx, in, preProcessVamana)
			writeErrC := make(chan error, 1)
			go func() {
				err := im.cx.With(cacheName, bucket, func(pc cache.SharedPointCache) error {
					vamanaIndex, err := vamana.NewIndexVamana(cacheName, pc, *params.VectorVamana, im.maxNodeId)
					if err != nil {
						return fmt.Errorf("could not create vamana index: %w", err)
					}
					return <-vamanaIndex.InsertUpdateDelete(ctx, out)
				})
				writeErrC <- err
				close(writeErrC)
			}()
			return utils.MergeErrorsWithContext(ctx, transformErrC, writeErrC)
		}
		// ---------------------------
	case models.IndexTypeInteger:
		intIndex := inverted.NewIndexInverted[int64](bucket)
		drainFn = func(ctx context.Context, in <-chan decodedPointChange) <-chan error {
			out, transformErrC := utils.TransformWithContext(ctx, in, preProcessInverted[int64])
			errC := intIndex.InsertUpdateDelete(ctx, out)
			return utils.MergeErrorsWithContext(ctx, transformErrC, errC)
		}
	default:
		return nil, fmt.Errorf("unsupported index property type: %s", params.Type)
	} // End of property type switch
	return drainFn, nil
}

func preProcessInverted[T inverted.Invertable](change decodedPointChange) (invChange inverted.IndexChange[T], skip bool, err error) {
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

func preProcessVamana(change decodedPointChange) (gn cache.GraphNode, skip bool, err error) {
	// ---------------------------
	gn.NodeId = change.nodeId
	if change.newData != nil {
		gn.Vector, err = castDataToVector(change.newData)
	}
	return
}
