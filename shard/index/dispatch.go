package index

import (
	"context"
	"fmt"
	"sync"

	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index/flat"
	"github.com/semafind/semadb/shard/index/inverted"
	"github.com/semafind/semadb/shard/index/text"
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
	ctx, cancel := context.WithCancelCause(ctx)
	var wg sync.WaitGroup
	// ---------------------------
	/* We drain the original changes channel to multiplex into the appropriate
	 * index queues which become their own pipelines. */
	decodedQ := make(map[string]chan decodedPointChange)
	dec := msgpack.NewDecoder(nil)
	wg.Add(1)
	distributeSinkErrC := utils.SinkWithContext(ctx, changes, func(change IndexPointChange) error {
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
				drainErrC := df(ctx, queue)
				// Listen to errors on the drain function, if an index fails we
				// abort the entire operation
				wg.Add(1)
				go func() {
					if err := <-drainErrC; err != nil {
						cancel(err)
					}
					wg.Done()
				}()
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
	// ---------------------------
	go func() {
		// Go function to close index queues when the dispatching finishes
		if err := <-distributeSinkErrC; err != nil {
			cancel(err)
		}
		// Close any queues
		for _, queue := range decodedQ {
			close(queue)
		}
		wg.Done()
	}()
	// ---------------------------
	dispatchErrC := make(chan error, 1)
	go func() {
		// Wait for all the drain functions and distribute sink to finish
		wg.Wait()
		// Did we succeed or fail?
		dispatchErrC <- context.Cause(ctx)
		close(dispatchErrC)
	}()
	// ---------------------------
	return dispatchErrC
}

type DrainFn func(ctx context.Context, in <-chan decodedPointChange) <-chan error

func (im indexManager) getDrainFn(bucketName string, params models.IndexSchemaValue) (DrainFn, error) {
	// ---------------------------
	bucket, err := im.bm.Get(bucketName)
	if err != nil {
		return nil, fmt.Errorf("could not get write bucket %s: %w", bucketName, err)
	}
	cacheName := im.cacheRoot + "/" + bucketName
	// ---------------------------
	var drainFn DrainFn
	switch params.Type {
	case models.IndexTypeVectorVamana:
		// Transform
		drainFn = func(ctx context.Context, in <-chan decodedPointChange) <-chan error {
			out, transformErrC := utils.TransformWithContext(ctx, in, preProcessVamana)
			writeErrC := make(chan error, 1)
			newVamanaFn := func() (cache.Cachable, error) {
				return vamana.NewIndexVamana(cacheName, *params.VectorVamana, bucket)
			}
			go func() {
				writeErrC <- im.cx.With(cacheName, false, newVamanaFn, func(cached cache.Cachable) error {
					vamanaIndex := cached.(*vamana.IndexVamana)
					/* This update bucket business shouldn't cause a discrepancy
					 * in the index because there should be only one write
					 * operation on the bucket and after each write operation it
					 * should be flushed. Any subsequent requests, although start
					 * with a new bucket, that bucket should persist the changes
					 * of the previous one whilst sharing the cached items. For
					 * example, two write requests would happen one after the
					 * other and each would use their own bucket but have to wait
					 * until the cache is available. */
					vamanaIndex.UpdateBucket(bucket)
					return <-vamanaIndex.InsertUpdateDelete(ctx, out)
				})
				close(writeErrC)
			}()
			return utils.MergeErrorsWithContext(ctx, transformErrC, writeErrC)
		}
		// ---------------------------
	case models.IndexTypeVectorFlat:
		drainFn = func(ctx context.Context, in <-chan decodedPointChange) <-chan error {
			out, transformErrC := utils.TransformWithContext(ctx, in, preProcessVamana)
			writeErrC := make(chan error, 1)
			newFlatFn := func() (cache.Cachable, error) {
				return flat.NewIndexFlat(*params.VectorFlat, bucket)
			}
			go func() {
				writeErrC <- im.cx.With(cacheName, false, newFlatFn, func(cached cache.Cachable) error {
					flatIndex := cached.(flat.IndexFlat)
					flatIndex.UpdateBucket(bucket)
					return <-flatIndex.InsertUpdateDelete(ctx, out)
				})
			}()
			return utils.MergeErrorsWithContext(ctx, transformErrC, writeErrC)
		}
	case models.IndexTypeText:
		textIndex, err := text.NewIndexText(bucket, *params.Text)
		if err != nil {
			return nil, fmt.Errorf("could not create text index: %w", err)
		}
		drainFn = func(ctx context.Context, in <-chan decodedPointChange) <-chan error {
			out, transformErrC := utils.TransformWithContext(ctx, in, preProcessText)
			errC := textIndex.InsertUpdateDelete(ctx, out)
			return utils.MergeErrorsWithContext(ctx, transformErrC, errC)
		}
	case models.IndexTypeString:
		stringIndex := inverted.NewIndexInvertedString(bucket, *params.String)
		drainFn = func(ctx context.Context, in <-chan decodedPointChange) <-chan error {
			out, transformErrC := utils.TransformWithContext(ctx, in, preProcessInverted[string])
			errC := stringIndex.InsertUpdateDelete(ctx, out)
			return utils.MergeErrorsWithContext(ctx, transformErrC, errC)
		}
	case models.IndexTypeStringArray:
		stringArrayIndex := inverted.NewIndexInvertedArrayString(bucket, *params.StringArray)
		drainFn = func(ctx context.Context, in <-chan decodedPointChange) <-chan error {
			out, transformErrC := utils.TransformWithContext(ctx, in, preProcessInvertedArray[string])
			errC := stringArrayIndex.InsertUpdateDelete(ctx, out)
			return utils.MergeErrorsWithContext(ctx, transformErrC, errC)
		}
	case models.IndexTypeInteger:
		intIndex := inverted.NewIndexInverted[int64](bucket)
		drainFn = func(ctx context.Context, in <-chan decodedPointChange) <-chan error {
			out, transformErrC := utils.TransformWithContext(ctx, in, preProcessInverted[int64])
			errC := intIndex.InsertUpdateDelete(ctx, out)
			return utils.MergeErrorsWithContext(ctx, transformErrC, errC)
		}
	case models.IndexTypeFloat:
		intIndex := inverted.NewIndexInverted[float64](bucket)
		drainFn = func(ctx context.Context, in <-chan decodedPointChange) <-chan error {
			out, transformErrC := utils.TransformWithContext(ctx, in, preProcessInverted[float64])
			errC := intIndex.InsertUpdateDelete(ctx, out)
			return utils.MergeErrorsWithContext(ctx, transformErrC, errC)
		}
	default:
		return nil, fmt.Errorf("unsupported index property type: %s", params.Type)
	} // End of property type switch
	return drainFn, nil
}

func preProcessText(change decodedPointChange) (doc text.Document, skip bool, err error) {
	// ---------------------------
	doc.Id = change.nodeId
	if change.newData != nil {
		text, ok := change.newData.(string)
		if !ok {
			err = fmt.Errorf("could not cast new text data: %v", change.newData)
			return
		}
		doc.Text = text
	}
	return
}

func preProcessInvertedArray[T inverted.Invertable](change decodedPointChange) (invChange inverted.IndexArrayChange[T], skip bool, err error) {
	// ---------------------------
	invChange.Id = change.nodeId
	invChange.PreviousData, err = castDataToArray[T](change.oldData)
	if err != nil {
		return
	}
	invChange.CurrentData, err = castDataToArray[T](change.newData)
	return
}

func preProcessInverted[T inverted.Invertable](change decodedPointChange) (invChange inverted.IndexChange[T], skip bool, err error) {
	// ---------------------------
	invChange.Id = change.nodeId
	if change.oldData != nil {
		prevValue, ok := change.oldData.(T)
		if !ok {
			err = fmt.Errorf("could not cast old inverted data: %v %T", change.oldData, change.oldData)
			return
		}
		invChange.PreviousData = &prevValue
	}
	if change.newData != nil {
		currentValue, ok := change.newData.(T)
		if !ok {
			err = fmt.Errorf("could not cast new inverted data: %v %T", change.newData, change.newData)
			return
		}
		invChange.CurrentData = &currentValue
	}
	return
}

func preProcessVamana(change decodedPointChange) (vc vamana.IndexVectorChange, skip bool, err error) {
	// ---------------------------
	vc.Id = change.nodeId
	vc.Vector, err = castDataToArray[float32](change.newData)
	return
}
