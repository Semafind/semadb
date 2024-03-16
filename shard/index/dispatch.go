package index

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/diskstore"
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

func getPropertyFromBytes(dec *msgpack.Decoder, data []byte, property string) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	dec.Reset(bytes.NewReader(data))
	// ---------------------------
	queryResult, err := dec.Query(property)
	if err != nil {
		return nil, fmt.Errorf("could not query field: %w", err)
	}
	if len(queryResult) == 0 {
		// The field is not found
		return nil, nil
	}
	// ---------------------------
	return queryResult[0], nil
}

// ---------------------------
const (
	opInsert = "insert"
	opUpdate = "update"
	opDelete = "delete"
)

// Determines the operation to be performed on the index and extracts the previous
// and current property values.
func getOperation(dec *msgpack.Decoder, propertyName string, prevData, currentData []byte) (prevProp, currentProp any, op string, err error) {
	prevProp, err = getPropertyFromBytes(dec, prevData, propertyName)
	if err != nil {
		err = fmt.Errorf("could not get previous property %s: %w", propertyName, err)
		return
	}
	currentProp, err = getPropertyFromBytes(dec, currentData, propertyName)
	if err != nil {
		err = fmt.Errorf("could not get new property %s: %w", propertyName, err)
		return
	}
	switch {
	case prevProp == nil && currentProp != nil:
		// Insert
		op = opInsert
	case prevProp != nil && currentProp != nil:
		// Update
		op = opUpdate
	case currentProp == nil:
		// Delete
		op = opDelete
	default:
		err = fmt.Errorf("unexpected previous and current values for %s: %v - %v", propertyName, prevProp, currentProp)
	}
	// Named return values are a language feature that allows us to declare what
	// the return values are named and then we can just return without specifying
	// the return values.
	return
}

// ---------------------------

func castDataToVector(data any) ([]float32, error) {
	// The problem is the query returns []any and we need to
	// convert it to the appropriate type, doing .([]float32) doesn't work
	var vector []float32
	if data != nil {
		if anyArr, ok := data.([]any); !ok {
			return vector, fmt.Errorf("expected vector got %T", data)
		} else {
			vector = make([]float32, len(anyArr))
			for i, v := range anyArr {
				vector[i] = v.(float32)
			}
		}
	}
	return vector, nil
}

// ---------------------------

// Dispatch is a function that dispatches the new data to the appropriate index
func Dispatch(
	ctx context.Context,
	bm diskstore.BucketManager,
	cm *cache.Manager,
	cacheRoot string,
	indexSchema models.IndexSchema,
	maxNodeId uint64,
	changes <-chan IndexPointChange,
) error {
	// ---------------------------
	var indexWg sync.WaitGroup
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	dec := msgpack.NewDecoder(nil)
	// ---------------------------
	vectorQ := make(map[string]chan cache.GraphNode)
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
			for propName, params := range indexSchema {
				_, current, op, err := getOperation(dec, propName, change.PreviousData, change.NewData)
				if err != nil {
					return fmt.Errorf("could not get operation for property %s: %w", propName, err)
				}
				// ---------------------------
				// e.g. index/vamana/myvector
				bucketName := fmt.Sprintf("index/%s/%s", params.Type, propName)
				// ---------------------------
				switch params.Type {
				case models.IndexTypeVectorVamana:
					// ---------------------------
					vector, err := castDataToVector(current)
					if err != nil {
						return fmt.Errorf("could not cast data to vector for property %s: %w", propName, err)
					}
					// ---------------------------
					// e.g. [shardId]/index/vamana/myvector
					cacheName := cacheRoot + "/" + bucketName
					// e.g. index/vamana/myvector/insert
					qName := bucketName + "/" + op
					queue, ok := vectorQ[qName]
					if !ok {
						queue = make(chan cache.GraphNode)
						log.Debug().Str("component", "indexDispatch").Str("queue", qName).Msg("creating new queue")
						vectorQ[qName] = queue
						bucket, err := bm.WriteBucket(bucketName)
						if err != nil {
							return fmt.Errorf("could not get write bucket %s: %w", bucketName, err)
						}
						indexVamana, err := vamana.NewIndexVamana(cacheName, *params.VectorVamana, maxNodeId)
						if err != nil {
							return fmt.Errorf("could not get new vamana index %s: %w", propName, err)
						}
						indexWg.Add(1)
						go func() {
							defer indexWg.Done()
							// Recall the queue name includes op, so we don't
							// interleave insert, update and delete operation
							// they all act on consistent states of the index
							// based on the cache.
							err := cm.With(cacheName, bucket, func(pc cache.ReadWriteCache) error {
								switch op {
								case opInsert:
									return indexVamana.Insert(ctx, pc, queue)
								case opDelete:
									return indexVamana.Delete(ctx, pc, queue)
								case opUpdate:
									return indexVamana.Update(ctx, pc, queue)
								}
								return nil
							})
							if err != nil {
								cancel(fmt.Errorf("could not perform vector index %s for %s: %w", params.Type, propName, err))
							}
						}()
					}
					// ---------------------------
					// Submit job to the queue
					select {
					case queue <- cache.GraphNode{NodeId: change.NodeId, Vector: vector}:
					case <-ctx.Done():
						return fmt.Errorf("context done while dispatching to %s: %w", qName, context.Cause(ctx))
					}
					// ---------------------------
				default:
					return fmt.Errorf("unsupported index property type: %s", params.Type)
				} // End of property type switch
			} // End of properties loop
		}
	}
	// ---------------------------
	// Close any queues
	for _, queue := range vectorQ {
		close(queue)
	}
	// ---------------------------
	// Wait for any indexing to finish
	indexWg.Wait()
	if err := context.Cause(ctx); err != nil {
		return fmt.Errorf("index dispatch context error: %w", err)
	}
	return nil
}
