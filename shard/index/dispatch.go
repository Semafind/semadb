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
	// typedResult, ok := queryResult[0].(T)
	// if !ok {
	// 	return nil, fmt.Errorf("could not convert property %s to type %T", property, typedResult)
	// }
	// ---------------------------
	return queryResult[0], nil
}

// Dispatch is a function that dispatches the new data to the appropriate index
func Dispatch(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	bm diskstore.BucketManager,
	cm *cache.Manager,
	cacheRoot string,
	indexSchema models.IndexSchema,
	maxNodeId uint,
	changes <-chan IndexPointChange,
) error {
	// ---------------------------
	var indexWg sync.WaitGroup
	dec := msgpack.NewDecoder(nil)
	// ---------------------------
	vectorQ := make(map[string]chan cache.GraphNode)
	// ---------------------------
outer:
	for change := range changes {
		if ctx.Err() != nil {
			break
		}
		// ---------------------------
		for _, iprop := range indexSchema.CollectAllProperties() {
			if ctx.Err() != nil {
				break outer
			}
			prev, err := getPropertyFromBytes(dec, change.PreviousData, iprop.Name)
			if err != nil {
				cancel(fmt.Errorf("could not get previous property %s: %w", iprop.Name, err))
				break outer
			}
			current, err := getPropertyFromBytes(dec, change.NewData, iprop.Name)
			if err != nil {
				cancel(fmt.Errorf("could not get new property %s: %w", iprop.Name, err))
				break outer
			}
			op := ""
			switch {
			case prev == nil && current != nil:
				// Insert
				op = "insert"
			case prev != nil && current != nil:
				// Update
				op = "update"
			case current == nil:
				// Delete
				op = "delete"
			default:
				cancel(fmt.Errorf("unexpected previous and current values for %s: %v - %v", iprop.Name, prev, current))
				break outer
			}
			switch iprop.Type {
			case "vectorFlat":
				fallthrough
			case "vectorVamana":
				// ---------------------------
				// The problem is the query returns a slice of interface{} and we need to
				// convert it to the appropriate type, doing .([]float32) doesn't work
				var vector []float32
				if current != nil {
					if anyArr, ok := current.([]any); !ok {
						cancel(fmt.Errorf("expected vector for property %s, got %T", iprop.Name, current))
						break outer
					} else {
						vector = make([]float32, len(anyArr))
						for i, v := range anyArr {
							vector[i] = v.(float32)
						}
					}
				}
				// ---------------------------
				// e.g. index/vamana/myvector
				bucketName := fmt.Sprintf("index/%s/%s", iprop.Type, iprop.Name)
				// e.g. [shardId]/index/vamana/myvector
				cacheName := cacheRoot + "/" + bucketName
				// e.g. index/vamana/myvector/insert
				qName := bucketName + "/" + op
				if queue, ok := vectorQ[qName]; ok {
					queue <- cache.GraphNode{NodeId: change.NodeId, Vector: vector}
				} else {
					newQ := make(chan cache.GraphNode)
					log.Debug().Str("component", "indexDispatch").Str("queue", qName).Msg("creating new queue")
					vectorQ[qName] = newQ
					bucket, err := bm.WriteBucket(bucketName)
					if err != nil {
						cancel(fmt.Errorf("could not get write bucket %s: %w", bucketName, err))
						break outer
					}
					indexWg.Add(1)
					go func() {
						defer indexWg.Done()
						// TODO: handle vector flat
						indexVamana, err := vamana.NewIndexVamana(cacheName, indexSchema.VectorVamana[iprop.Name], cm, maxNodeId)
						if err != nil {
							cancel(fmt.Errorf("could not get new vamana index %s: %w", iprop.Name, err))
							return
						}
						switch op {
						case "insert":
							err = indexVamana.Insert(ctx, cancel, bucket, newQ)
						case "delete":
							err = indexVamana.Delete(ctx, cancel, bucket, newQ)
						case "update":
							err = indexVamana.Update(ctx, cancel, bucket, newQ)
						}
						if err != nil {
							cancel(fmt.Errorf("could not perform vector index %s for %s: %w", iprop.Type, iprop.Name, err))
						}
					}()
					newQ <- cache.GraphNode{NodeId: change.NodeId, Vector: vector}
				}
				// ---------------------------
			} // End of property type switch
		} // End of properties loop
		// ---------------------------
	} // End of changes loop
	// ---------------------------
	// Close any queues
	for _, queue := range vectorQ {
		close(queue)
	}
	// ---------------------------
	// Wait for any indexing to finish
	indexWg.Wait()
	return nil
}
