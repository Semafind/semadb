package flat

import (
	"context"
	"fmt"

	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/index/vamana"
	"github.com/semafind/semadb/shard/vectorstore"
	"github.com/semafind/semadb/utils"
)

type IndexFlat struct {
	vecStore vectorstore.VectorStore
}

func NewIndexFlat(params models.IndexVectorFlatParameters, bucket diskstore.Bucket) (inf IndexFlat, err error) {
	// ---------------------------
	vstore, err := vectorstore.New(params.Quantizer, bucket, params.DistanceMetric, int(params.VectorSize))
	if err != nil {
		err = fmt.Errorf("failed to create vector store: %w", err)
		return
	}
	inf.vecStore = vstore
	// ---------------------------
	return
}

func (inf IndexFlat) SizeInMemory() int64 {
	return inf.vecStore.SizeInMemory()
}

func (inf IndexFlat) UpdateBucket(bucket diskstore.Bucket) {
	inf.vecStore.UpdateBucket(bucket)
}

func (inf IndexFlat) InsertUpdateDelete(ctx context.Context, points <-chan vamana.IndexVectorChange) <-chan error {
	sinkErrC := utils.SinkWithContext(ctx, points, func(point vamana.IndexVectorChange) error {
		// Does this point exist?
		var err error
		switch {
		case point.Vector != nil:
			// Insert or update
			_, err = inf.vecStore.Set(point.Id, point.Vector)
		case point.Vector == nil:
			// Delete
			err = inf.vecStore.Delete(point.Id)
		default:
			err = fmt.Errorf("unknown operation for point: %d", point.Id)
		}
		return err
	})
	errC := make(chan error, 1)
	// We use this go routine to flush the vector store after all points have
	// been processed
	go func() {
		defer close(errC)
		if err := <-sinkErrC; err != nil {
			errC <- fmt.Errorf("failed to insert/update/delete: %w", err)
			return
		}
		errC <- inf.vecStore.Flush()
	}()
	return errC
}

func (inf IndexFlat) Search(ctx context.Context, options models.SearchVectorFlatOptions) ([]models.SearchResult, error) {
	// distSet := vamana.NewDistSet(options.Limit, 0, inf.vecStore.DistanceFromFloat(options.Vector))
	// TODO
	return nil, nil
}
