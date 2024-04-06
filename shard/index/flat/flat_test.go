package flat_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/index/flat"
	"github.com/semafind/semadb/shard/index/vamana"
	"github.com/semafind/semadb/utils"
	"github.com/stretchr/testify/require"
)

var flatParams = models.IndexVectorFlatParameters{
	VectorSize:     2,
	DistanceMetric: "euclidean",
}

func checkVectorCount(t *testing.T, bucket diskstore.Bucket, expected int) {
	t.Helper()
	// Check vector count
	count := 0
	err := bucket.ForEach(func(key []byte, value []byte) error {
		_, ok := conversion.NodeIdFromKey(key, 'v')
		if ok {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, expected, count)
}

func randPoints(size int, offset int) []vamana.IndexVectorChange {
	points := make([]vamana.IndexVectorChange, size)
	for i := 0; i < size; i++ {
		randVector := make([]float32, 2)
		randVector[0] = rand.Float32()
		randVector[1] = rand.Float32()
		points[i] = vamana.IndexVectorChange{
			// 0 is not allowed, 1 is start node
			Id:     uint64(i + offset + 2),
			Vector: randVector,
		}
	}
	return points
}

func Test_ConcurrentCUD(t *testing.T) {
	bucket := diskstore.NewMemBucket(false)
	inv, err := flat.NewIndexFlat(flatParams, bucket)
	require.NoError(t, err)
	// Pre-insert
	in := make(chan vamana.IndexVectorChange)
	errC := inv.InsertUpdateDelete(context.Background(), in)
	for _, rp := range randPoints(50, 0) {
		in <- rp
	}
	// ---------------------------
	var wg sync.WaitGroup
	wg.Add(3)
	// Insert more
	go func() {
		for _, rp := range randPoints(50, 50) {
			in <- rp
		}
		wg.Done()
	}()
	// ---------------------------
	// Update some
	go func() {
		for _, rp := range randPoints(25, 25) {
			in <- rp
		}
		wg.Done()
	}()
	// ---------------------------
	// Delete some
	go func() {
		for i := 0; i < 25; i++ {
			in <- vamana.IndexVectorChange{Id: uint64(i + 2), Vector: nil}
		}
		wg.Done()
	}()
	// ---------------------------
	wg.Wait()
	close(in)
	require.NoError(t, <-errC)
	checkVectorCount(t, bucket, 75)
}

func Test_Search(t *testing.T) {
	bucket := diskstore.NewMemBucket(false)
	inv, err := flat.NewIndexFlat(flatParams, bucket)
	require.NoError(t, err)
	// Pre-insert
	ctx := context.Background()
	rps := randPoints(50, 0)
	in := utils.ProduceWithContext(ctx, rps)
	errC := inv.InsertUpdateDelete(ctx, in)
	require.NoError(t, <-errC)
	// ---------------------------
	// Search
	options := models.SearchVectorFlatOptions{
		Vector: rps[0].Vector,
		Limit:  10,
	}
	filter := roaring64.BitmapOf(rps[0].Id)
	rSet, results, err := inv.Search(ctx, options, filter)
	require.NoError(t, err)
	require.EqualValues(t, 1, rSet.GetCardinality())
	require.Len(t, results, 1)
	require.Equal(t, rps[0].Id, results[0].NodeId)
	require.Equal(t, float32(0), *results[0].Distance)
}
