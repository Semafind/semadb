package flat_test

import (
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/distance"
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

func randPoints(size, offset int) []vamana.IndexVectorChange {
	points := make([]vamana.IndexVectorChange, size)
	vectorSize := 2
	for i := 0; i < size; i++ {
		randVector := make([]float32, vectorSize)
		sum := float32(0)
		for j := 0; j < vectorSize; j++ {
			randVector[j] = rand.Float32()
			sum += randVector[j]
		}
		for j := 0; j < vectorSize; j++ {
			randVector[j] /= sum
		}
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

func Test_Recall(t *testing.T) {
	distFnNames := []string{models.DistanceCosine, models.DistanceEuclidean, models.DistanceDot}
	for _, distFnName := range distFnNames {
		testName := fmt.Sprintf("distFn=%s", distFnName)
		t.Run(testName, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			params := models.IndexVectorFlatParameters{
				VectorSize:     2,
				DistanceMetric: distFnName,
			}
			inv, err := flat.NewIndexFlat(params, bucket)
			require.NoError(t, err)
			// Pre-insert
			ctx := context.Background()
			rps := randPoints(2000, 0)
			in := utils.ProduceWithContext(ctx, rps)
			errC := inv.InsertUpdateDelete(ctx, in)
			require.NoError(t, <-errC)
			// ---------------------------
			// Search
			options := models.SearchVectorFlatOptions{
				Vector: rps[0].Vector,
				Limit:  10,
			}
			// ---------------------------
			// Find ground truth
			groundTruth := make([]models.SearchResult, 0)
			for _, rp := range rps {
				distFn, _ := distance.GetFloatDistanceFn(params.DistanceMetric)
				dist := distFn(options.Vector, rp.Vector)
				groundTruth = append(groundTruth, models.SearchResult{
					NodeId:   rp.Id,
					Distance: &dist,
				})
			}
			slices.SortFunc(groundTruth, func(a, b models.SearchResult) int {
				return cmp.Compare(*a.Distance, *b.Distance)
			})
			groundTruth = groundTruth[:options.Limit]
			// ---------------------------
			rSet, results, err := inv.Search(ctx, options, nil)
			require.NoError(t, err)
			require.EqualValues(t, 10, rSet.GetCardinality())
			require.Len(t, results, 10)
			for i, res := range results {
				require.Equal(t, groundTruth[i].NodeId, res.NodeId)
				require.Equal(t, groundTruth[i].Distance, res.Distance)
			}
		})
	}
}
