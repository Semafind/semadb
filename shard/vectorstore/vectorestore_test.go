package vectorstore_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/vectorstore"
	"github.com/stretchr/testify/require"
)

var storeTypes = []string{models.QuantizerNone, models.QuantizerBinary, models.QuantizerProduct}

func checkBucketIsEmpty(t *testing.T, bucket diskstore.Bucket, empty bool) {
	t.Helper()
	count := 0
	bucket.ForEach(func(key []byte, value []byte) error {
		count++
		return nil
	})
	require.Equal(t, empty, count == 0)
}

func setupVectorStore(t *testing.T, storeType string, bucket diskstore.Bucket) vectorstore.VectorStore {
	t.Helper()
	var s vectorstore.VectorStore
	var err error
	switch storeType {
	case models.QuantizerNone:
		params := models.PlainQuantizerParameters{}
		s, err = vectorstore.New(params, bucket, models.DistanceEuclidean, 4)
	case models.QuantizerBinary:
		params := models.BinaryQuantizerParamaters{
			Threshold:        nil,
			TriggerThreshold: 5,
		}
		s, err = vectorstore.New(params, bucket, models.DistanceEuclidean, 4)
	case models.QuantizerProduct:
		params := models.ProductQuantizerParameters{
			NumCentroids:     256,
			NumSubVectors:    2,
			TriggerThreshold: 5,
		}
		s, err = vectorstore.New(params, bucket, models.DistanceEuclidean, 4)
	}
	require.NoError(t, err)
	return s
}

func triggerFit(t *testing.T, s vectorstore.VectorStore) {
	t.Helper()
	require.NoError(t, s.Set(1, []float32{1, 2, 3, 4}))
	require.NoError(t, s.Set(2, []float32{4, 5, 6, 7}))
	require.NoError(t, s.Set(3, []float32{7, 8, 9, 10}))
	require.NoError(t, s.Set(4, []float32{-10, -11, -12, -13}))
	require.NoError(t, s.Set(5, []float32{-13, 14, -15, 16}))
	require.NoError(t, s.Fit())
}

func Test_Fit(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			s := setupVectorStore(t, storeType, bucket)
			triggerFit(t, s)
		})
	}
}

func Test_ExistsSetDeleteFlush(t *testing.T) {
	for _, storeType := range storeTypes {
		for _, trigger := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/fit=%v", storeType, trigger), func(t *testing.T) {
				bucket := diskstore.NewMemBucket(false)
				s := setupVectorStore(t, storeType, bucket)
				checkBucketIsEmpty(t, bucket, true)
				if trigger {
					triggerFit(t, s)
				}
				require.False(t, s.Exists(7))
				require.NoError(t, s.Set(7, []float32{1, 2, 3, 4}))
				require.True(t, s.Exists(7))
				require.NoError(t, s.Flush())
				checkBucketIsEmpty(t, bucket, false)
				require.NoError(t, s.Delete(7))
				checkBucketIsEmpty(t, bucket, false)
				require.False(t, s.Exists(7))
				require.NoError(t, s.Flush())
			})
		}
	}
}

func Test_Persistance(t *testing.T) {
	for _, storeType := range storeTypes {
		for _, trigger := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/fit=%v", storeType, trigger), func(t *testing.T) {
				bucket := diskstore.NewMemBucket(false)
				s := setupVectorStore(t, storeType, bucket)
				if trigger {
					triggerFit(t, s)
				}
				require.NoError(t, s.Set(7, []float32{1, 2, 3, 4}))
				require.NoError(t, s.Flush())
				s2 := setupVectorStore(t, storeType, bucket)
				require.True(t, s2.Exists(7))
			})
		}
	}
}

func Test_DistanceFromFloat(t *testing.T) {
	for _, storeType := range storeTypes {
		for _, trigger := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/fit=%v", storeType, trigger), func(t *testing.T) {
				bucket := diskstore.NewMemBucket(false)
				s := setupVectorStore(t, storeType, bucket)
				if trigger {
					triggerFit(t, s)
				}
				require.NoError(t, s.Set(7, []float32{1, 2, 3, 4}))
				require.NoError(t, s.Set(8, []float32{4, 5, 6, 7}))
				dist := s.DistanceFromFloat([]float32{1, 2, 3, 4})
				require.Equal(t, float32(0), dist(7))
				require.Less(t, dist(7), dist(8))
				require.Equal(t, float32(math.MaxFloat32), dist(42))
			})
		}
	}
}

func Test_DistanceFromPoint(t *testing.T) {
	for _, storeType := range storeTypes {
		for _, trigger := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/fit=%v", storeType, trigger), func(t *testing.T) {
				bucket := diskstore.NewMemBucket(false)
				s := setupVectorStore(t, storeType, bucket)
				if trigger {
					triggerFit(t, s)
				}
				require.NoError(t, s.Set(7, []float32{1, 2, 3, 4}))
				require.NoError(t, s.Set(8, []float32{4, 5, 6, 7}))
				dist := s.DistanceFromPoint(7)
				require.Equal(t, float32(0), dist(7))
				require.Less(t, dist(7), dist(8))
				require.Equal(t, float32(math.MaxFloat32), dist(42))
			})
		}
	}
}
