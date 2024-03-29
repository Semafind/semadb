package vectorstore_test

import (
	"math"
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/vectorstore"
	"github.com/stretchr/testify/require"
)

var storeTypes = []string{models.QuantizerNone, models.QuantizerBinary}

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
	distFn, _ := distance.GetDistanceFn("euclidean")
	var s vectorstore.VectorStore
	var err error
	switch storeType {
	case models.QuantizerNone:
		params := models.PlainQuantizerParameters{}
		s, err = vectorstore.New(params, bucket, distFn)
	case models.QuantizerBinary:
		params := models.BinaryQuantizerParamaters{
			Threshold:        nil,
			TriggerThreshold: 5,
		}
		s, err = vectorstore.New(params, bucket, distFn)
	}
	require.NoError(t, err)
	return s
}

func Test_ExistsSetDeleteFlush(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			s := setupVectorStore(t, storeType, bucket)
			require.False(t, s.Exists(1))
			require.NoError(t, s.Set(1, []float32{1, 2, 3}))
			require.True(t, s.Exists(1))
			checkBucketIsEmpty(t, bucket, true)
			require.NoError(t, s.Flush())
			checkBucketIsEmpty(t, bucket, false)
			require.NoError(t, s.Delete(1))
			checkBucketIsEmpty(t, bucket, false)
			require.False(t, s.Exists(1))
			require.NoError(t, s.Flush())
			checkBucketIsEmpty(t, bucket, true)
		})
	}
}

func Test_Persistance(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			s := setupVectorStore(t, storeType, bucket)
			require.NoError(t, s.Set(1, []float32{1, 2, 3}))
			require.NoError(t, s.Flush())
			s2 := setupVectorStore(t, storeType, bucket)
			require.True(t, s2.Exists(1))
		})
	}
}

func Test_DistanceFromFloat(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			s := setupVectorStore(t, storeType, bucket)
			require.NoError(t, s.Set(1, []float32{1, 2, 3}))
			require.NoError(t, s.Set(2, []float32{4, 5, 6}))
			dist := s.DistanceFromFloat([]float32{1, 2, 3})
			require.Equal(t, float32(0), dist(1))
			require.Less(t, dist(1), dist(2))
			require.Equal(t, float32(math.MaxFloat32), dist(3))
		})
	}
}

func Test_DistanceFromPoint(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			s := setupVectorStore(t, storeType, bucket)
			require.NoError(t, s.Set(1, []float32{1, 2, 3}))
			require.NoError(t, s.Set(2, []float32{4, 5, 6}))
			dist := s.DistanceFromPoint(1)
			require.Equal(t, float32(0), dist(1))
			require.Less(t, dist(1), dist(2))
			require.Equal(t, float32(math.MaxFloat32), dist(3))
		})
	}
}
