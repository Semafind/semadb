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

var storeTypes = []string{models.QuantizerNone}

func checkBucketIsEmpty(t *testing.T, bucket diskstore.Bucket, empty bool) {
	t.Helper()
	count := 0
	bucket.ForEach(func(key []byte, value []byte) error {
		count++
		return nil
	})
	require.Equal(t, empty, count == 0)
}

func Test_ExistsSetDeleteFlush(t *testing.T) {
	distFn, _ := distance.GetDistanceFn("cosine")
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			s, err := vectorstore.New(storeType, bucket, distFn)
			require.NoError(t, err)
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
	distFn, _ := distance.GetDistanceFn("cosine")
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			s, err := vectorstore.New(storeType, bucket, distFn)
			require.NoError(t, err)
			require.NoError(t, s.Set(1, []float32{1, 2, 3}))
			require.NoError(t, s.Flush())
			s2, err := vectorstore.New(storeType, bucket, distFn)
			require.NoError(t, err)
			require.True(t, s2.Exists(1))
		})
	}
}

func Test_DistanceFromFloat(t *testing.T) {
	distFn, _ := distance.GetDistanceFn("euclidean")
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			s, err := vectorstore.New(storeType, bucket, distFn)
			require.NoError(t, err)
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
	distFn, _ := distance.GetDistanceFn("euclidean")
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			bucket := diskstore.NewMemBucket(false)
			s, err := vectorstore.New(storeType, bucket, distFn)
			require.NoError(t, err)
			require.NoError(t, s.Set(1, []float32{1, 2, 3}))
			require.NoError(t, s.Set(2, []float32{4, 5, 6}))
			dist := s.DistanceFromPoint(1)
			require.Equal(t, float32(0), dist(1))
			require.Less(t, dist(1), dist(2))
			require.Equal(t, float32(math.MaxFloat32), dist(3))
		})
	}
}
