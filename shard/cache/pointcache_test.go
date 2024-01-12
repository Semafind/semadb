package cache

import (
	"testing"

	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
)

// ---------------------------

type tempDiskStore struct {
	items map[string][]byte
}

func newTempDiskStore() diskStore {
	return &tempDiskStore{
		items: make(map[string][]byte),
	}
}

func (t *tempDiskStore) Get(key []byte) []byte {
	return t.items[string(key)]
}

func (t *tempDiskStore) Put(key []byte, value []byte) error {
	t.items[string(key)] = value
	return nil
}

func (t *tempDiskStore) ForEach(f func(k, v []byte) error) error {
	for k, v := range t.items {
		if err := f([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

func (t *tempDiskStore) Delete(key []byte) error {
	delete(t.items, string(key))
	return nil
}

func (t *tempDiskStore) Writable() bool {
	return true
}

// ---------------------------

func randCachePoints(size int) []*CachePoint {
	ps := make([]*CachePoint, size)
	for i := 0; i < size; i++ {
		ps[i] = &CachePoint{
			ShardPoint: ShardPoint{
				NodeId: uint64(i + 1),
				Point: models.Point{
					Vector:   []float32{1, 2, 3},
					Metadata: []byte("metadata"),
				},
			},
		}
	}
	return ps
}

func tempPointCache(t *testing.T) *PointCache {
	pc := newPointCache(newTempDiskStore(), newSharedInMemCache())
	return pc
}

func TestPointCache_GetPoint(t *testing.T) {
	t.Run("Empty store", func(t *testing.T) {
		pc := tempPointCache(t)
		_, err := pc.GetPoint(1)
		require.Error(t, err)
	})
	t.Run("From cache", func(t *testing.T) {
		pc := tempPointCache(t)
		cachePoint := randCachePoints(1)[0]
		pc.sharedCache.points[cachePoint.NodeId] = cachePoint
		p, err := pc.GetPoint(cachePoint.NodeId)
		require.NoError(t, err)
		require.Equal(t, cachePoint, p)
	})
	t.Run("From disk", func(t *testing.T) {
		pc := tempPointCache(t)
		cachePoint := randCachePoints(1)[0]
		pc.SetPoint(cachePoint.ShardPoint)
		pc.Flush()
		pc2 := tempPointCache(t)
		pc2.bucket = pc.bucket
		p, err := pc2.GetPoint(cachePoint.NodeId)
		require.NoError(t, err)
		require.Equal(t, cachePoint.NodeId, p.NodeId)
		require.Equal(t, cachePoint.Vector, p.Vector)
	})
}

// func TestPointCacheFlush(t *testing.T) {
// 	ps := randCachePoints(10)
// 	store := newSharedInMemStore()
// 	withTempBucket(t, false, func(b *bbolt.Bucket) {
// 		pc := newPointCache(b, store)
// 		for _, p := range ps {
// 			_, err := pc.SetPoint(p.ShardPoint)
// 			require.NoError(t, err)
// 		}
// 		require.Greater(t, store.estimatedSize.Load(), int32(0))
// 		require.NoError(t, pc.Flush())
// 	})
// 	require.Equal(t, 10, len(ps))
// 	for _, p := range ps {
// 		require.Equal(t, p.isDirty, false)
// 	}
// }
