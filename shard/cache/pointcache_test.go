package cache

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
)

// ---------------------------

type tempDiskStore struct {
	items    map[string][]byte
	readOnly bool
}

func newTempDiskStore(readOnly bool) diskStore {
	return &tempDiskStore{
		items:    make(map[string][]byte),
		readOnly: readOnly,
	}
}

func (t *tempDiskStore) Get(key []byte) []byte {
	return t.items[string(key)]
}

var ErrReadOnly = errors.New("read only")

func (t *tempDiskStore) Put(key []byte, value []byte) error {
	if t.readOnly {
		return ErrReadOnly
	}
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
	if t.readOnly {
		return ErrReadOnly
	}
	delete(t.items, string(key))
	return nil
}

func (t *tempDiskStore) Writable() bool {
	return !t.readOnly
}

// ---------------------------

func randCachePoints(size int) []*CachePoint {
	ps := make([]*CachePoint, size)
	for i := 0; i < size; i++ {
		ps[i] = &CachePoint{
			ShardPoint: ShardPoint{
				NodeId: uint64(i + 1),
				Point: models.Point{
					Id:       uuid.New(),
					Vector:   []float32{1, 2, 3},
					Metadata: []byte("metadata"),
				},
			},
		}
	}
	return ps
}

func tempPointCache(t *testing.T) *PointCache {
	pc := newPointCache(newTempDiskStore(false), newSharedInMemCache())
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

func TestPointCache_GetPointByUUID(t *testing.T) {
	t.Run("Empty store", func(t *testing.T) {
		pc := tempPointCache(t)
		_, err := pc.GetPointByUUID(uuid.New())
		require.Error(t, err)
	})
	/* We don't have a way to get from cache using UUID. The points are stored by
	 * Node Id not UUID. */
	t.Run("From disk", func(t *testing.T) {
		pc := tempPointCache(t)
		cachePoint := randCachePoints(1)[0]
		pc.SetPoint(cachePoint.ShardPoint)
		pc.Flush()
		pc2 := tempPointCache(t)
		pc2.bucket = pc.bucket
		p, err := pc2.GetPointByUUID(cachePoint.Id)
		require.NoError(t, err)
		require.Equal(t, cachePoint.NodeId, p.NodeId)
		require.Equal(t, cachePoint.Vector, p.Vector)
	})
}

func TestPointCache_SetPoint(t *testing.T) {
	pc := tempPointCache(t)
	randPoints := randCachePoints(10)
	for _, p := range randPoints {
		pc.SetPoint(p.ShardPoint)
	}
	require.Len(t, pc.sharedCache.points, 10)
	require.Greater(t, pc.sharedCache.estimatedSize.Load(), int64(0))
}

func TestPointCache_Neighbours(t *testing.T) {
	pc := tempPointCache(t)
	randPoints := randCachePoints(10)
	randPoints[0].AddNeighbour(randPoints[1])
	randPoints[0].AddNeighbour(randPoints[2])
	randPoints[0].AddNeighbour(randPoints[3])
	for _, p := range randPoints {
		pc.SetPoint(p.ShardPoint)
	}
	// ---------------------------
	// We run this twice to see if the cached neighbours are persisted across
	// calls, that is the call (1) gets neighbours from cache and stores as a
	// list on the point, (2) just re-uses the list from the point.
	for i := 0; i < 2; i++ {
		err := pc.WithReadOnlyPointNeighbours(randPoints[0], func(neighbours []*CachePoint) error {
			require.Len(t, neighbours, 3)
			require.Equal(t, randPoints[1].NodeId, neighbours[0].NodeId)
			require.Equal(t, randPoints[2].NodeId, neighbours[1].NodeId)
			require.Equal(t, randPoints[3].NodeId, neighbours[2].NodeId)
			return nil
		})
		require.NoError(t, err)
	}
}

func TestPointCache_Flush(t *testing.T) {
	pc := tempPointCache(t)
	randPoints := randCachePoints(10)
	for _, p := range randPoints {
		pc.SetPoint(p.ShardPoint)
	}
	// We'll have some edge dirty and deleted points too
	pc.sharedCache.points[1].isDirty = false
	pc.sharedCache.points[2].isDirty = false
	pc.sharedCache.points[2].isEdgeDirty = true
	pc.sharedCache.points[3].isDeleted = true
	require.NoError(t, pc.Flush())
	// ---------------------------
	for _, p := range randPoints {
		if p.NodeId == 3 {
			continue
		}
		require.Equal(t, p, pc.sharedCache.points[p.NodeId])
		require.False(t, p.isDirty)
		require.False(t, p.isEdgeDirty)
		require.False(t, p.isDeleted)
	}
	require.Len(t, pc.sharedCache.points, 9)
}
