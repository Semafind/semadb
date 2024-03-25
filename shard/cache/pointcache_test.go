package cache

import (
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/stretchr/testify/require"
)

// ---------------------------

func randCachePoints(size int) []*CachePoint {
	ps := make([]*CachePoint, size)
	for i := 0; i < size; i++ {
		ps[i] = &CachePoint{
			GraphNode: GraphNode{
				NodeId: uint64(i + 1),
				Vector: []float32{1, 2, 3},
			},
		}
	}
	return ps
}

func tempPointCache() *pointCache {
	graphBucket := diskstore.NewMemBucket(false)
	return &pointCache{
		graphBucket: graphBucket,
		sharedCache: newSharedInMemCache(),
	}
}

func TestPointCache_GetPoint(t *testing.T) {
	t.Run("Empty store", func(t *testing.T) {
		pc := tempPointCache()
		_, err := pc.GetPoint(1)
		require.Error(t, err)
	})
	t.Run("From cache", func(t *testing.T) {
		pc := tempPointCache()
		cachePoint := randCachePoints(1)[0]
		pc.sharedCache.points[cachePoint.NodeId] = cachePoint
		p, err := pc.GetPoint(cachePoint.NodeId)
		require.NoError(t, err)
		require.Equal(t, cachePoint, p)
	})
	t.Run("From disk", func(t *testing.T) {
		pc := tempPointCache()
		cachePoint := randCachePoints(1)[0]
		pc.SetPoint(cachePoint.GraphNode)
		pc.flush()
		pc2 := tempPointCache()
		// pc2.bucket = pc.bucket
		pc2.graphBucket = pc.graphBucket
		p, err := pc2.GetPoint(cachePoint.NodeId)
		require.NoError(t, err)
		require.Equal(t, cachePoint.NodeId, p.NodeId)
		require.Equal(t, cachePoint.Vector, p.Vector)
	})
}

func TestPointCache_SetPoint(t *testing.T) {
	pc := tempPointCache()
	randPoints := randCachePoints(10)
	for _, p := range randPoints {
		pc.SetPoint(p.GraphNode)
	}
	require.Len(t, pc.sharedCache.points, 10)
	require.Greater(t, pc.sharedCache.estimatedSize.Load(), int64(0))
}

func TestPointCache_Neighbours(t *testing.T) {
	pc := tempPointCache()
	randPoints := randCachePoints(10)
	randPoints[0].AddNeighbour(randPoints[1])
	randPoints[0].AddNeighbour(randPoints[2])
	randPoints[0].AddNeighbour(randPoints[3])
	for _, p := range randPoints {
		pc.SetPoint(p.GraphNode)
	}
	// ---------------------------
	// We run this twice to see if the cached neighbours are persisted across
	// calls, that is the call (1) gets neighbours from cache and stores as a
	// list on the point, (2) just re-uses the list from the point.
	for i := 0; i < 2; i++ {
		err := pc.WithPointNeighbours(randPoints[0], true, func(neighbours []*CachePoint) error {
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
	pc := tempPointCache()
	randPoints := randCachePoints(10)
	for _, p := range randPoints {
		pc.SetPoint(p.GraphNode)
	}
	// We'll have some edge dirty and deleted points too
	pc.sharedCache.points[1].isDirty = false
	pc.sharedCache.points[2].isDirty = false
	pc.sharedCache.points[2].isEdgeDirty = true
	pc.sharedCache.points[3].isDeleted = true
	require.NoError(t, pc.flush())
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

func TestPointCache_ReadOnly(t *testing.T) {
	pc := tempPointCache()
	pc.isReadOnly = true
	// ---------------------------
	require.True(t, pc.IsReadOnly())
	_, err := pc.SetPoint(GraphNode{NodeId: 1})
	require.Error(t, err)
	err = pc.WithPointNeighbours(&CachePoint{}, false, func([]*CachePoint) error {
		return nil
	})
	require.Error(t, err)
	err = pc.flush()
	require.Error(t, err)
}
