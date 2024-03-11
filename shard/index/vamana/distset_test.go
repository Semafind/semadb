package vamana

import (
	"testing"

	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/shard/cache"
	"github.com/stretchr/testify/require"
)

func randDistElems(queryVector []float32, dists ...float32) []*cache.CachePoint {
	elems := make([]*cache.CachePoint, len(dists))
	for i, dist := range dists {
		elems[i] = &cache.CachePoint{
			ShardPoint: cache.ShardPoint{
				NodeId: uint64(i),
				Vector: []float32{queryVector[0] + dist, queryVector[1] + dist},
			},
		}
	}
	return elems
}

func setupDistSet(capacity int) DistSet {
	distFn, _ := distance.GetDistanceFn("euclidean")
	return NewDistSet([]float32{1.0, 2.0}, capacity, 0, distFn)
}

func TestDistSet_Add(t *testing.T) {
	ds := setupDistSet(2)
	elems := randDistElems(ds.queryVector, 0.5, 1.0, 0.2)
	ds.AddPoint(elems...)
	require.Equal(t, 3, ds.Len())
	wantOrder := []uint{0, 1, 2}
	for i, elem := range ds.items {
		require.Equal(t, elems[wantOrder[i]].NodeId, elem.point.NodeId)
	}
	ds.Sort()
	wantOrder = []uint{2, 0, 1}
	for i, elem := range ds.items {
		require.Equal(t, elems[wantOrder[i]].NodeId, elem.point.NodeId)
	}
}

func TestDistSet_Add_Duplicate(t *testing.T) {
	ds := setupDistSet(3)
	elems := randDistElems(ds.queryVector, 0.5, 1.0, 0.1)
	ds.AddPoint(elems...)
	ds.AddPoint(elems[0])
	require.Equal(t, 3, ds.Len())
	ds.Sort()
	wantOrder := []uint{2, 0, 1}
	for i, elem := range ds.items {
		require.Equal(t, elems[wantOrder[i]].NodeId, elem.point.NodeId)
	}
}

func TestDistSet_AddWithLimit(t *testing.T) {
	ds := setupDistSet(2)
	elems := randDistElems(ds.queryVector, 0.5, 1.0, 0.1)
	ds.AddPointWithLimit(elems...)
	require.Equal(t, 2, ds.Len())
	wantOrder := []uint{2, 0}
	for i, elem := range ds.items {
		require.Equal(t, elems[wantOrder[i]].NodeId, elem.point.NodeId)
	}
}
