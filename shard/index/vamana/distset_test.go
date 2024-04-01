package vamana

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func setupDistSet(capacity int, maxId uint64, dists ...float32) DistSet {
	distFn := func(id uint64) float32 {
		return dists[id]
	}
	return NewDistSet(capacity, maxId, distFn)
}

func checkOrder(t *testing.T, ds DistSet, order ...uint64) {
	t.Helper()
	require.Equal(t, len(order), ds.Len())
	for i, elem := range ds.items {
		require.Equal(t, order[i], elem.Id)
	}
}

func TestDistSet_Add(t *testing.T) {
	ds := setupDistSet(2, 0, 0.5, 1.0, 0.2)
	// So adding 0 means the point distance will be 0.5, 1 means 1.0, 2 means
	ds.Add(0, 1, 2)
	checkOrder(t, ds, 0, 1, 2)
	ds.Sort()
	checkOrder(t, ds, 2, 0, 1)
}

func TestDistSet_Add_Bitset(t *testing.T) {
	ds := setupDistSet(2, 10, 0.5, 1.0, 0.2)
	ds.Add(0, 1, 2, 0)
	checkOrder(t, ds, 0, 1, 2)
	ds.Sort()
	checkOrder(t, ds, 2, 0, 1)
	ds.Release()
}

func TestDistSet_Add_Duplicate(t *testing.T) {
	ds := setupDistSet(3, 0, 0.5, 1.0, 0.1)
	ds.Add(0, 1, 2)
	ds.Add(0)
	require.Equal(t, 3, ds.Len())
	ds.Sort()
	checkOrder(t, ds, 2, 0, 1)
}

func TestDistSet_AddWithLimit(t *testing.T) {
	ds := setupDistSet(2, 0, 0.5, 1.0, 0.1, 1.2)
	ds.AddWithLimit(0, 1, 2)
	checkOrder(t, ds, 2, 0)
	ds.AddWithLimit(3, 3)
	checkOrder(t, ds, 2, 0)
}
