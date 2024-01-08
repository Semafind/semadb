package shard

import (
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/assert"
)

func randDistElems(queryVector []float32, dists ...float32) []*CachePoint {
	elems := make([]*CachePoint, len(dists))
	for i, dist := range dists {
		elems[i] = &CachePoint{
			ShardPoint: ShardPoint{
				NodeId: uint64(i),
				Point: models.Point{
					Id:     uuid.New(),
					Vector: []float32{queryVector[0] + dist, queryVector[1] + dist},
				},
			},
		}
	}
	return elems
}

func setupDistSet(capacity int) DistSet {
	distFn, _ := distance.GetDistanceFn("euclidean")
	return NewDistSet([]float32{1.0, 2.0}, capacity, true, distFn)
}

func TestDistSet_Add(t *testing.T) {
	ds := setupDistSet(2)
	elems := randDistElems(ds.queryVector, 0.5, 1.0, 0.2)
	ds.AddPoint(elems...)
	assert.Equal(t, 3, ds.Len())
	wantOrder := []uint{0, 1, 2}
	for i, elem := range ds.items {
		assert.Equal(t, elems[wantOrder[i]].Id, elem.point.Id)
	}
	ds.Sort()
	wantOrder = []uint{2, 0, 1}
	for i, elem := range ds.items {
		assert.Equal(t, elems[wantOrder[i]].Id, elem.point.Id)
	}
}

func TestDistSet_Add_Duplicate(t *testing.T) {
	ds := setupDistSet(3)
	elems := randDistElems(ds.queryVector, 0.5, 1.0, 0.1)
	ds.AddPoint(elems...)
	ds.AddPoint(elems[0])
	assert.Equal(t, 3, ds.Len())
	ds.Sort()
	wantOrder := []uint{2, 0, 1}
	for i, elem := range ds.items {
		assert.Equal(t, elems[wantOrder[i]].Id, elem.point.Id)
	}
}

func TestDistSet_AddWithLimit(t *testing.T) {
	ds := setupDistSet(2)
	elems := randDistElems(ds.queryVector, 0.5, 1.0, 0.1)
	ds.AddPointWithLimit(elems...)
	assert.Equal(t, 2, ds.Len())
	wantOrder := []uint{2, 0}
	for i, elem := range ds.items {
		assert.Equal(t, elems[wantOrder[i]].Id, elem.point.Id)
	}
}
