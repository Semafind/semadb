package shard

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/assert"
)

func randDistElems(dists ...float32) []DistSetElem {
	elems := make([]DistSetElem, len(dists))
	for i, dist := range dists {
		rid := uuid.New()
		cp := &CachePoint{
			ShardPoint: ShardPoint{
				Point: models.Point{
					Id: rid,
				},
			},
		}
		elems[i] = DistSetElem{distance: dist, point: cp}
	}
	return elems
}

func TestDistSet_Add(t *testing.T) {
	ds := NewDistSet([]float32{1.0, 2.0}, 2, eucDist)
	elems := randDistElems(0.5, 1.0, 0.2)
	ds.Add(elems...)
	assert.Equal(t, 3, ds.Len())
	wantOrder := []uint{0, 1, 2}
	for i, elem := range ds.items {
		assert.Equal(t, elems[wantOrder[i]].point.Id, elem.point.Id)
	}
	ds.Sort()
	fmt.Println(ds)
	wantOrder = []uint{2, 0, 1}
	for i, elem := range ds.items {
		assert.Equal(t, elems[wantOrder[i]].point.Id, elem.point.Id)
	}
}

func TestDistSet_Add_Duplicate(t *testing.T) {
	ds := NewDistSet([]float32{1.0, 2.0}, 3, eucDist)
	elems := randDistElems(0.5, 1.0, 0.1)
	ds.Add(elems...)
	ds.Add(elems[0])
	assert.Equal(t, 3, ds.Len())
	ds.Sort()
	wantOrder := []uint{2, 0, 1}
	for i, elem := range ds.items {
		assert.Equal(t, elems[wantOrder[i]].point.Id, elem.point.Id)
	}
}

func TestDistSet_KeepFirstK(t *testing.T) {
	ds := NewDistSet([]float32{1.0, 2.0}, 3, eucDist)
	elems := randDistElems(0.5, 1.0, 0.2)
	ds.Add(elems...)
	ds.Sort()
	ds.KeepFirstK(2)
	assert.Equal(t, 2, ds.Len())
	wantOrder := []uint{2, 0}
	for i, elem := range ds.items {
		assert.Equal(t, elems[wantOrder[i]].point.Id, elem.point.Id)
	}
}

func TestDistSet_Pop_Remove(t *testing.T) {
	ds := NewDistSet([]float32{1.0, 2.0}, 3, eucDist)
	elems := randDistElems(0.5, 1.0, 0.2)
	ds.Add(elems...)
	ds.Sort()
	assert.Equal(t, 3, ds.Len())
	assert.Equal(t, elems[2].point.Id, ds.Pop().point.Id)
	assert.False(t, ds.Contains(elems[2].point.Id))
	ds.Remove(elems[0].point.Id)
	assert.Equal(t, 1, ds.Len())
	assert.Equal(t, elems[1].point.Id, ds.Pop().point.Id)
	assert.Equal(t, 0, ds.Len())
}
