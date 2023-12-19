package shard

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/google/uuid"
)

type DistSetElem struct {
	point        *CachePoint
	distance     float32
	visited      bool
	pruneRemoved bool
}

// This data structure is exclusively used by search and robust pruning.
// Therefore, we optimise just for those cases and make assumptions about the
// inner workings of the data structure breaking encapsulation for now. It may
// not be complete semantically and there are tricks and shortcuts that are
// taken to make it faster. Please take care when interacting with this data
// structure.
type DistSet struct {
	items       []DistSetElem
	set         map[uuid.UUID]struct{} // struct{} is a zero byte type, so it takes up no space
	queryVector []float32
	distFn      func([]float32, []float32) float32
}

func NewDistSet(queryVector []float32, capacity int, distFn func([]float32, []float32) float32) DistSet {
	return DistSet{queryVector: queryVector, items: make([]DistSetElem, 0, capacity), set: make(map[uuid.UUID]struct{}, capacity), distFn: distFn}
}

// ---------------------------

func (ds *DistSet) Len() int {
	return len(ds.set)
}

func (ds *DistSet) String() string {
	return fmt.Sprintf("DistSet{items: %+v, set: %+v}", ds.items, ds.set)
}

// ---------------------------

// Adding entries only computes distance if needed
func (ds *DistSet) AddPoint(points ...*CachePoint) {
	for _, p := range points {
		if _, ok := ds.set[p.Id]; ok {
			continue
		}
		ds.set[p.Id] = struct{}{}
		distance := ds.distFn(p.Vector, ds.queryVector)
		ds.items = append(ds.items, DistSetElem{distance: distance, point: p})
	}
}

// Add item to distance set if it is not already present
func (ds *DistSet) AddAlreadyUnique(items ...DistSetElem) {
	ds.items = append(ds.items, items...)
}

func (ds *DistSet) Sort() {
	slices.SortFunc(ds.items, func(a, b DistSetElem) int {
		return cmp.Compare(a.distance, b.distance)
	})
}

// ---------------------------
