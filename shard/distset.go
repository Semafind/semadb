package shard

import (
	"fmt"

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
	sortedUntil int
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

func (ds *DistSet) KeepFirstK(k int) {
	if len(ds.items) > k {
		ds.items = ds.items[:k]
	}
	if ds.sortedUntil > k {
		ds.sortedUntil = k
	}
}

func (ds *DistSet) Sort() {
	/* We are using insertion here because we are assuming that the array is
	 * mostly sorted. As new points are added to the set, they are appended to
	 * the end of the array. Note that during greedy search, points are appended
	 * never removed. So we only need to sort the unsorted part of the array.
	 * Compared to slices.SortFunc, the compiler can inline this operation,
	 * making it faster. */
	// Perform insertion sort on the unsorted part of the array, sorting in
	// ascending order
	for i := ds.sortedUntil; i < len(ds.items); i++ {
		for j := i; j > 0 && ds.items[j].distance < ds.items[j-1].distance; j-- {
			ds.items[j], ds.items[j-1] = ds.items[j-1], ds.items[j]
		}
	}
	ds.sortedUntil = len(ds.items)
}

// ---------------------------
