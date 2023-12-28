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
	return len(ds.items)
}

func (ds *DistSet) String() string {
	return fmt.Sprintf("DistSet{items: %+v, set: %+v}", ds.items, ds.set)
}

// ---------------------------

// Add points while respecting the capacity of the array, used in greedy search
func (ds *DistSet) AddPointWithLimit(points ...*CachePoint) {
	for _, p := range points {
		// ---------------------------
		// First check if we've seen this point before. We we have than it has
		// been considered and we can skip it.
		if _, ok := ds.set[p.Id]; ok {
			continue
		}
		ds.set[p.Id] = struct{}{}
		// ---------------------------
		// If we haven't seen it before, compute the distance
		distance := ds.distFn(p.Vector, ds.queryVector)
		// Is it worth adding? Greedy search is only interested in the k closest
		// points. If we have already seen k points and this point is further
		// away than the kth point, then we can skip it.
		limit := cap(ds.items)
		if len(ds.items) == limit && distance > ds.items[limit-1].distance {
			continue
		}
		// We are going to add it, so create the new element
		newElem := DistSetElem{distance: distance, point: p}
		if len(ds.items) < limit {
			ds.items = append(ds.items, newElem)
			ds.sortedUntil++
		} else {
			ds.items[len(ds.items)-1] = newElem
		}
		// Insert new element into the array in sorted order.
		for i := len(ds.items) - 1; i > 0 && ds.items[i].distance < ds.items[i-1].distance; i-- {
			ds.items[i], ds.items[i-1] = ds.items[i-1], ds.items[i]
		}
	}
}

// Add entries and only computes distance if the point is never seen before
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

// Add item to distance set without checking for duplicates
func (ds *DistSet) AddAlreadyUnique(items ...DistSetElem) {
	ds.items = append(ds.items, items...)
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
