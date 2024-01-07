package shard

import (
	"fmt"
	"sync"

	"github.com/bits-and-blooms/bitset"
	"github.com/semafind/semadb/distance"
)

var globalSetPool sync.Pool

func init() {
	globalSetPool = sync.Pool{
		New: func() any {
			// This initial size is important. If we set it too low, then we will
			// have to resize the bitset as we add more points. If we set it too
			// high, then we will waste memory. We can't set it too too high
			// because we'll run out of memory. The principle is around the
			// number of points we expect to see in a shard. In the database,
			// larger collections are sharded so we set a reasonable upper bound
			// of 500k. But recall that this is the maximum node id, things can
			// get ugly if a lot of points are deleted and re-inserted pushing
			// the node id up without bound. In the future we can look to address
			// this perhaps by reusing node ids.
			return bitset.New(500000)
		},
	}
}

// ---------------------------

type visitedSet interface {
	CheckAndVisit(uint64) bool
	Release()
}

// For smaller distsets we use maps because they are cheaper to allocate and
// deallocate. For larger distsets we use bitsets because they are faster to
// check and visit. We use a pool to reuse bitsets.
type VisitedMap struct {
	set map[uint64]struct{}
}

func NewVisitedMap(capacity int) *VisitedMap {
	return &VisitedMap{set: make(map[uint64]struct{}, capacity)}
}

func (vm *VisitedMap) CheckAndVisit(id uint64) bool {
	if _, ok := vm.set[id]; ok {
		return true
	}
	vm.set[id] = struct{}{}
	return false
}

func (vm *VisitedMap) Release() {
	vm.set = nil
}

type VisitedBitSet struct {
	set *bitset.BitSet
}

func NewVisitedBitSet() *VisitedBitSet {
	set := globalSetPool.Get().(*bitset.BitSet)
	set.ClearAll()
	return &VisitedBitSet{set: set}
}

func (vb *VisitedBitSet) CheckAndVisit(id uint64) bool {
	if vb.set.Test(uint(id)) {
		return true
	}
	vb.set.Set(uint(id))
	return false
}

func (vb *VisitedBitSet) Release() {
	globalSetPool.Put(vb.set)
	vb.set = nil
}

// ---------------------------

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
	set         visitedSet
	queryVector []float32
	distFn      distance.DistFunc
	sortedUntil int
}

func NewDistSet(queryVector []float32, capacity int, fast bool, distFn distance.DistFunc) DistSet {
	var set visitedSet
	if fast {
		set = NewVisitedBitSet()
	} else {
		set = NewVisitedMap(capacity)
	}
	return DistSet{queryVector: queryVector, items: make([]DistSetElem, 0, capacity), set: set, distFn: distFn}
}

// ---------------------------

func (ds *DistSet) Len() int {
	return len(ds.items)
}

func (ds *DistSet) String() string {
	return fmt.Sprintf("DistSet{items: %+v}", ds.items)
}

// ---------------------------

// Add points while respecting the capacity of the array, used in greedy search
func (ds *DistSet) AddPointWithLimit(points ...*CachePoint) {
	for _, p := range points {
		// ---------------------------
		// First check if we've seen this point before. We we have than it has
		// been considered and we can skip it. We are casting to uint because
		// we don't expect the shards to grow above couple million points. In
		// the future we can look to address this with bitsets that use
		// compression such as roaring.
		if ds.set.CheckAndVisit(p.NodeId) {
			continue
		}
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
		if ds.set.CheckAndVisit(p.NodeId) {
			continue
		}
		distance := ds.distFn(p.Vector, ds.queryVector)
		ds.items = append(ds.items, DistSetElem{distance: distance, point: p})
	}
}

func (ds *DistSet) Release() {
	ds.set.Release()
	ds.set = nil
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
