package vamana

import (
	"sync"

	"github.com/bits-and-blooms/bitset"
	"github.com/semafind/semadb/shard/vectorstore"
)

/* The bitset size is an important number to consider. If we just have a single
 * large bitset (which we did in the past) then we end up wasting or just running
 * out memory. The amount of memory a bitset uses is N/8 bytes, so it's not
 * terrible even at 5 million bits ~0.6 MiB. But recall that each search
 * operation requires a clean bitset. 10 concurrent searches means the pool will
 * yield 10x5 million bits, ~3MiB. We also consider the following:
 *
 * 1. Search is really fast. For 10 concurrent searches to happen, the requests
 * need to come at near identical times. A single shard can handle hundreds of
 * requests per second, so hopefully this is rare.
 *
 * 2. Bitsets get reused and garbage collected via sync.Pool. This means that
 * the memory is not actually allocated until it is needed. So if we have 10
 * search requests one after another, then we will only ever have 1 or 2 bitsets
 * in memory.
 *
 * 3. When run as part of a cluster, a large collection is sharded. So the
 * maximum number of points in a shard never goes crazy. Instead, multiple
 * shards work together to handle the load. These shards could be on the
 * same machine or different machines.
 *
 * The biset size is not a hard limit but we fallback to using a map if it
 * exceeds the maximum size of around 5 million for now. Greedy search will most
 * likely visit a very small subset of such a large collection, so maps are fine
 * in those cases. Finally, please note that the bitset size is not the same as
 * the number of points in the shard, instead it is the maximum node Id. For
 * example, if we have 1m points and delete the first 500k, we still need a
 * bitset of size 1m.
 *
 * We add a bit of slack to the bitset size to account for the extra start nodes.
 */
var visitBitSetSizes = []uint{110_000, 260_000, 520_000, 1_300_000, 2_600_000, 5_200_000, 10_500_000}

// The global set pool is reused between searches. This is because we don't
// want to allocate a new bitset for every search. sync.Pool gives a thread safe
// and potentially garbage collected way to reuse objects.
var globalSetPool map[uint]*sync.Pool

func init() {
	globalSetPool = make(map[uint]*sync.Pool, len(visitBitSetSizes))
	for _, size := range visitBitSetSizes {
		// This innocuous looking re-assignment is necessary because of how
		// closures work in Go. If we don't do this, then the size variable will
		// be shared between all the closures and we will end up with a single
		// pool of size 10_500_000.
		numBits := size
		globalSetPool[size] = &sync.Pool{
			New: func() any {
				return bitset.New(numBits)
			},
		}
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
	set  *bitset.BitSet
	pool *sync.Pool
}

func NewVisitedBitSet(bs *bitset.BitSet, pool *sync.Pool) *VisitedBitSet {
	set := bs
	set.ClearAll()
	return &VisitedBitSet{set: set, pool: pool}
}

func (vb *VisitedBitSet) CheckAndVisit(id uint64) bool {
	if vb.set.Test(uint(id)) {
		return true
	}
	vb.set.Set(uint(id))
	return false
}

func (vb *VisitedBitSet) Release() {
	vb.pool.Put(vb.set)
	vb.set = nil
}

// ---------------------------

type DistSetElem struct {
	Id           uint64
	distance     float32
	visited      bool
	pruneRemoved bool
}

// This data structure is exclusively used by search and robust pruning.
// Therefore, we optimise just for those cases and make assumptions about the
// inner workings of the data structure potentially breaking encapsulation. It
// may not be complete semantically and there are tricks and shortcuts that are
// taken to make it faster. Please take care when interacting with this data
// structure.
type DistSet struct {
	items       []DistSetElem
	set         visitedSet
	distFn      vectorstore.PointIdDistFn
	sortedUntil int
}

func NewDistSet(capacity int, maxNodeId uint64, distFn vectorstore.PointIdDistFn) DistSet {
	var set visitedSet
	visitSize := maxNodeId
	if visitSize == 0 || visitSize > uint64(visitBitSetSizes[len(visitBitSetSizes)-1]) {
		set = NewVisitedMap(capacity)
	} else {
		for _, size := range visitBitSetSizes {
			if visitSize <= uint64(size) {
				pool := globalSetPool[size]
				set = NewVisitedBitSet(pool.Get().(*bitset.BitSet), pool)
				break
			}
		}
	}
	return DistSet{items: make([]DistSetElem, 0, capacity), set: set, distFn: distFn}
}

// ---------------------------

func (ds *DistSet) Len() int {
	return len(ds.items)
}

// ---------------------------

// Add points while respecting the capacity of the array, used in greedy search
func (ds *DistSet) AddWithLimit(ids ...uint64) {
	for _, id := range ids {
		// ---------------------------
		// First check if we've seen this point before. We we have than it has
		// been considered and we can skip it. We are casting to uint because
		// we don't expect the shards to grow above couple million points. In
		// the future we can look to address this with bitsets that use
		// compression such as roaring.
		if ds.set.CheckAndVisit(id) {
			continue
		}
		// ---------------------------
		// If we haven't seen it before, compute the distance
		distance := ds.distFn(id)
		// Is it worth adding? Greedy search is only interested in the k closest
		// points. If we have already seen k points and this point is further
		// away than the kth point, then we can skip it.
		limit := cap(ds.items)
		if len(ds.items) == limit && distance > ds.items[limit-1].distance {
			continue
		}
		// We are going to add it, so create the new element
		newElem := DistSetElem{Id: id, distance: distance}
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
func (ds *DistSet) Add(ids ...uint64) {
	for _, id := range ids {
		if ds.set.CheckAndVisit(id) {
			continue
		}
		distance := ds.distFn(id)
		ds.items = append(ds.items, DistSetElem{Id: id, distance: distance})
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
