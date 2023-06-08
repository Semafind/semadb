package collection

import (
	"fmt"
	"sort"
)

type DistSetElem struct {
	visited   bool
	distance  float32
	id        string
	embedding []float32
}

type DistSet struct {
	items       []*DistSetElem
	set         map[string]bool
	queryVector []float32
}

func NewDistSet(queryVector []float32, capacity int) *DistSet {
	return &DistSet{queryVector: queryVector, items: make([]*DistSetElem, 0, capacity), set: make(map[string]bool, capacity)}
}

// ---------------------------
// HEAP INTERFACE

func (ds *DistSet) Len() int {
	return len(ds.set)
}

func (ds *DistSet) Less(i, j int) bool {
	return ds.items[i].distance < ds.items[j].distance
}

func (ds *DistSet) Swap(i, j int) {
	ds.items[i], ds.items[j] = ds.items[j], ds.items[i]
}

// func (ds *DistSet) Push(x any) {
// 	item := x.(DistSetElem)
// 	ds.items = append(ds.items, &item)
// }

// func (ds *DistSet) Pop() any {
// 	n := len(ds.items)
// 	item := ds.items[n-1]
// 	ds.items[n-1] = nil // avoid memory leak
// 	ds.items = ds.items[0 : n-1]
// 	return item
// }

// ---------------------------

func (ds *DistSet) String() string {
	return fmt.Sprintf("DistSet{items: %+v, set: %+v}", ds.items, ds.set)
}

// ---------------------------

// Adding entries only computes distance if needed
func (ds *DistSet) AddEntry(entries ...*CacheEntry) {
	for _, entry := range entries {
		if ds.set[entry.Id] {
			continue
		}
		ds.set[entry.Id] = true
		distance := eucDist(entry.Embedding, ds.queryVector)
		ds.items = append(ds.items, &DistSetElem{distance: distance, id: entry.Id, embedding: entry.Embedding})
	}
}

// Add item to distance set if it is not already present
func (ds *DistSet) Add(items ...*DistSetElem) {
	for _, item := range items {
		if ds.set[item.id] {
			continue
		}
		ds.set[item.id] = true
		ds.items = append(ds.items, item)
	}
}

func (ds *DistSet) Sort() {
	sort.Sort(ds)
}

func (ds *DistSet) Contains(id string) bool {
	return ds.set[id]
}

func (ds *DistSet) Pop() *DistSetElem {
	// Find the first element in list that is still in set
	i := 0
	var toReturn *DistSetElem
	for ; i < len(ds.items); i++ {
		item := ds.items[i]
		ds.items[i] = nil // avoid memory leak
		if ds.set[item.id] {
			toReturn = item
			delete(ds.set, item.id)
			break
		}
	}
	ds.items = ds.items[(i + 1):]
	return toReturn
}

func (ds *DistSet) KeepFirstK(k int) {
	for i := k; i < len(ds.items); i++ {
		delete(ds.set, ds.items[i].id)
		ds.items[i] = nil // avoid memory leak
	}
	if k < len(ds.items) {
		ds.items = ds.items[:k]
	}
}

func (ds *DistSet) Remove(id string) {
	delete(ds.set, id)
}

// ---------------------------
