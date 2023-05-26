package collection

import (
	"fmt"
	"sort"
)

type DistSetElem struct {
	visited  bool
	distance float32
	id       string
}

type DistSet struct {
	items []*DistSetElem
	set   map[string]bool
}

func NewDistSet(capacity int) *DistSet {
	return &DistSet{items: make([]*DistSetElem, 0, capacity), set: make(map[string]bool, capacity)}
}

// ---------------------------
// HEAP INTERFACE

func (ds *DistSet) Len() int {
	return len(ds.items)
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

func (ds *DistSet) KeepFirstK(k int) {
	for i := k; i < ds.Len(); i++ {
		delete(ds.set, ds.items[i].id)
		ds.items[i] = nil // avoid memory leak
	}
	ds.items = ds.items[:k]
}

// ---------------------------
