package collection

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDistSet_Add(t *testing.T) {
	ds := NewDistSet(2)
	ds.Add(&DistSetElem{distance: 0.5, id: "2"})
	ds.Add(&DistSetElem{distance: 1.0, id: "1"})
	ds.Add(&DistSetElem{distance: 0.2, id: "3"})
	fmt.Println(ds)
	assert.Equal(t, 3, ds.Len())
	wantOrder := []string{"2", "1", "3"}
	for i, elem := range ds.items {
		assert.Equal(t, wantOrder[i], elem.id)
	}
	ds.Sort()
	wantOrder = []string{"3", "2", "1"}
	for i, elem := range ds.items {
		assert.Equal(t, wantOrder[i], elem.id)
	}
}

func TestDistSet_Add_Duplicate(t *testing.T) {
	ds := NewDistSet(2)
	ds.Add(&DistSetElem{distance: 0.5, id: "2"})
	ds.Add(&DistSetElem{distance: 1.0, id: "1"})
	ds.Add(&DistSetElem{distance: 0.1, id: "1"})
	ds.Add(&DistSetElem{distance: 0.2, id: "3"})
	ds.Add(&DistSetElem{distance: 0.4, id: "3"})
	assert.Equal(t, 3, ds.Len())
}
