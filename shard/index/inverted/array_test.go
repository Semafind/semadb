package inverted_test

import (
	"context"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/index/inverted"
	"github.com/stretchr/testify/require"
)

var arrayItems = [][]int64{
	{1, 2, 3},
	{2, 3, 4},
	{3, 4, 5},
}

var arrayInverseMapping = map[int64]*roaring64.Bitmap{
	1: roaring64.BitmapOf(0),
	2: roaring64.BitmapOf(0, 1),
	3: roaring64.BitmapOf(0, 1, 2),
	4: roaring64.BitmapOf(1, 2),
	5: roaring64.BitmapOf(2),
}

func setupArrayIndex(t *testing.T) *inverted.IndexInvertedArray[int64] {
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInvertedArray[int64](b)
	in := make(chan inverted.IndexArrayChange[int64])
	errC := inv.InsertUpdateDelete(context.Background(), in)
	for i, v := range arrayItems {
		in <- inverted.IndexArrayChange[int64]{
			Id:          uint64(i),
			CurrentData: v,
		}
	}
	close(in)
	require.NoError(t, <-errC)
	checkTermCount(t, b, 5)
	return inv
}

func TestArray_Insert(t *testing.T) {
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInvertedArray[int64](b)
	in := make(chan inverted.IndexArrayChange[int64])
	errC := inv.InsertUpdateDelete(context.Background(), in)
	for i, v := range arrayItems {
		in <- inverted.IndexArrayChange[int64]{
			Id:          uint64(i),
			CurrentData: v,
		}
	}
	close(in)
	require.NoError(t, <-errC)
	checkTermCount(t, b, 5)
}

func TestArray_Persistance(t *testing.T) {
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInvertedArray[int64](b)
	in := make(chan inverted.IndexArrayChange[int64])
	errC := inv.InsertUpdateDelete(context.Background(), in)
	for i, v := range arrayItems {
		in <- inverted.IndexArrayChange[int64]{
			Id:          uint64(i),
			CurrentData: v,
		}
	}
	close(in)
	require.NoError(t, <-errC)
	checkTermCount(t, b, 5)
	// ---------------------------
	inv2 := inverted.NewIndexInvertedArray[int64](b)
	for k, v := range arrayInverseMapping {
		set, err := inv2.Search([]int64{k}, models.OperatorContainsAll)
		require.NoError(t, err)
		require.True(t, set.Equals(v))
	}
}

func TestArray_ConcurrentCUDD(t *testing.T) {
	inv := setupArrayIndex(t)
	in := make(chan inverted.IndexArrayChange[int64])
	errC := inv.InsertUpdateDelete(context.Background(), in)
	// Pre-insert with initial items
	for i, v := range arrayItems {
		in <- inverted.IndexArrayChange[int64]{
			Id:          uint64(i),
			CurrentData: v,
		}
	}
	// ---------------------------
	var wg sync.WaitGroup
	wg.Add(3)
	// Concurrent insert
	go func() {
		for i := 10; i < 12; i++ {
			in <- inverted.IndexArrayChange[int64]{
				Id:          uint64(i),
				CurrentData: []int64{1, 2, 3},
			}
		}
		wg.Done()
	}()
	// ---------------------------
	// Concurrent update
	go func() {
		for i, v := range arrayItems {
			if v[0]%2 == 0 {
				in <- inverted.IndexArrayChange[int64]{
					Id:           uint64(i),
					PreviousData: v,
					CurrentData:  []int64{6, 7},
				}
			}
		}
		wg.Done()
	}()
	// ---------------------------
	// Concurrent delete
	go func() {
		for i, v := range arrayItems {
			if v[0]%2 != 0 {
				in <- inverted.IndexArrayChange[int64]{
					Id:           uint64(i),
					PreviousData: v,
				}
			}
		}
		wg.Done()
	}()
	// ---------------------------
	wg.Wait()
	close(in)
	require.NoError(t, <-errC)
	// ---------------------------
	newInverseMapping := map[int64]*roaring64.Bitmap{
		1: roaring64.BitmapOf(10, 11),
		2: roaring64.BitmapOf(10, 11),
		3: roaring64.BitmapOf(10, 11),
		4: roaring64.BitmapOf(),
		5: roaring64.BitmapOf(),
		6: roaring64.BitmapOf(1),
		7: roaring64.BitmapOf(1),
	}
	for k, v := range newInverseMapping {
		set, err := inv.Search([]int64{k}, models.OperatorContainsAll)
		require.NoError(t, err)
		require.True(t, set.Equals(v))
	}
}

func TestArray_Search(t *testing.T) {
	inv := setupArrayIndex(t)
	// ---------------------------
	// var arrayItems = [][]int64{
	// 	{1, 2, 3},
	// 	{2, 3, 4},
	// 	{3, 4, 5},
	// }
	tests := []struct {
		name        string
		query       []int64
		operator    string
		expectedIds []uint64
	}{
		{"singleAll", []int64{1}, models.OperatorContainsAll, []uint64{0}},
		{"singleAlltwoDoc", []int64{2}, models.OperatorContainsAll, []uint64{0, 1}},
		{"twoAll", []int64{1, 2}, models.OperatorContainsAll, []uint64{0}},
		{"twoAlltwoDoc", []int64{1, 2}, models.OperatorContainsAny, []uint64{0, 1}},
		{"threeAll", []int64{1, 2, 3}, models.OperatorContainsAll, []uint64{0}},
		{"threeAllthreeDoc", []int64{1, 2, 3}, models.OperatorContainsAny, []uint64{0, 1, 2}},
		{"empty", []int64{1, 2, 3, 4}, models.OperatorContainsAll, []uint64{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := inv.Search(test.query, test.operator)
			require.NoError(t, err)
			groundTruth := roaring64.BitmapOf(test.expectedIds...)
			require.True(t, res.Equals(groundTruth))
		})
	}
}
