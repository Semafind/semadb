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

var items = []int64{1, 1, 2, 2, 2, 3, 4, 4, 4}
var inverseMapping = map[int64]*roaring64.Bitmap{
	1: roaring64.BitmapOf(0, 1),
	2: roaring64.BitmapOf(2, 3, 4),
	3: roaring64.BitmapOf(5),
	4: roaring64.BitmapOf(6, 7, 8),
}

func checkTermCount(t *testing.T, b diskstore.Bucket, expected int) {
	var count int
	err := b.ForEach(func(k, v []byte) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, expected, count)
}

/* Testing for one type should cover the rest due to generics. We test the
 * conversion to and from sortable types in the sortable files. */

func setupIndexInverted(t *testing.T) *inverted.IndexInverted[int64] {
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInverted[int64](b)
	in := make(chan inverted.IndexChange[int64])
	errC := inv.InsertUpdateDelete(context.Background(), in)
	for i, v := range items {
		in <- inverted.IndexChange[int64]{
			Id:          uint64(i),
			CurrentData: &v,
		}
	}
	close(in)
	require.NoError(t, <-errC)
	checkTermCount(t, b, 4)
	return inv
}

func Test_Insert(t *testing.T) {
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInverted[int64](b)
	in := make(chan inverted.IndexChange[int64])
	errC := inv.InsertUpdateDelete(context.Background(), in)
	for i, v := range items {
		in <- inverted.IndexChange[int64]{
			Id:          uint64(i),
			CurrentData: &v,
		}
	}
	close(in)
	require.NoError(t, <-errC)
	checkTermCount(t, b, 4)
}

func Test_Persistance(t *testing.T) {
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInverted[int64](b)
	in := make(chan inverted.IndexChange[int64])
	errC := inv.InsertUpdateDelete(context.Background(), in)
	for i, v := range items {
		in <- inverted.IndexChange[int64]{
			Id:          uint64(i),
			CurrentData: &v,
		}
	}
	close(in)
	require.NoError(t, <-errC)
	checkTermCount(t, b, 4)
	// ---------------------------
	// New inverted index
	inv2 := inverted.NewIndexInverted[int64](b)
	// Check if the terms are still there
	for k, rSet := range inverseMapping {
		res, err := inv2.Search(k, k, models.OperatorEquals)
		require.NoError(t, err)
		require.True(t, res.Equals(rSet))
	}
}

func Test_ConcurrentCUD(t *testing.T) {
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInverted[int64](b)
	in := make(chan inverted.IndexChange[int64])
	errC := inv.InsertUpdateDelete(context.Background(), in)
	// Pre-insert
	for i, v := range items {
		in <- inverted.IndexChange[int64]{
			Id:          uint64(i),
			CurrentData: &v,
		}
	}
	var wg sync.WaitGroup
	wg.Add(3)
	// ---------------------------
	// Insert more
	go func() {
		for i := 0; i < 10; i++ {
			data := int64(10)
			in <- inverted.IndexChange[int64]{
				Id:          uint64(i + 10),
				CurrentData: &data,
			}
		}
		wg.Done()
	}()
	// Update, 1 and 3 go to 11 and 13
	go func() {
		for i, v := range items {
			if v%2 == 0 {
				continue
			}
			newData := v + 10
			in <- inverted.IndexChange[int64]{
				Id:           uint64(i),
				PreviousData: &v,
				CurrentData:  &newData,
			}
		}
		wg.Done()
	}()
	// Delete, 2 and 4
	go func() {
		for i, v := range items {
			if v%2 != 0 {
				continue
			}
			in <- inverted.IndexChange[int64]{
				Id:           uint64(i),
				PreviousData: &v,
			}
		}
		wg.Done()
	}()
	wg.Wait()
	close(in)
	// ---------------------------
	require.NoError(t, <-errC)
	// We should have 11, 13, 10
	checkTermCount(t, b, 3)
	// ---------------------------
}

func Test_Search_Equals(t *testing.T) {
	inv := setupIndexInverted(t)
	// ---------------------------
	for k, rSet := range inverseMapping {
		res, err := inv.Search(k, k, models.OperatorEquals)
		require.NoError(t, err)
		require.True(t, res.Equals(rSet))
	}
}

func Test_Search_StartsWith(t *testing.T) {
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInverted[string](b)
	in := make(chan inverted.IndexChange[string])
	errC := inv.InsertUpdateDelete(context.Background(), in)
	for i, v := range []string{"foo", "food", "bar", "foob"} {
		in <- inverted.IndexChange[string]{
			Id:          uint64(i),
			CurrentData: &v,
		}
	}
	close(in)
	require.NoError(t, <-errC)
	checkTermCount(t, b, 4)
	// ----------------
	rSet, err := inv.Search("foo", "foo", models.OperatorStartsWith)
	require.NoError(t, err)
	require.True(t, rSet.Equals(roaring64.BitmapOf(0, 1, 3)))
}

func Test_Search(t *testing.T) {
	inv := setupIndexInverted(t)
	// ----------------
	tests := []struct {
		query              int64
		endQuery           int64
		operator           string
		expectedBitmapKeys []int64
	}{
		{query: 2, endQuery: 2, operator: models.OperatorEquals, expectedBitmapKeys: []int64{2}},
		{query: 2, endQuery: 2, operator: models.OperatorNotEquals, expectedBitmapKeys: []int64{1, 3, 4}},
		{query: 2, endQuery: 2, operator: models.OperatorGreaterThan, expectedBitmapKeys: []int64{3, 4}},
		{query: 2, endQuery: 2, operator: models.OperatorGreaterOrEq, expectedBitmapKeys: []int64{2, 3, 4}},
		{query: 2, endQuery: 2, operator: models.OperatorLessThan, expectedBitmapKeys: []int64{1}},
		{query: 2, endQuery: 2, operator: models.OperatorLessOrEq, expectedBitmapKeys: []int64{1, 2}},
		{query: 1, endQuery: 3, operator: models.OperatorInRange, expectedBitmapKeys: []int64{1, 2, 3}},
		{query: 42, endQuery: 42, operator: models.OperatorGreaterOrEq, expectedBitmapKeys: []int64{}},
	}
	for _, test := range tests {
		t.Run(test.operator, func(t *testing.T) {
			rSet, err := inv.Search(test.query, test.endQuery, test.operator)
			require.NoError(t, err)
			finalSet := roaring64.New()
			for _, k := range test.expectedBitmapKeys {
				finalSet.Or(inverseMapping[k])
			}
			require.True(t, rSet.Equals(finalSet))
		})
	}
}
