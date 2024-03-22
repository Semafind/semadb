package inverted_test

import (
	"context"
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/index/inverted"
	"github.com/stretchr/testify/require"
)

func checkTermCount(t *testing.T, b diskstore.Bucket, expected int) {
	var count int
	err := b.ForEach(func(k, v []byte) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, expected, count)
}

func Test_Integer(t *testing.T) {
	// Test cases
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInverted[int64](b)
	ctx := context.Background()
	in := make(chan inverted.IndexChange[int64])
	go func() {
		for i := 0; i < 100; i++ {
			data := int64(i) % 5
			in <- inverted.IndexChange[int64]{
				Id:          uint64(i),
				CurrentData: &data,
			}
		}
		close(in)
	}()
	errC := inv.InsertUpdateDelete(ctx, in)
	require.NoError(t, <-errC)
	checkTermCount(t, b, 5)
}

func Test_BasicIntegerSearch(t *testing.T) {
	// Test cases
	b := diskstore.NewMemBucket(false)
	inv := inverted.NewIndexInverted[int64](b)
	ctx := context.Background()
	in := make(chan inverted.IndexChange[int64])
	go func() {
		for i := 0; i < 20; i++ {
			data := int64(i) % 17
			in <- inverted.IndexChange[int64]{
				Id:          uint64(i),
				CurrentData: &data,
			}
		}
		close(in)
	}()
	errC := inv.InsertUpdateDelete(ctx, in)
	require.NoError(t, <-errC)
	checkTermCount(t, b, 17)
	// ---------------------------
	rSet, err := inv.Search(0, 0, models.OperatorEquals)
	require.NoError(t, err)
	require.EqualValues(t, 2, rSet.GetCardinality())
	iterator := rSet.Iterator()
	require.Equal(t, iterator.Next(), uint64(0))
	require.Equal(t, iterator.Next(), uint64(17))
}

// TODO: Test Insert, Update, Delete
// TODO: Test Search operators
