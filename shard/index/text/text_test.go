package text_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/index/text"
	"github.com/stretchr/testify/require"
)

func checkDocCount(t *testing.T, b diskstore.Bucket, expected int) {
	t.Helper()
	var count int
	err := b.ForEach(func(k, v []byte) error {
		if k[0] == 'd' {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, expected, count)
}

func checkTermCount(t *testing.T, b diskstore.Bucket, expected int) {
	var count int
	rSets := make([]*roaring64.Bitmap, 0)
	err := b.ForEach(func(k, v []byte) error {
		if k[0] != 't' {
			return nil
		}
		count++
		rSet := roaring64.New()
		readSize, err := rSet.ReadFrom(bytes.NewReader(v))
		require.NoError(t, err)
		require.Equal(t, readSize, int64(len(v)))
		rSets = append(rSets, rSet)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, expected, count)
	combined := roaring64.FastOr(rSets...)
	checkDocCount(t, b, int(combined.GetCardinality()))
}

func Test_Insert(t *testing.T) {
	// ---------------------------
	b := diskstore.NewMemBucket(false)
	index, err := text.NewIndexText(b, models.IndexTextParameters{
		Analyser: "standard",
	})
	require.NoError(t, err)
	ctx := context.Background()
	out := make(chan text.Document)
	errC := index.InsertUpdateDelete(ctx, out)
	for i := 0; i < 100; i++ {
		out <- text.Document{
			Id:   uint64(i),
			Text: "hello world",
		}
	}
	close(out)
	require.NoError(t, <-errC)
	checkDocCount(t, b, 100)
	checkTermCount(t, b, 2)
}

func Benchmark_Insert(b *testing.B) {
	// ---------------------------
	bucket := diskstore.NewMemBucket(false)
	index, err := text.NewIndexText(bucket, models.IndexTextParameters{
		Analyser: "standard",
	})
	require.NoError(b, err)
	out := make(chan text.Document)
	errC := index.InsertUpdateDelete(context.Background(), out)
	// ---------------------------
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out <- text.Document{
			Id:   uint64(i),
			Text: "hello world " + fmt.Sprint(i),
		}
	}
	close(out)
	require.NoError(b, <-errC)
}

func Test_Search(t *testing.T) {
	// ---------------------------
	b := diskstore.NewMemBucket(false)
	index, err := text.NewIndexText(b, models.IndexTextParameters{
		Analyser: "standard",
	})
	// ---------------------------
	require.NoError(t, err)
	out := make(chan text.Document)
	errC := index.InsertUpdateDelete(context.Background(), out)
	for i := 0; i < 100; i++ {
		out <- text.Document{
			Id:   uint64(i),
			Text: "hello world " + fmt.Sprint(i),
		}
	}
	close(out)
	require.NoError(t, <-errC)
	checkDocCount(t, b, 100)
	checkTermCount(t, b, 102)
	// ---------------------------
	// containsAll
	so := models.SearchTextOptions{
		Value:    "hello world 42",
		Operator: models.OperatorContainsAll,
		Limit:    10,
	}
	results, err := index.Search(so)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, uint64(42), results[0].NodeId)
	// ---------------------------
	so.Operator = models.OperatorContainsAny
	results, err = index.Search(so)
	require.NoError(t, err)
	require.Len(t, results, 10)
	require.Equal(t, uint64(42), results[0].NodeId)
	require.Greater(t, *results[0].Score, *results[1].Score)
}

func Test_Persistance(t *testing.T) {
	// ---------------------------
	b := diskstore.NewMemBucket(false)
	index, err := text.NewIndexText(b, models.IndexTextParameters{
		Analyser: "standard",
	})
	require.NoError(t, err)
	out := make(chan text.Document)
	errC := index.InsertUpdateDelete(context.Background(), out)
	for i := 0; i < 10; i++ {
		out <- text.Document{
			Id:   uint64(i),
			Text: "hello world",
		}
	}
	close(out)
	require.NoError(t, <-errC)
	checkDocCount(t, b, 10)
	checkTermCount(t, b, 2)
	// ---------------------------
	// Can we create a new index and read from the bucket?
	index, err = text.NewIndexText(b, models.IndexTextParameters{
		Analyser: "standard",
	})
	require.NoError(t, err)
	so := models.SearchTextOptions{
		Value:    "hello world 42",
		Operator: models.OperatorContainsAny,
		Limit:    10,
	}
	results, err := index.Search(so)
	require.NoError(t, err)
	require.Len(t, results, 10)
}

func Test_CRUD(t *testing.T) {
	// ---------------------------
	b := diskstore.NewMemBucket(false)
	index, err := text.NewIndexText(b, models.IndexTextParameters{
		Analyser: "standard",
	})
	require.NoError(t, err)
	out := make(chan text.Document)
	errC := index.InsertUpdateDelete(context.Background(), out)
	// ---------------------------
	for i := 0; i < 100; i++ {
		// Insert
		out <- text.Document{
			Id:   uint64(i),
			Text: "hello world " + fmt.Sprint(i),
		}
	}
	// ---------------------------
	var wg sync.WaitGroup
	wg.Add(3)
	// Insert
	go func() {
		for i := 100; i < 150; i++ {
			// Insert
			out <- text.Document{
				Id:   uint64(i),
				Text: "hello world " + fmt.Sprint(i),
			}
		}
		wg.Done()
	}()
	// Update
	go func() {
		for i := 0; i < 50; i++ {
			// Insert
			out <- text.Document{
				Id:   uint64(i),
				Text: "foo bar " + fmt.Sprint(i),
			}
		}
		wg.Done()
	}()
	// Delete
	go func() {
		for i := 50; i < 100; i++ {
			// Insert
			out <- text.Document{
				Id:   uint64(i),
				Text: "",
			}
		}
		wg.Done()
	}()
	// ---------------------------
	wg.Wait()
	close(out)
	require.NoError(t, <-errC)
	// ---------------------------
	checkDocCount(t, b, 100)
	checkTermCount(t, b, 104)
}
