package text_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/index/text"
	"github.com/stretchr/testify/require"
)

func checkDocCount(t *testing.T, b diskstore.Bucket, expected int) {
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
}

func Test_Search(t *testing.T) {
	// ---------------------------
	b := diskstore.NewMemBucket(false)
	index, err := text.NewIndexText(b, models.IndexTextParameters{
		Analyser: "standard",
	})
	// ---------------------------
	require.NoError(t, err)
	ctx := context.Background()
	out := make(chan text.Document)
	errC := index.InsertUpdateDelete(ctx, out)
	for i := 0; i < 100; i++ {
		out <- text.Document{
			Id:   uint64(i),
			Text: "hello world " + fmt.Sprint(i),
		}
	}
	close(out)
	require.NoError(t, <-errC)
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
