package index_test

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index"
	"github.com/semafind/semadb/utils"
	"github.com/stretchr/testify/require"
)

/*
What the random point with index i looks like:

ii := i + idOffset + 2 // 0 and 1 are reserved for graph indices such as start node
fi := float32(ii)
point := models.PointAsMap{
	"vector":      []float32{fi, fi + 1},
	"flat":        []float32{fi, fi + 1},
	"description": fmt.Sprintf("This is a description %d", ii),
	"category":    fmt.Sprintf("category %d", ii),
	"labels":      []string{fmt.Sprintf("label1 %d", ii), fmt.Sprintf("label2 %d", ii+1)},
	"size":        ii,
	"price":       fi + 0.5,
	"extra":       fmt.Sprintf("extra %d", ii),
}
*/

func populateIndex(t *testing.T, ds diskstore.DiskStore, cacheM *cache.Manager) {
	t.Helper()
	ctx := context.Background()
	cacheTx := cacheM.NewTransaction()
	// ---------------------------
	err := ds.Write(func(bm diskstore.BucketManager) error {
		indexManager := index.NewIndexManager(bm, cacheTx, "cache", sampleIndexSchema)
		// ---------------------------
		points := randPoints(100, 0)
		in := utils.ProduceWithContext(ctx, points)
		errC := indexManager.Dispatch(ctx, in)
		// ---------------------------
		return <-errC
	})
	require.NoError(t, err)
	cacheTx.Commit(false)
}

func performSearch(t *testing.T, ds diskstore.DiskStore, cacheM *cache.Manager, req models.Query) (*roaring64.Bitmap, []models.SearchResult) {
	t.Helper()
	var rSet *roaring64.Bitmap
	var results []models.SearchResult
	err := ds.Read(func(bm diskstore.BucketManager) error {
		im := index.NewIndexManager(bm, cacheM.NewTransaction(), "cache", sampleIndexSchema)
		var err error
		rSet, results, err = im.Search(context.Background(), req)
		return err
	})
	require.NoError(t, err)
	return rSet, results
}

func TestSearch_Single(t *testing.T) {
	store, _ := diskstore.Open("")
	cacheM := cache.NewManager(-1)
	populateIndex(t, store, cacheM)
	// Are the points in the store?
	for propName := range sampleIndexSchema {
		q := models.Query{
			Property: propName,
			Integer: &models.SearchIntegerOptions{
				Value:    42,
				Operator: models.OperatorEquals,
			},
			Float: &models.SearchFloatOptions{
				Value:    42.5,
				Operator: models.OperatorEquals,
			},
			String: &models.SearchStringOptions{
				Value:    "category 42",
				Operator: models.OperatorEquals,
			},
			VectorFlat: &models.SearchVectorFlatOptions{
				Vector: []float32{42, 43},
				Limit:  10,
			},
			VectorVamana: &models.SearchVectorVamanaOptions{
				Vector: []float32{42, 43},
				Limit:  10,
			},
			Text: &models.SearchTextOptions{
				Value:    "description 42",
				Operator: models.OperatorContainsAny,
				Limit:    10,
			},
			StringArray: &models.SearchStringArrayOptions{
				Value:    []string{"label1 42", "label2 43"},
				Operator: models.OperatorContainsAll,
			},
		}
		rSet, results := performSearch(t, store, cacheM, q)
		if propName == "nonExistent" {
			require.EqualValues(t, 0, rSet.GetCardinality())
			require.Equal(t, 0, len(results))
			continue
		}
		require.True(t, rSet.Contains(42), "expected 42 in %s", propName)
		if propName == "vector" || propName == "flat" || propName == "description" {
			require.EqualValues(t, 10, rSet.GetCardinality())
			require.Equal(t, 10, len(results))
			require.EqualValues(t, 42, results[0].NodeId, "got %d for %s", results[0].NodeId, propName)
		} else {
			require.EqualValues(t, 1, rSet.GetCardinality())
			require.Equal(t, 0, len(results), "got results for %s", propName)
		}
	}
}
