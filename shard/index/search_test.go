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

func TestSearch_NonIndexField(t *testing.T) {
	store, _ := diskstore.Open("")
	cacheM := cache.NewManager(-1)
	populateIndex(t, store, cacheM)
	// Search for a non-indexed field
	q := models.Query{
		Property: "randomField9000",
		String: &models.SearchStringOptions{
			Value:    "extra 42",
			Operator: models.OperatorEquals,
		},
	}
	err := store.Read(func(bm diskstore.BucketManager) error {
		im := index.NewIndexManager(bm, cacheM.NewTransaction(), "cache", sampleIndexSchema)
		var err error
		_, _, err = im.Search(context.Background(), q)
		require.Error(t, err)
		return err
	})
	require.Error(t, err)
}

func TestSearch_Single(t *testing.T) {
	store, _ := diskstore.Open("")
	cacheM := cache.NewManager(-1)
	populateIndex(t, store, cacheM)
	// Search for each property
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
				Vector:     []float32{42, 43},
				SearchSize: 75,
				Limit:      10,
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

func TestSearch_FilterSpecific(t *testing.T) {
	store, _ := diskstore.Open("")
	cacheM := cache.NewManager(-1)
	populateIndex(t, store, cacheM)
	// ---------------------------
	filterQ := models.Query{
		Property: "size",
		Integer: &models.SearchIntegerOptions{
			Value:    42,
			Operator: models.OperatorInRange,
			EndValue: 46,
		},
	}
	// ---------------------------
	for _, propName := range []string{"vector", "flat", "description"} {
		q := models.Query{
			Property: propName,
			VectorVamana: &models.SearchVectorVamanaOptions{
				Vector:     []float32{42, 43},
				SearchSize: 75,
				Limit:      10,
				Filter:     &filterQ,
			},
			VectorFlat: &models.SearchVectorFlatOptions{
				Vector: []float32{42, 43},
				Limit:  10,
				Filter: &filterQ,
			},
			Text: &models.SearchTextOptions{
				Value:    "description 42",
				Operator: models.OperatorContainsAny,
				Limit:    10,
				Filter:   &filterQ,
			},
		}
		rSet, results := performSearch(t, store, cacheM, q)
		expectedSet := roaring64.BitmapOf(42, 43, 44, 45, 46)
		require.True(t, rSet.Equals(expectedSet), "expected %s", expectedSet.String())
		require.Len(t, results, 5)
		require.Equal(t, uint64(42), results[0].NodeId)
	}
	// ---------------------------
}

func TestSearch_And(t *testing.T) {
	store, _ := diskstore.Open("")
	cacheM := cache.NewManager(-1)
	populateIndex(t, store, cacheM)
	// ---------------------------
	q := models.Query{
		Property: "_and",
		// These two queries create an overlap
		And: []models.Query{
			{
				Property: "description",
				Text: &models.SearchTextOptions{
					Value:    "description 42",
					Operator: models.OperatorContainsAny,
					Limit:    10,
				},
			},
			{
				Property: "description",
				Text: &models.SearchTextOptions{
					Value:    "description 43",
					Operator: models.OperatorContainsAny,
					Limit:    10,
				},
			},
		},
	}
	// ---------------------------
	rSet, results := performSearch(t, store, cacheM, q)
	expectedSet := roaring64.BitmapOf(2, 3, 4, 5, 6, 7, 8, 9, 10)
	require.True(t, rSet.Equals(expectedSet), "expected %s", expectedSet.String())
	require.Len(t, results, 9)
	// Check sorting on the results
	for i := 0; i < len(results)-1; i++ {
		require.True(t, expectedSet.Contains(results[i].NodeId))
		require.GreaterOrEqual(t, results[i].HybridScore, results[i+1].HybridScore)
	}
}

func TestSearch_Or(t *testing.T) {
	store, _ := diskstore.Open("")
	cacheM := cache.NewManager(-1)
	populateIndex(t, store, cacheM)
	// ---------------------------
	q := models.Query{
		Property: "_or",
		// These two queries create an overlap
		Or: []models.Query{
			{
				Property: "description",
				Text: &models.SearchTextOptions{
					Value:    "description 42",
					Operator: models.OperatorContainsAny,
					Limit:    10,
				},
			},
			{
				Property: "description",
				Text: &models.SearchTextOptions{
					Value:    "description 43",
					Operator: models.OperatorContainsAny,
					Limit:    10,
				},
			},
		},
	}
	// ---------------------------
	rSet, results := performSearch(t, store, cacheM, q)
	expectedSet := roaring64.BitmapOf(2, 3, 4, 5, 6, 7, 8, 9, 10, 42, 43)
	require.True(t, rSet.Equals(expectedSet), "expected %s", expectedSet.String())
	require.Len(t, results, 11)
	// Check sorting on the results
	for i := 0; i < len(results)-1; i++ {
		require.True(t, expectedSet.Contains(results[i].NodeId))
		require.GreaterOrEqual(t, results[i].HybridScore, results[i+1].HybridScore)
	}
	require.Equal(t, uint64(42), results[0].NodeId)
	require.Equal(t, uint64(43), results[1].NodeId)
}
