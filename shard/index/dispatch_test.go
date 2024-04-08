package index_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index"
	"github.com/semafind/semadb/utils"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

var sampleIndexSchema = models.IndexSchema{
	"vector": models.IndexSchemaValue{
		Type: models.IndexTypeVectorVamana,
		VectorVamana: &models.IndexVectorVamanaParameters{
			VectorSize:     2,
			DistanceMetric: "euclidean",
			SearchSize:     75,
			DegreeBound:    64,
			Alpha:          1.2,
		},
	},
	"flat": models.IndexSchemaValue{
		Type: models.IndexTypeVectorFlat,
		VectorFlat: &models.IndexVectorFlatParameters{
			VectorSize:     2,
			DistanceMetric: "euclidean",
		},
	},
	"description": models.IndexSchemaValue{
		Type: models.IndexTypeText,
		Text: &models.IndexTextParameters{
			Analyser: "standard",
		},
	},
	"category": models.IndexSchemaValue{
		Type: models.IndexTypeString,
		String: &models.IndexStringParameters{
			CaseSensitive: false,
		},
	},
	"labels": models.IndexSchemaValue{
		Type: models.IndexTypeStringArray,
		StringArray: &models.IndexStringArrayParameters{
			IndexStringParameters: models.IndexStringParameters{
				CaseSensitive: false,
			},
		},
	},
	"size": models.IndexSchemaValue{
		Type: models.IndexTypeInteger,
	},
	"price": models.IndexSchemaValue{
		Type: models.IndexTypeFloat,
	},
	"nonExistent": models.IndexSchemaValue{
		Type: models.IndexTypeInteger,
	},
}

func randPoints(size int, idOffset int) []index.IndexPointChange {
	points := make([]index.IndexPointChange, size)
	for i := 0; i < size; i++ {
		ii := i + idOffset + 2 // 0 and 1 are reserved for graph indices such as start node
		fi := float32(i + idOffset)
		point := models.PointAsMap{
			"vector":      []float32{fi, fi + 1},
			"flat":        []float32{fi, fi},
			"description": fmt.Sprintf("This is a description %d", ii),
			"category":    fmt.Sprintf("category %d", ii),
			"labels":      []string{fmt.Sprintf("label1 %d", ii), fmt.Sprintf("label2 %d", ii)},
			"size":        ii,
			"price":       fi + 0.5,
			"extra":       fmt.Sprintf("extra %d", ii),
		}
		sampleIndexSchema.CheckCompatibleMap(point)
		pointBytes, _ := msgpack.Marshal(point)
		points[i] = index.IndexPointChange{
			NodeId:  uint64(ii),
			NewData: pointBytes,
		}
	}
	return points
}

func TestDispatch_Insert(t *testing.T) {
	store, _ := diskstore.Open("")
	cacheM := cache.NewManager(-1)
	cacheTx := cacheM.NewTransaction()
	ctx := context.Background()
	// ---------------------------
	err := store.Write(func(bm diskstore.BucketManager) error {
		indexManager := index.NewIndexManager(bm, cacheTx, "cache", sampleIndexSchema)
		// ---------------------------
		points := randPoints(100, 0)
		in := utils.ProduceWithContext(ctx, points)
		errC := indexManager.Dispatch(ctx, in)
		require.NoError(t, <-errC)
		// ---------------------------
		return nil
	})
	require.NoError(t, err)
	cacheTx.Commit(false)
	// Are the points in the store?
	err = store.Read(func(bm diskstore.BucketManager) error {
		for propName, params := range sampleIndexSchema {
			bucketName := fmt.Sprintf("index/%s/%s", params.Type, propName)
			bucket, err := bm.Get(bucketName)
			require.NoError(t, err)
			count := 0
			bucket.ForEach(func(k, v []byte) error {
				count++
				return nil
			})
			expected := 100
			if propName == "nonExistent" {
				expected = 0
			}
			require.GreaterOrEqual(t, count, expected, "expected 100 for %s", bucketName)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestDispatch_Delete(t *testing.T) {
	store, _ := diskstore.Open("")
	cacheM := cache.NewManager(-1)
	cacheTx := cacheM.NewTransaction()
	ctx := context.Background()
	// ---------------------------
	err := store.Write(func(bm diskstore.BucketManager) error {
		indexManager := index.NewIndexManager(bm, cacheTx, "cache", sampleIndexSchema)
		// ---------------------------
		points := randPoints(100, 0)
		in := utils.ProduceWithContext(ctx, points)
		errC := indexManager.Dispatch(ctx, in)
		require.NoError(t, <-errC)
		// ---------------------------
		for i := range points {
			points[i].PreviousData = points[i].NewData
			points[i].NewData = nil
		}
		in = utils.ProduceWithContext(ctx, points)
		errC = indexManager.Dispatch(ctx, in)
		require.NoError(t, <-errC)
		// ---------------------------
		return nil
	})
	require.NoError(t, err)
	cacheTx.Commit(false)
	// Did we clean up the points?
	err = store.Read(func(bm diskstore.BucketManager) error {
		for propName, params := range sampleIndexSchema {
			bucketName := fmt.Sprintf("index/%s/%s", params.Type, propName)
			bucket, err := bm.Get(bucketName)
			require.NoError(t, err)
			count := 0
			bucket.ForEach(func(k, v []byte) error {
				count++
				return nil
			})
			expected := 0
			switch params.Type {
			case models.IndexTypeVectorVamana:
				// These are max node id, start node vector and edges
				expected = 3
			case models.IndexTypeText:
				// The number of documents is left behind
				expected = 1
			}
			require.Equal(t, expected, count, "expected 0 for %s", bucketName)
		}
		return nil
	})
	require.NoError(t, err)
}
