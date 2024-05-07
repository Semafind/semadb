package models_test

import (
	"testing"

	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
)

func TestIndexSchema_Validate_Empty(t *testing.T) {
	// These types require parameters to be provided
	iTypes := []string{models.IndexTypeString, models.IndexTypeStringArray,
		models.IndexTypeText, models.IndexTypeVectorFlat,
		models.IndexTypeVectorVamana}
	for _, iType := range iTypes {
		schema := models.IndexSchema{
			"prop": models.IndexSchemaValue{
				Type: iType,
				// We don't provide any parameters
			},
		}
		err := schema.Validate()
		// We should get an error
		require.Error(t, err)
	}
}

func TestIndexSchema_Validate_Haversine(t *testing.T) {
	// The haversine distance requires a vector size of 2
	iTypes := []string{models.IndexTypeVectorFlat, models.IndexTypeVectorVamana}
	for _, iType := range iTypes {
		schema := models.IndexSchema{
			"prop": models.IndexSchemaValue{
				Type: iType,
				VectorFlat: &models.IndexVectorFlatParameters{
					DistanceMetric: models.DistanceHaversine,
					VectorSize:     1,
				},
				VectorVamana: &models.IndexVectorVamanaParameters{
					DistanceMetric: models.DistanceHaversine,
					VectorSize:     1,
				},
			},
		}
		err := schema.Validate()
		require.Error(t, err)
	}
}

func TestIndexSchema_CheckCompatibleMap(t *testing.T) {
	// Check if the schema is compatible with a map
	// ---------------------------
	// Here is a kitchen sink schema
	schema := models.IndexSchema{
		"propVectorFlat": models.IndexSchemaValue{
			Type: models.IndexTypeVectorFlat,
			VectorFlat: &models.IndexVectorFlatParameters{
				DistanceMetric: models.DistanceEuclidean,
				VectorSize:     2,
			},
		},
		"propVectorVamana": models.IndexSchemaValue{
			Type: models.IndexTypeVectorVamana,
			VectorVamana: &models.IndexVectorVamanaParameters{
				DistanceMetric: models.DistanceEuclidean,
				VectorSize:     2,
			},
		},
		"propText": models.IndexSchemaValue{
			Type: models.IndexTypeText,
			Text: &models.IndexTextParameters{
				Analyser: "standard",
			},
		},
		"propString": models.IndexSchemaValue{
			Type: models.IndexTypeString,
			String: &models.IndexStringParameters{
				CaseSensitive: false,
			},
		},
		"propInteger": models.IndexSchemaValue{
			Type: models.IndexTypeInteger,
		},
		"propFloat": models.IndexSchemaValue{
			Type: models.IndexTypeFloat,
		},
		"propStringArray": models.IndexSchemaValue{
			Type: models.IndexTypeStringArray,
			StringArray: &models.IndexStringArrayParameters{
				IndexStringParameters: models.IndexStringParameters{
					CaseSensitive: false,
				},
			},
		},
	}
	// ---------------------------
	// Here is a matching map
	m := models.PointAsMap{
		"propVectorFlat":   []any{1.0, 2.0},
		"propVectorVamana": []any{1.0, 2.0},
		"propText":         "hello world",
		"propString":       "hello",
		"propInteger":      1,
		"propFloat":        1.1,
		"propStringArray":  []any{"hello", "world"},
	}
	err := schema.CheckCompatibleMap(m)
	require.NoError(t, err)
}
