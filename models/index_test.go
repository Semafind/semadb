package models_test

import (
	"encoding/json"
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

// ---------------------------
// Here is a kitchen sink schema
var sampleSchema models.IndexSchema = models.IndexSchema{
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
	"nested.propInteger": models.IndexSchemaValue{
		Type: models.IndexTypeInteger,
	},
}

// ---------------------------

func TestIndexSchema_CheckCompatibleMap(t *testing.T) {
	// Check if the schema is compatible with a map
	// ---------------------------
	tests := []struct {
		name       string
		jsonString string
		fail       bool
	}{
		{
			name:       "Valid Vector Flat",
			jsonString: `{"propVectorFlat": [1.0, 2.0]}`,
			fail:       false,
		},
		{
			name:       "Invalid Size Vector Flat",
			jsonString: `{"propVectorFlat": [1.0]}`,
			fail:       true,
		},
		{
			name:       "Invalid Type Vector Flat",
			jsonString: `{"propVectorFlat": "string"}`,
			fail:       true,
		},
		{
			name:       "Valid Vector Vamana",
			jsonString: `{"propVectorVamana": [1.0, 2.0]}`,
			fail:       false,
		},
		{
			name:       "Invalid Size Vector Vamana",
			jsonString: `{"propVectorVamana": [1.0]}`,
			fail:       true,
		},
		{
			name:       "Invalid Type Vector Vamana",
			jsonString: `{"propVectorVamana": "string"}`,
			fail:       true,
		},
		{
			name:       "Valid Text",
			jsonString: `{"propText": "text"}`,
			fail:       false,
		},
		{
			name:       "Invalid Type Text",
			jsonString: `{"propText": 1}`,
			fail:       true,
		},
		{
			name:       "Valid String",
			jsonString: `{"propString": "string"}`,
			fail:       false,
		},
		{
			name:       "Invalid Type String",
			jsonString: `{"propString": 1}`,
			fail:       true,
		},
		{
			name:       "Valid Integer",
			jsonString: `{"propInteger": 1}`,
			fail:       false,
		},
		{
			name:       "Invalid Type Integer",
			jsonString: `{"propInteger": "string"}`,
			fail:       true,
		},
		{
			name:       "Valid Float",
			jsonString: `{"propFloat": 1.0}`,
			fail:       false,
		},
		{
			name:       "Invalid Type Float",
			jsonString: `{"propFloat": "string"}`,
			fail:       true,
		},
		{
			name:       "Valid String Array",
			jsonString: `{"propStringArray": ["string1", "string2"]}`,
			fail:       false,
		},
		{
			name:       "Invalid Type String Array",
			jsonString: `{"propStringArray": "string"}`,
			fail:       true,
		},
		{
			name:       "Valid Nested Integer",
			jsonString: `{"nested": {"propInteger": 1}}`,
			fail:       false,
		},
		{
			name:       "Invalid Nested Type Integer",
			jsonString: `{"nested": {"propInteger": "string"}}`,
			fail:       true,
		},
		{
			name:       "Invalid Nested Type",
			jsonString: `{"nested": 42}`,
			fail:       true,
		},
	}
	// ---------------------------
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m models.PointAsMap
			err := json.Unmarshal([]byte(tt.jsonString), &m)
			require.NoError(t, err)
			err = sampleSchema.CheckCompatibleMap(m)
			if tt.fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
	// ---------------------------
}
