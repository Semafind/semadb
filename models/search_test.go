package models_test

import (
	"testing"

	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
)

func TestSearch_QueryValidate(t *testing.T) {
	// ---------------------------
	tests := []struct {
		name  string
		query models.Query
		fail  bool
	}{
		{
			name: "_id String Non-UUID",
			query: models.Query{
				Property: "_id",
				String: &models.SearchStringOptions{
					Operator: models.OperatorEquals,
				},
			},
			fail: true,
		},
		{
			name: "_id String Valid",
			query: models.Query{
				Property: "_id",
				String: &models.SearchStringOptions{
					Operator: models.OperatorEquals,
					Value:    "123e4567-e89b-12d3-a456-426614174000",
				},
			},
			fail: false,
		},
		{
			name: "_id StringArray Non-UUID",
			query: models.Query{
				Property: "_id",
				StringArray: &models.SearchStringArrayOptions{
					Operator: models.OperatorContainsAny,
					Value:    []string{"123e4567-e89b-12d3-a456-426614174000", "gandalf"},
				},
			},
			fail: true,
		},
		{
			name: "_id StringArray Non-UUID",
			query: models.Query{
				Property: "_id",
				StringArray: &models.SearchStringArrayOptions{
					Operator: models.OperatorContainsAny,
					Value:    []string{"123e4567-e89b-12d3-a456-426614174000"},
				},
			},
			fail: false,
		},
		{
			name: "Invalid vector vamana length",
			query: models.Query{
				Property: "propVectorVamana",
				VectorVamana: &models.SearchVectorVamanaOptions{
					Vector: []float32{1.0},
				},
			},
			fail: true,
		},
		{
			name: "Invalid Vector Vamana filter",
			query: models.Query{
				Property: "propVectorVamana",
				VectorVamana: &models.SearchVectorVamanaOptions{
					Vector:     []float32{1.0, 2.0},
					SearchSize: 10,
					Limit:      10,
					Filter: &models.Query{
						Property: "propInteger",
						Float: &models.SearchFloatOptions{
							Operator: models.OperatorEquals,
							Value:    1.0,
						},
					},
				},
			},
			fail: true,
		},
		{
			name: "Invalid flat vector filter",
			query: models.Query{
				Property: "propVectorFlat",
				VectorFlat: &models.SearchVectorFlatOptions{
					Vector: []float32{1.0, 2.0},
					Filter: &models.Query{
						Property: "propString",
						Float: &models.SearchFloatOptions{
							Operator: models.OperatorEquals,
							Value:    1.0,
						},
					},
				},
			},
			fail: true,
		},
		{
			name: "Invalid text filter",
			query: models.Query{
				Property: "propText",
				Text: &models.SearchTextOptions{
					Value: "text",
					Filter: &models.Query{
						Property: "propString",
						Float: &models.SearchFloatOptions{
							Operator: models.OperatorEquals,
							Value:    1.0,
						},
					},
				},
			},
			fail: true,
		},
		{
			name: "Valid composite query",
			query: models.Query{
				Property: "_and",
				And: []models.Query{
					{
						Property: "propString",
						String: &models.SearchStringOptions{
							Operator: models.OperatorEquals,
							Value:    "string",
						},
					},
					{
						Property: "_or",
						Or: []models.Query{
							{
								Property: "propFloat",
								Float: &models.SearchFloatOptions{
									Operator: models.OperatorEquals,
									Value:    1.0,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Non-existent property",
			query: models.Query{
				Property: "nonExistent",
			},
			fail: true,
		},
	}
	// ---------------------------
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.query.Validate(sampleSchema)
			if tt.fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
