package utils_test

import (
	"testing"

	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/utils"
	"github.com/stretchr/testify/require"
)

func Test_CompareAny(t *testing.T) {
	// Test cases
	tests := []struct {
		name string
		a    any
		b    any
		want int
	}{
		{
			name: "int32",
			a:    int32(1),
			b:    int32(1),
			want: 0,
		},
		{
			name: "uint32",
			a:    uint32(1),
			b:    uint32(1),
			want: 0,
		},
		{
			name: "float32",
			a:    float32(1),
			b:    float32(1),
			want: 0,
		},
		{
			name: "string",
			a:    "a",
			b:    "a",
			want: 0,
		},
		{
			name: "different types",
			a:    "a",
			b:    1,
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := utils.CompareAny(tt.a, tt.b); got != tt.want {
				t.Errorf("CompareAny() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_SortSearchResults(t *testing.T) {
	results := []models.SearchResult{
		{
			NodeId: 2,
			DecodedData: map[string]any{
				"age":      2,
				"category": "A",
				"maybe":    5,
				"hasA":     "a",
			},
		},
		{
			NodeId: 1,
			DecodedData: map[string]any{
				"age":      1,
				"category": "A",
				"maybe":    4,
				"hasB":     "b",
			},
		},
		{
			NodeId: 3,
			DecodedData: map[string]any{
				"age":      3,
				"category": "B",
				"hasB":     "b",
			},
		},
	}
	tests := []struct {
		name     string
		sortOpts []models.SortOption
		expected []uint64
	}{
		{
			"sort by age",
			[]models.SortOption{
				{
					Property:   "age",
					Descending: true,
				},
			},
			[]uint64{3, 2, 1},
		},
		{
			"2 value sort",
			[]models.SortOption{
				{
					Property:   "category",
					Descending: false,
				},
				{
					Property:   "age",
					Descending: true,
				},
			},
			[]uint64{2, 1, 3},
		},
		{
			"missing value",
			[]models.SortOption{
				{
					Property:   "maybe",
					Descending: true,
				},
			},
			[]uint64{2, 1, 3},
		},
		{
			"non existing property",
			[]models.SortOption{
				{
					Property:   "gandalf",
					Descending: true,
				},
				{
					Property:   "age",
					Descending: true,
				},
			},
			[]uint64{3, 2, 1},
		},
	}
	// ---------------------------
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			utils.SortSearchResults(results, tt.sortOpts)
			for i, r := range results {
				require.Equal(t, tt.expected[i], r.NodeId)
			}
		})
	}
}
