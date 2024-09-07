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

func Test_AccessNestedProperty(t *testing.T) {
	data := map[string]any{
		"age":      2,
		"category": "A",
		"maybe":    5,
		"hasA":     "a",
		"nested": map[string]any{
			"size":  3,
			"maybe": 5,
			"again": map[string]any{
				"size": 7,
			},
		},
	}
	// Test cases
	tests := []struct {
		name string
		path string
		want any
		ok   bool
	}{
		{
			name: "exists",
			path: "age",
			want: 2,
			ok:   true,
		},
		{
			name: "does not exist",
			path: "gandalf",
			want: nil,
			ok:   false,
		},
		{
			name: "nested exists",
			path: "nested.size",
			want: 3,
			ok:   true,
		},
		{
			name: "nested doest not exist",
			path: "nested.gandalf",
			want: nil,
			ok:   false,
		},
		{
			name: "nested again",
			path: "nested.again.size",
			want: 7,
			ok:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := utils.AccessNestedProperty(data, tt.path)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.ok, ok)
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
				"nested": map[string]any{
					"size":  3,
					"maybe": 5,
				},
			},
		},
		{
			NodeId: 1,
			DecodedData: map[string]any{
				"age":      1,
				"category": "A",
				"maybe":    4,
				"hasB":     "b",
				"nested": map[string]any{
					"size":  6,
					"maybe": 4,
				},
			},
		},
		{
			NodeId: 3,
			DecodedData: map[string]any{
				"age":      3,
				"category": "B",
				"hasB":     "b",
				"nested": map[string]any{
					"size": 5,
				},
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
		{
			"nested property",
			[]models.SortOption{
				{
					Property:   "nested.size",
					Descending: true,
				},
			},
			[]uint64{1, 3, 2},
		},
		{
			"nested property missing",
			[]models.SortOption{
				{
					Property:   "nested.maybe",
					Descending: false,
				},
			},
			[]uint64{1, 2, 3},
		},
		{
			"non-existing nested property",
			[]models.SortOption{
				{
					Property:   "maybe.notHere",
					Descending: true,
				},
			},
			[]uint64{2, 1, 3},
		},
	}
	// ---------------------------
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results_cp := make([]models.SearchResult, len(results))
			copy(results_cp, results)
			utils.SortSearchResults(results_cp, tt.sortOpts)
			for i, r := range results_cp {
				require.Equal(t, tt.expected[i], r.NodeId)
			}
		})
	}
}
