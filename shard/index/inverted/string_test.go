package inverted_test

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/index/inverted"
	"github.com/stretchr/testify/require"
)

func Test_InvertedStringIndex(t *testing.T) {
	items := []string{"hello", "world", "HELLO", "world", "WORLD", "foo", "BAR", "bar", "bar"}
	tests := []struct {
		name            string
		caseSensitive   bool
		expectedMapping map[string][]uint64
	}{
		{
			name:          "case sensitive",
			caseSensitive: true,
			expectedMapping: map[string][]uint64{
				"hello": {0},
				"HELLO": {2},
				"world": {1, 3},
				"WORLD": {4},
				"foo":   {5},
				"BAR":   {6},
				"bar":   {7, 8},
			},
		},
		{
			name:          "case insensitive",
			caseSensitive: false,
			expectedMapping: map[string][]uint64{
				"hello": {0, 2},
				"world": {1, 3, 4},
				"foo":   {5},
				"bar":   {6, 7, 8},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := diskstore.NewMemBucket(false)
			inv := inverted.NewIndexInvertedString(b, models.IndexStringParameters{
				CaseSensitive: tt.caseSensitive,
			})
			in := make(chan inverted.IndexChange[string])
			errC := inv.InsertUpdateDelete(context.Background(), in)
			for i, v := range items {
				in <- inverted.IndexChange[string]{
					Id:          uint64(i),
					CurrentData: &v,
				}
			}
			close(in)
			require.NoError(t, <-errC)
			checkTermCount(t, b, len(tt.expectedMapping))
			// Check the mapping
			for term, expected := range tt.expectedMapping {
				res, err := inv.Search(models.SearchStringOptions{
					Value:    term,
					Operator: models.OperatorEquals,
				})
				require.NoError(t, err)
				require.True(t, res.Equals(roaring64.BitmapOf(expected...)))
			}
		})
	}
}

func Test_InvertedArrayStringIndex(t *testing.T) {
	tests := []struct {
		name           string
		caseSensitive  bool
		query          []string
		operator       string
		expectedDocIds []uint64
	}{
		{
			name:           "case sensitive all",
			caseSensitive:  true,
			query:          []string{"hello", "world"},
			operator:       models.OperatorContainsAll,
			expectedDocIds: []uint64{0},
		},
		{
			name:           "case insensitive all",
			caseSensitive:  false,
			query:          []string{"HELLO", "world"},
			operator:       models.OperatorContainsAll,
			expectedDocIds: []uint64{0, 1},
		},
		{
			name:           "case insensitive any",
			caseSensitive:  false,
			query:          []string{"hello", "BAR"},
			operator:       models.OperatorContainsAny,
			expectedDocIds: []uint64{0, 1, 3, 4},
		},
		{
			name:           "case sensitive any",
			caseSensitive:  true,
			query:          []string{"hello", "bar"},
			operator:       models.OperatorContainsAny,
			expectedDocIds: []uint64{0, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := [][]string{
				{"hello", "world"},
				{"HELLO", "world"},
				{"world", "WORLD"},
				{"foo", "BAR"},
				{"bar", "bar"},
			}
			b := diskstore.NewMemBucket(false)
			inv := inverted.NewIndexInvertedArrayString(b, models.IndexStringArrayParameters{
				IndexStringParameters: models.IndexStringParameters{
					CaseSensitive: tt.caseSensitive,
				},
			})
			in := make(chan inverted.IndexArrayChange[string])
			errC := inv.InsertUpdateDelete(context.Background(), in)
			for i, v := range items {
				in <- inverted.IndexArrayChange[string]{
					Id:          uint64(i),
					CurrentData: v,
				}
			}
			close(in)
			require.NoError(t, <-errC)
			if tt.caseSensitive {
				checkTermCount(t, b, 7)
			} else {
				checkTermCount(t, b, 4)
			}
			// Check the mapping
			res, err := inv.Search(models.SearchStringArrayOptions{
				Value:    tt.query,
				Operator: tt.operator,
			})
			require.NoError(t, err)
			require.True(t, res.Equals(roaring64.BitmapOf(tt.expectedDocIds...)))
		})
	}

}
