package shard

import (
	"fmt"
	"testing"

	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
)

/*
fi := float32(i)
pointData := models.PointAsMap{
	"vector":      randVector,
	"flat":        []float32{fi, fi + 1},
	"description": fmt.Sprintf("This is a description %d", i),
	"category":    fmt.Sprintf("category %d", i),
	"labels":      []string{fmt.Sprintf("label1 %d", i), fmt.Sprintf("label2 %d", i+1)},
	"size":        i,
	"price":       fi + 0.5,
}
if rand.Float32() < 0.5 {
	pointData["extra"] = fmt.Sprintf("extra %d", i%5)
}
*/

func TestSearch_Select(t *testing.T) {
	// ---------------------------
	s := tempShard(t)
	points := randPoints(100)
	err := s.InsertPoints(points)
	require.NoError(t, err)
	// ---------------------------
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "size",
			Integer: &models.SearchIntegerOptions{
				Value:    10,
				Operator: models.OperatorLessOrEq,
			},
		},
		Select: []string{"size", "category", "nonExistent"},
	}
	res, err := s.SearchPoints(sr)
	require.NoError(t, err)
	require.Len(t, res, 11)
	for i := 0; i < 11; i++ {
		require.Nil(t, res[i].Data)
		require.Nil(t, res[i].Distance)
		require.Nil(t, res[i].Score)
		require.NotNil(t, res[i].DecodedData)
		require.Len(t, res[i].DecodedData, 2)
		require.Equal(t, int64(i), res[i].DecodedData["size"])
	}
}

func TestSearch_Sort(t *testing.T) {
	// ---------------------------
	s := tempShard(t)
	points := randPoints(100)
	err := s.InsertPoints(points)
	require.NoError(t, err)
	// ---------------------------
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "size",
			Integer: &models.SearchIntegerOptions{
				Value:    10,
				Operator: models.OperatorLessOrEq,
			},
		},
		Select: []string{"size"},
		Sort: []models.SortOption{
			{Property: "size", Descending: true},
		},
	}
	res, err := s.SearchPoints(sr)
	require.NoError(t, err)
	require.Len(t, res, 11)
	for i := 0; i < 11; i++ {
		require.Equal(t, int64(10-i), res[i].DecodedData["size"])
	}
}

func TestSearch_SortPartial(t *testing.T) {
	// ---------------------------
	s := tempShard(t)
	points := randPoints(100)
	err := s.InsertPoints(points)
	require.NoError(t, err)
	// ---------------------------
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "size",
			Integer: &models.SearchIntegerOptions{
				Value:    10,
				Operator: models.OperatorLessOrEq,
			},
		},
		Select: []string{"size", "extra"},
		Sort: []models.SortOption{
			{Property: "extra", Descending: true},
			{Property: "nonExistent", Descending: true},
			{Property: "size", Descending: true},
		},
	}
	res, err := s.SearchPoints(sr)
	require.NoError(t, err)
	require.Len(t, res, 11)
	/* We expect points "extra" property to come first and sorted in descending
	 * order, if they have the same extra value, we then sort by size descending
	 * order. If they don't have the extra property they are last and sorted by
	 * size. Something like but not always this because of random on extra field:
	 *
	 * map[extra:extra 4 size:9]
	 * map[extra:extra 3 size:8]
	 * map[extra:extra 3 size:3]
	 * map[extra:extra 2 size:2]
	 * map[extra:extra 1 size:6]
	 * map[extra:extra 0 size:10]
	 * map[extra:extra 0 size:5]
	 * map[size:7]
	 * map[size:4]
	 * map[size:1]
	 * map[size:0]
	 */
	for _, r := range res {
		fmt.Println(r.DecodedData)
	}
	for i := 0; i < len(res)-1; i++ {
		ax, aok := res[i].DecodedData["extra"]
		as := res[i].DecodedData["size"]
		bx, bok := res[i+1].DecodedData["extra"]
		bs := res[i+1].DecodedData["size"]
		if aok && bok {
			if ax == bx {
				require.GreaterOrEqual(t, as, bs)
			} else {
				require.Greater(t, ax, bx)
			}
		} else if !aok && !bok {
			require.Greater(t, as, bs)
		}
	}
}
