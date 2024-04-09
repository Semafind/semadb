package shard

import (
	"testing"

	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
)

func Test_UpdateMerge(t *testing.T) {
	s := tempShard(t)
	pmaps := randPointsAsMap(10)
	points := pointsAsMapToPoints(pmaps)
	err := s.InsertPoints(points)
	require.NoError(t, err)
	// Update a point
	p := pmaps[0]
	p["size"] = int64(100)
	p["price"] = DELETEVALUE
	updatePoints := pointsAsMapToPoints(pmaps[:1])
	updatePoints[0].Id = points[0].Id
	// ---------------------------
	updatedIds, err := s.UpdatePoints(updatePoints)
	require.NoError(t, err)
	require.Len(t, updatedIds, 1)
	require.Equal(t, points[0].Id, updatedIds[0])
	// ---------------------------
	// Search for the point and see if it is updated
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "size",
			Integer: &models.SearchIntegerOptions{
				Value:    100,
				Operator: models.OperatorEquals,
			},
		},
		Select: []string{"size", "price"},
	}
	res, err := s.SearchPoints(sr)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, int64(100), res[0].DecodedData["size"])
	require.Len(t, res[0].DecodedData, 1)
}

func Test_UpdateExceedsUserPlan(t *testing.T) {
	s := tempShard(t)
	pmaps := randPointsAsMap(10)
	points := pointsAsMapToPoints(pmaps)
	err := s.InsertPoints(points)
	require.NoError(t, err)
	// Update a point
	p := pmaps[0]
	largeVector := make([]float32, 10000)
	p["vector"] = largeVector
	updatePoints := pointsAsMapToPoints(pmaps[:1])
	updatePoints[0].Id = points[0].Id
	// ---------------------------
	_, err = s.UpdatePoints(updatePoints)
	require.Error(t, err)
}
