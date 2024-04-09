package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearch_Select(t *testing.T) {
	// ---------------------------
	s := tempShard(t)
	points := randPoints(100)
	err := s.InsertPoints(points)
	require.NoError(t, err)
	// ---------------------------
}
