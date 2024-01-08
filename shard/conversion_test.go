package shard

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_uint64ToBytes(t *testing.T) {
	for i := 0; i < 10; i++ {
		randInt := rand.Uint64()
		b := uint64ToBytes(randInt)
		require.Equal(t, randInt, bytesToUint64(b))
	}
}

func Test_float32ToBytes(t *testing.T) {
	for i := 0; i < 10; i++ {
		randFloat1 := rand.Float32()
		randFloat2 := rand.Float32()
		b := float32ToBytes([]float32{randFloat1, randFloat2})
		require.Equal(t, randFloat1, bytesToFloat32(b)[0])
		require.Equal(t, randFloat2, bytesToFloat32(b)[1])
	}
}

func Test_edgeListToBytes(t *testing.T) {
	for i := 0; i < 10; i++ {
		randSize := rand.Intn(10)
		randEdges := make([]uint64, randSize)
		for j := 0; j < randSize; j++ {
			randEdges[j] = rand.Uint64()
		}
		b := edgeListToBytes(randEdges)
		require.Equal(t, randEdges, bytesToEdgeList(b))
	}
}
