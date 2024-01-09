package shard

import (
	"fmt"
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
	fns := []struct {
		name      string
		forwardFn func([]float32) []byte
		reverseFn func([]byte) []float32
	}{
		{"safe", float32ToBytesSafe, bytesToFloat32Safe},
		{"raw", float32ToBytesRaw, bytesToFloat32Raw},
	}
	vecSizes := []int{128, 768, 960, 1536}
	// ---------------------------
	for _, vecSize := range vecSizes {
		randFloats := make([]float32, vecSize)
		for j := 0; j < vecSize; j++ {
			randFloats[j] = rand.Float32()
		}
		for _, fn := range fns {
			t.Run(fmt.Sprintf("%s/%d", fn.name, vecSize), func(t *testing.T) {
				bytesSlice := fn.forwardFn(randFloats[:])
				require.Equal(t, randFloats, fn.reverseFn(bytesSlice))
			})
		}
	}
}

func Benchmark_float32ToBytes(b *testing.B) {
	// ---------------------------
	fns := []struct {
		name      string
		forwardFn func([]float32) []byte
		reverseFn func([]byte) []float32
	}{
		{"safe", float32ToBytesSafe, bytesToFloat32Safe},
		{"raw", float32ToBytesRaw, bytesToFloat32Raw},
	}
	vecSizes := []int{128, 768, 960, 1536}
	// ---------------------------
	for _, vecSize := range vecSizes {
		randFloats := make([]float32, vecSize)
		for j := 0; j < vecSize; j++ {
			randFloats[j] = rand.Float32()
		}
		for _, fn := range fns {
			b.Run(fmt.Sprintf("%s/%d", fn.name, vecSize), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					bytesSlice := fn.forwardFn(randFloats[:])
					fn.reverseFn(bytesSlice)
				}
			})
		}
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
