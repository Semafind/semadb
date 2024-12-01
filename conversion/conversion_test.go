package conversion

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Uint64ToBytes(t *testing.T) {
	for i := 0; i < 10; i++ {
		randInt := rand.Uint64()
		b := Uint64ToBytes(randInt)
		require.Equal(t, randInt, BytesToUint64(b))
	}
}

func Test_SingleFloat32ToBytes(t *testing.T) {
	for i := 0; i < 10; i++ {
		randFloat := rand.Float32()
		b := SingleFloat32ToBytes(randFloat)
		require.Equal(t, randFloat, BytesToSingleFloat32(b))
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

func Test_EdgeListToBytes(t *testing.T) {
	for i := 0; i < 10; i++ {
		randSize := rand.IntN(10)
		randEdges := make([]uint64, randSize)
		for j := 0; j < randSize; j++ {
			randEdges[j] = rand.Uint64()
		}
		b := EdgeListToBytes(randEdges)
		require.Equal(t, randEdges, BytesToEdgeList(b))
	}
}
