package distance

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/semafind/semadb/distance/asm"
	"github.com/stretchr/testify/assert"
)

var vectorTable = []struct {
	name          string
	x             []float32
	y             []float32
	wantDot       float32
	wantEuclidean float32
}{
	{"Zero", []float32{0, 0, 0}, []float32{0, 0, 0}, 0, 0},
	{"One", []float32{1, 1}, []float32{1, 1}, 2, 0},
	{"Two", []float32{1, 2, 3}, []float32{4, 5, 6}, 32, 27},
	{"Negative", []float32{-1, -2, -3}, []float32{-4, -5, -6}, 32, 27},
	{"Mixed", []float32{-1, 2, 3}, []float32{4, -5, 6}, 4, 83},
}

func TestPureDotProduct(t *testing.T) {
	for _, tt := range vectorTable {
		t.Run(tt.name, func(t *testing.T) {
			got := dotProductPureGo(tt.x, tt.y)
			assert.Equal(t, tt.wantDot, got)
		})
	}
}

func TestASMdotProduct(t *testing.T) {
	for _, tt := range vectorTable {
		t.Run(tt.name, func(t *testing.T) {
			got := asm.Dot(tt.x, tt.y)
			assert.Equal(t, tt.wantDot, got)
		})
	}
}

func TestPureSquaredEuclidean(t *testing.T) {
	for _, tt := range vectorTable {
		t.Run(tt.name, func(t *testing.T) {
			got := squaredEuclideanDistancePureGo(tt.x, tt.y)
			assert.Equal(t, tt.wantEuclidean, got)
		})
	}
}

func TestASMSquaredEuclidean(t *testing.T) {
	x := []float32{1, 2, 3}
	y := []float32{4, 5, 6}
	got := asm.SquaredEuclideanDistance(x, y)
	want := float32(27)
	assert.Equal(t, want, got)
}

var benchTable = []struct {
	name string
	fn   func([]float32, []float32) float32
}{
	{"PureDotProduct", dotProductPureGo},
	{"ASMDotProduct", asm.Dot},
	{"PureSquaredEuclidean", squaredEuclideanDistancePureGo},
	{"ASMSquaredEuclidean", asm.SquaredEuclideanDistance},
}

var bechSizes = []int{768, 1536}

func randVector(size int) []float32 {
	vector := make([]float32, size)
	for i := 0; i < size; i++ {
		vector[i] = rand.Float32()
	}
	return vector
}

func BenchmarkDist(b *testing.B) {
	for _, size := range bechSizes {
		for _, bench := range benchTable {
			x := randVector(size)
			y := randVector(size)
			runName := fmt.Sprintf("%s-%d", bench.name, size)
			b.Run(runName, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					bench.fn(x, y)
				}
			})
		}
	}
}
