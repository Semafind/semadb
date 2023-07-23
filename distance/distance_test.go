package distance

import (
	"testing"

	"github.com/semafind/semadb/distance/asm"
	"github.com/stretchr/testify/assert"
)

func TestPureDotProduct(t *testing.T) {
	x := []float32{1, 2, 3}
	y := []float32{4, 5, 6}
	got := dotProductPureGo(x, y)
	want := float32(32)
	assert.Equal(t, want, got)
}

func TestASMdotProduct(t *testing.T) {
	x := []float32{1, 2, 3}
	y := []float32{4, 5, 6}
	got := asm.Dot(x, y)
	want := float32(32)
	assert.Equal(t, want, got)
}

func TestPureSquaredEuclidean(t *testing.T) {
	x := []float32{1, 2, 3}
	y := []float32{4, 5, 6}
	got := squaredEuclideanDistancePureGo(x, y)
	want := float32(27)
	assert.Equal(t, want, got)
}

func TestASMSquaredEuclidean(t *testing.T) {
	x := []float32{1, 2, 3}
	y := []float32{4, 5, 6}
	got := asm.SquaredEuclideanDistance(x, y)
	want := float32(27)
	assert.Equal(t, want, got)
}
