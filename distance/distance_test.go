package distance

import (
	"testing"

	"github.com/stretchr/testify/require"
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
			require.Equal(t, tt.wantDot, got)
		})
	}
}

func TestPureSquaredEuclidean(t *testing.T) {
	for _, tt := range vectorTable {
		t.Run(tt.name, func(t *testing.T) {
			got := squaredEuclideanDistancePureGo(tt.x, tt.y)
			require.Equal(t, tt.wantEuclidean, got)
		})
	}
}

func TestHammingDistance(t *testing.T) {
	x := []uint64{0b1001, 0b1}
	y := []uint64{0b1101, 0b0}
	dist := hammingDistance(x, y)
	require.Equal(t, float32(2), dist)
}

func TestJaccardDistance(t *testing.T) {
	x := []uint64{0b1001, 0b1}
	y := []uint64{0b1101, 0b0}
	dist := jaccardDistance(x, y)
	require.Equal(t, float32(0.5), dist)
	x = []uint64{0b0, 0b0}
	y = []uint64{0b0, 0b0}
	dist = jaccardDistance(x, y)
	require.Equal(t, float32(0.0), dist)
}

func TestHaversineDistance(t *testing.T) {
	// Airport example from
	// https://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.haversine_distances.html
	x := []float32{-34.83333, -58.5166646}
	y := []float32{49.0083899664, 2.53844117956}
	dist := haversineDistance(x, y)
	dist /= 1000 // in km
	require.InDelta(t, 11099.54, dist, 0.01)
}
