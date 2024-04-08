package distance

import (
	"fmt"
	"math/bits"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/distance/asm"
	"github.com/semafind/semadb/models"
	"golang.org/x/sys/cpu"
)

type DistFunc func(x, y []float32) float32

var euclideanDistance DistFunc = squaredEuclideanDistancePureGo
var dotProductImpl DistFunc = dotProductPureGo

func hasASMSupport() bool {
	return cpu.X86.HasAVX2 && cpu.X86.HasFMA && cpu.X86.HasSSE3
}

func init() {
	if hasASMSupport() {
		log.Info().Msg("Using ASM support for dot and euclidean distance")
		dotProductImpl = asm.Dot
		euclideanDistance = asm.SquaredEuclideanDistance
	}
}

func dotProductDistance(x, y []float32) float32 {
	return -dotProductImpl(x, y)
}

func cosineDistance(x, y []float32) float32 {
	return 1 - dotProductImpl(x, y)
}

func HammingDistance(x, y []uint64) float32 {
	dist := 0
	for i := range x {
		// The XOR ^ operator returns a 1 in each bit position for which the
		// corresponding bits of the two operands are different. Then we count
		// the number of bits that are different.
		dist += bits.OnesCount64(x[i] ^ y[i])
	}
	return float32(dist)
}

func JaccardDistance(x, y []uint64) float32 {
	intersection := 0
	union := 0
	for i := range x {
		intersection += bits.OnesCount64(x[i] & y[i])
		union += bits.OnesCount64(x[i] | y[i])
	}
	return 1 - float32(intersection)/float32(union)
}

// GetDistanceFn returns the distance function by name.
func GetDistanceFn(name string) (DistFunc, error) {
	switch name {
	case models.DistanceEuclidean:
		return euclideanDistance, nil
	case models.DistanceDot:
		return dotProductDistance, nil
	case models.DistanceCosine:
		return cosineDistance, nil
	default:
		return nil, fmt.Errorf("unknown float32 distance function: %s", name)
	}
}
