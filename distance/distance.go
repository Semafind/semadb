package distance

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/distance/asm"
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

// GetDistanceFn returns the distance function by name.
func GetDistanceFn(name string) (DistFunc, error) {
	switch name {
	case "euclidean":
		return euclideanDistance, nil
	case "dot":
		return dotProductDistance, nil
	case "cosine":
		return cosineDistance, nil
	default:
		return nil, fmt.Errorf("unknown distance function: %s", name)
	}
}
