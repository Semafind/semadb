package distance

import (
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/distance/asm"
	"golang.org/x/sys/cpu"
)

type DistFunc func(x, y []float32) float32

var EuclideanDistance DistFunc = squaredEuclideanDistancePureGo
var dotProductImpl DistFunc = dotProductPureGo

func hasASMSupport() bool {
	return cpu.X86.HasAVX2 && cpu.X86.HasFMA && cpu.X86.HasSSE3
}

func init() {
	if hasASMSupport() {
		log.Info().Msg("Using ASM support for dot and euclidean distance")
		dotProductImpl = asm.Dot
		EuclideanDistance = asm.SquaredEuclideanDistance
	}
}

func DotProductDistance(x, y []float32) float32 {
	return -dotProductImpl(x, y)
}

func CosineDistance(x, y []float32) float32 {
	return 1 - dotProductImpl(x, y)
}
