package distance

import (
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/distance/asm"
	"golang.org/x/sys/cpu"
)

type DistFunc func(x, y []float32) float32

var EuclideanDistance DistFunc = eucDistPureGo
var dotProductImpl DistFunc = dotProductPureGo

func init() {
	if cpu.X86.HasAVX2 && cpu.X86.HasFMA && cpu.X86.HasSSE3 {
		log.Info().Msg("Using AVX2 dot product implementation")
		dotProductImpl = asm.Dot
	}
}

func eucDistPureGo(x, y []float32) float32 {
	var sum float32
	for i := range x {
		diff := x[i] - y[i]
		sum += diff * diff
	}
	return sum
}

func dotProductPureGo(x, y []float32) float32 {
	var sum float32
	for i := range x {
		sum += x[i] * y[i]
	}
	return sum
}

func DotProductDistance(x, y []float32) float32 {
	return -dotProductImpl(x, y)
}

func CosineDistance(x, y []float32) float32 {
	return 1 - dotProductImpl(x, y)
}
