package distance

import (
	"runtime"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/distance/asm"
	"golang.org/x/sys/cpu"
)

/* This init function allows us to override the dotProductImpl and
 * euclideanDistance functions with the assembly implementations if the CPU
 * supports it. Note the file name has _amd64 suffix. This means it will only be
 * compiled if runtime.GOARCH is amd64.
 *
 * Read more at https://pkg.go.dev/cmd/go#hdr-Build_constraints
 */

func init() {
	if cpu.X86.HasAVX2 && cpu.X86.HasFMA && cpu.X86.HasSSE3 {
		log.Info().Str("GOARCH", runtime.GOARCH).Msg("Using ASM support for dot and euclidean distance")
		dotProductImpl = asm.Dot
		euclideanDistance = asm.SquaredEuclideanDistance
	} else {
		log.Warn().Str("GOARCH", runtime.GOARCH).Msg("No ASM support for dot and euclidean distance")
	}
}
