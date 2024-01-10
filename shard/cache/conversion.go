package cache

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/rs/zerolog/log"
)

var float32ToBytes func([]float32) []byte = float32ToBytesSafe
var bytesToFloat32 func([]byte) []float32 = bytesToFloat32Safe

/* On the topic of endianness. Digging into unsafe package is not ideal but one
 * of the main bottle necks of a disk based vector database like SemaDB is
 * reading and writing vectors and graph edges. Vectors can be quite large, e.g.
 * 768 dimensions, meaning a lot of time is spent ensuring the byte order is
 * written and read correctly. The hope, perhaps naively, is that if the original
 * architecture matches the preferred little endian encoding, we can dump and
 * read data directly speeding things up significantly (see benchmark in test
 * file). We are only using this for the slices of floats as they are mainly
 * immutable in the database, i.e. we don't modify vectors just store and
 * compute distances between them. Single numbers are fast enough we don't
 * mind. Another thing to consider is that, it is unlikely that SemaDB will be
 * run on many different architectures. But as the unsafe package name suggests
 * it can be a risky thing to do. So we'll monitor this closely. Especially if
 * SemaDB is run in a cluster of heterogeneous machines.
 *
 * One of the roadmap items is to utilise a shared cache of decoded points
 * (including vectors). So we can potentially play safe (with safe functions) to
 * pay the cost once but benefit from the cache.
 *
 * In this context, endianness is concerned about the byte order of integers in
 * memory. What about floating point numbers? The IEEE 754 standard doesn't
 * mention any endianness. So we need to be careful again. Note that
 * math.Float32bits and math.Float64bits just give the raw bytes as an integer
 * under the hood, that is:
 *
 * func Float32bits(f float32) uint32 { return *(*uint32)(unsafe.Pointer(&f)) }
 * last checked 2009-12-15
 *
 * So to store a float32 array in a byte slice, we look to dump the raw bytes
 * into the slice if the architecture matches little endian. If not we use the
 * safe version. The same goes for reading the bytes back into a float32 array
 * which is focus point for the search action.
 */

func init() {
	// Determine native endianness
	var i uint16 = 0xABCD
	isLittleEndian := *(*byte)(unsafe.Pointer(&i)) == 0xCD
	log.Info().Bool("isLittleEndian", isLittleEndian).Msg("Endianness")
	if isLittleEndian {
		float32ToBytes = float32ToBytesRaw
		bytesToFloat32 = bytesToFloat32Raw
	}
}

func Uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

func BytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func float32ToBytesSafe(f []float32) []byte {
	b := make([]byte, len(f)*4)
	for i, v := range f {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	return b
}

func bytesToFloat32Safe(b []byte) []float32 {
	// We allocate a new slice because the original byte slice may be disposed.
	// Most likely this byte slices comes from a BoltDB transaction.
	f := make([]float32, len(b)/4)
	for i := range f {
		f[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return f
}

func float32ToBytesRaw(f []float32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&f[0])), len(f)*4)
}

func bytesToFloat32Raw(b []byte) []float32 {
	// TODO: When the byte slice is going to be used outside a transaction, we
	// need to allocate and copy.
	// f := make([]float32, len(b)/4)
	// copy(f, unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)/4))
	// return f
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)/4)
}

func edgeListToBytes(edges []uint64) []byte {
	b := make([]byte, len(edges)*8)
	for i, e := range edges {
		binary.LittleEndian.PutUint64(b[i*8:], e)
	}
	return b
}

func bytesToEdgeList(b []byte) []uint64 {
	edges := make([]uint64, len(b)/8)
	for i := range edges {
		edges[i] = binary.LittleEndian.Uint64(b[i*8:])
	}
	return edges
}
