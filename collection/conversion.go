/* Conversion happens because the key value store only handles bytes.*/
package collection

import (
	"encoding/binary"
	"math"

	"github.com/vmihailenco/msgpack/v5"
)

func uint64ToBytes(u uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, u)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func float32ToBytes(f []float32) ([]byte, error) {
	b := make([]byte, len(f)*4)
	for i, v := range f {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	return b, nil
}

func bytesToFloat32(b []byte) ([]float32, error) {
	f := make([]float32, len(b)/4)
	for i := range f {
		f[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return f, nil
}

func edgeListToBytes(edges []uint64) ([]byte, error) {
	return msgpack.Marshal(edges)
}

func bytesToEdgeList(b []byte) ([]uint64, error) {
	var edges []uint64
	err := msgpack.Unmarshal(b, &edges)
	return edges, err
}
