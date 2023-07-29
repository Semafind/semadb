package shard

import (
	"encoding/binary"
	"math"

	"github.com/google/uuid"
)

func float32ToBytes(f []float32) []byte {
	b := make([]byte, len(f)*4)
	for i, v := range f {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	return b
}

func bytesToFloat32(b []byte) []float32 {
	f := make([]float32, len(b)/4)
	for i := range f {
		f[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return f
}

func edgeListToBytes(edges []uuid.UUID) []byte {
	b := make([]byte, len(edges)*16)
	for i, e := range edges {
		copy(b[i*16:], e[:])
	}
	return b
}

func bytesToEdgeList(b []byte) []uuid.UUID {
	edges := make([]uuid.UUID, len(b)/16)
	for i := range edges {
		edges[i] = uuid.UUID{}
		copy(edges[i][:], b[i*16:(i+1)*16])
	}
	return edges
}
