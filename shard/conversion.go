package shard

import (
	"encoding/binary"
	"math"
)

func uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

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
