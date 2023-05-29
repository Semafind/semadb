/* Conversion happens because the key value store only handles bytes.*/
package collection

import "github.com/vmihailenco/msgpack/v5"

func float32ToBytes(f []float32) ([]byte, error) {
	return msgpack.Marshal(f)
}

func bytesToFloat32(b []byte) ([]float32, error) {
	var f []float32
	err := msgpack.Unmarshal(b, &f)
	return f, err
}

func edgeListToBytes(edges []string) ([]byte, error) {
	return msgpack.Marshal(edges)
}

func bytesToEdgeList(b []byte) ([]string, error) {
	var edges []string
	err := msgpack.Unmarshal(b, &edges)
	return edges, err
}
