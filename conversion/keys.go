package conversion

import "encoding/binary"

func NodeKey(id uint64, suffix byte) []byte {
	key := [10]byte{}
	key[0] = 'n'
	binary.LittleEndian.PutUint64(key[1:], id)
	key[9] = suffix
	return key[:]
}

func NodeIdFromKey(key []byte, suffix byte) (uint64, bool) {
	if len(key) != 10 || key[0] != 'n' || key[9] != suffix {
		return 0, false
	}
	return binary.LittleEndian.Uint64(key[1 : len(key)-1]), true
}
