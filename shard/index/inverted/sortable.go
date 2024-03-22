package inverted

import (
	"encoding/binary"
	"fmt"
	"math"
)

func toByteSortable[T Invertable](v T) ([]byte, error) {
	switch v := any(v).(type) {
	case string:
		return []byte(v), nil
	case uint64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], v)
		return buf[:], nil
	case int64:
		var buf [8]byte
		/* To make negative numbers sort correctly, we xor with the min int64
		 * value. This transforms the number into the range of uint64. For
		 * example, for an int8 example, 0 becomes 128, -1 becomes 127, -2
		 * becomes 126, etc while 1 becomes 129 and so on. So negative numbers
		 * decrease from the mid range the unsigned type and positive numbers
		 * increased from the mid point. An alternative could be to (v -
		 * math.MinInt64) to achieve the same effect. */
		vv := uint64(v ^ math.MinInt64)
		// vv := uint64(v - math.MinInt64)
		binary.BigEndian.PutUint64(buf[:], vv)
		return buf[:], nil
	case float64:
		/* Floats are bit more tricky to convert to a sortable byte array but follow a similar principle:
		 * https://stackoverflow.com/questions/54557158/byte-ordering-of-floats
		 */
		bits := math.Float64bits(v)
		if v >= 0 {
			bits ^= 0x8000000000000000 // math.MinInt64
		} else {
			bits ^= 0xffffffffffffffff // math.MaxUint64
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], bits)
		return buf[:], nil
	}
	return nil, fmt.Errorf("unsupported sortable type %T", v)
}

func fromByteSortable[T Invertable](b []byte, v *T) error {
	switch v := any(v).(type) {
	case *string:
		*v = string(b)
	case *uint64:
		*v = binary.BigEndian.Uint64(b)
	case *int64:
		vv := binary.BigEndian.Uint64(b)
		*v = int64(vv) ^ math.MinInt64
	case *float64:
		bits := binary.BigEndian.Uint64(b)
		// Check sign bit
		if bits&0x8000000000000000 != 0 {
			bits ^= 0x8000000000000000 // math.MinInt64
		} else {
			bits ^= 0xffffffffffffffff // math.MaxUint64
		}
		*v = math.Float64frombits(bits)
	default:
		return fmt.Errorf("unsupported sortable type %T", v)
	}
	return nil
}
