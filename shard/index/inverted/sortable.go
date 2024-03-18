package inverted

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Convert mixed precision types to 64 bit, passes through strings.
func upCastTo64(v any) (any, error) {
	/* This mess comes about because the index deals with 64 bit version yet
	 * parsing of JSON, or just incoming data, can be of mixed precision. */
	var vv = v
	switch v := v.(type) {
	// ---------------------------
	case int:
		vv = int64(v)
	case int8:
		vv = int64(v)
	case int16:
		vv = int64(v)
	case int32:
		vv = int64(v)
	case uint:
		vv = uint64(v)
	case uint8:
		vv = uint64(v)
	case uint16:
		vv = uint64(v)
	case uint32:
		vv = uint64(v)
	case float32:
		vv = float64(v)
	// ---------------------------
	case string:
	case float64:
	case uint64:
	case int64:
	// ---------------------------
	default:
		return nil, fmt.Errorf("unsupported upcast type %T", v)
	}
	return vv, nil
}

func toByteSortable(v any) ([]byte, error) {
	v, err := upCastTo64(v)
	if err != nil {
		return nil, err
	}
	// TODO: Refactor to use a type switch with generic type constraint [T indexable]
	// switch any(v).(type) {
	switch v := v.(type) {
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

func fromByteSortable(b []byte, v any) error {
	switch v := v.(type) {
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
