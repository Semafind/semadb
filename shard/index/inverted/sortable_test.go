package inverted

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ToByteSortable(t *testing.T) {
	// Test cases
	tests := []struct {
		name string
		v    []any
	}{
		{
			name: "int64",
			v:    []any{-10, -1, 2, 3},
		},
		{
			name: "uint64",
			v:    []any{uint64(1), uint64(2)},
		},
		{
			name: "float64",
			v:    []any{-10.7, -1.1, 2.2, 3.3},
		},
		{
			name: "string",
			v:    []any{"a", "b", "c", "d"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byteArr := make([][]byte, len(tt.v))
			for i, v := range tt.v {
				var bv []byte
				var err error
				switch v := v.(type) {
				case float64:
					bv, err = toByteSortable(v)
				case int64:
					bv, err = toByteSortable(v)
				case uint64:
					bv, err = toByteSortable(v)
				case string:
					bv, err = toByteSortable(v)
				}
				require.NoError(t, err)
				byteArr[i] = bv
			}
			for i := 1; i < len(byteArr); i++ {
				require.True(t, bytes.Compare(byteArr[i-1], byteArr[i]) <= 0)
			}
		})
	}
}

func Test_FromByteSortable(t *testing.T) {
	// Test cases
	values := []any{
		int64(-10), int64(-1), int64(2), int64(3),
		uint64(1), uint64(2),
		float64(-10.7), float64(-1.1), float64(2.2), float64(3.3),
		"a", "b", "c", "d",
		float64(1.4), float64(4.2),
	}
	for _, v := range values {
		switch v := v.(type) {
		case float64:
			bv, err := toByteSortable(v)
			require.NoError(t, err)
			var vv float64
			err = fromByteSortable(bv, &vv)
			require.NoError(t, err)
			require.Equal(t, v, vv)
		case int64:
			bv, err := toByteSortable(v)
			require.NoError(t, err)
			var vv int64
			err = fromByteSortable(bv, &vv)
			require.NoError(t, err)
			require.Equal(t, v, vv)
		case uint64:
			bv, err := toByteSortable(v)
			require.NoError(t, err)
			var vv uint64
			err = fromByteSortable(bv, &vv)
			require.NoError(t, err)
			require.Equal(t, v, vv)
		case string:
			bv, err := toByteSortable(v)
			require.NoError(t, err)
			var vv string
			err = fromByteSortable(bv, &vv)
			require.NoError(t, err)
			require.Equal(t, v, vv)
		default:
			require.FailNow(t, "unsupported type")
		}
	}
}
