package inverted

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_UpCast(t *testing.T) {
	// Test cases
	tests := []struct {
		name    string
		v       any
		want    any
		wantErr bool
	}{
		{
			name: "int32",
			v:    int32(1),
			want: int64(1),
		},
		{
			name: "uint32",
			v:    uint32(1),
			want: uint64(1),
		},
		{
			name: "float32",
			v:    float32(1),
			want: float64(1),
		},
		{
			name: "string",
			v:    "a",
			want: "a",
		},
		{
			name:    "unsupported type",
			v:       []int{1},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := upCastTo64(tt.v)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

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
				bv, err := toByteSortable(v)
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
		-10, -1, 2, 3,
		uint64(1), uint64(2),
		-10.7, -1.1, 2.2, 3.3,
		"a", "b", "c", "d",
		float64(1.4),
		float32(4.2),
	}
	for _, v := range values {
		bv, err := toByteSortable(v)
		require.NoError(t, err)
		switch v.(type) {
		case float64:
			var vv float64
			err = fromByteSortable(bv, &vv)
			require.NoError(t, err)
			require.Equal(t, v, vv)
		case int64:
			var vv int64
			err = fromByteSortable(bv, &vv)
			require.NoError(t, err)
			require.Equal(t, v, vv)
		case uint64:
			var vv uint64
			err = fromByteSortable(bv, &vv)
			require.NoError(t, err)
			require.Equal(t, v, vv)
		case string:
			var vv string
			err = fromByteSortable(bv, &vv)
			require.NoError(t, err)
			require.Equal(t, v, vv)
		}
	}
}
