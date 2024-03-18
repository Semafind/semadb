package utils_test

import (
	"testing"

	"github.com/semafind/semadb/utils"
)

func Test_CompareAny(t *testing.T) {
	// Test cases
	tests := []struct {
		name string
		a    any
		b    any
		want int
	}{
		{
			name: "int32",
			a:    int32(1),
			b:    int32(1),
			want: 0,
		},
		{
			name: "uint32",
			a:    uint32(1),
			b:    uint32(1),
			want: 0,
		},
		{
			name: "float32",
			a:    float32(1),
			b:    float32(1),
			want: 0,
		},
		{
			name: "string",
			a:    "a",
			b:    "a",
			want: 0,
		},
		{
			name: "different types",
			a:    "a",
			b:    1,
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := utils.CompareAny(tt.a, tt.b); got != tt.want {
				t.Errorf("CompareAny() = %v, want %v", got, tt.want)
			}
		})
	}
}
