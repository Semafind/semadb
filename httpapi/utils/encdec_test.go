package utils_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/semafind/semadb/httpapi/utils"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	tests := []struct {
		name     string
		v        any
		status   int
		want     string
		wantCode int
	}{
		{
			name:     "successful encoding",
			v:        map[string]string{"hello": "world"},
			status:   http.StatusOK,
			want:     "{\"hello\":\"world\"}\n",
			wantCode: http.StatusOK,
		},
		{
			name:     "encoding error",
			v:        func() {}, // Not JSON encodable
			status:   http.StatusOK,
			want:     "{\"error\":\"json: unsupported type: func()\"}\n",
			wantCode: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			utils.Encode(w, tt.status, tt.v)
			require.Equal(t, tt.wantCode, w.Code)
			require.Equal(t, tt.want, w.Body.String())
		})
	}
}

type testMap map[string]string

func (t testMap) Validate() error {
	if _, ok := t["hello"]; !ok {
		return fmt.Errorf("missing key hello")
	}
	return nil
}

func TestDecodeValid(t *testing.T) {
	tests := []struct {
		name    string
		body    []byte
		content string
		fail    bool
	}{
		{
			name:    "successful decoding and validation",
			body:    []byte(`{"hello": "world"}`),
			content: "application/json",
			fail:    false,
		},
		{
			name:    "invalid content type",
			body:    []byte(`{"hello": "world"}`),
			content: "text/plain",
			fail:    true,
		},
		{
			name:    "decoding error",
			body:    []byte(`{"hello": "world"`),
			content: "application/json",
			fail:    true,
		},
		{
			name:    "validation error",
			body:    []byte(`{"gandalf": "world"}`),
			content: "application/json",
			fail:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tt.body))
			r.Header.Set("Content-Type", tt.content)
			v, err := utils.DecodeValid[testMap](r)
			if tt.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testMap{"hello": "world"}, v)
		})
	}
}

// Benchmark decodevalid
func BenchmarkDecodeValid(b *testing.B) {
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"hello": "world"}`)))
	r.Header.Set("Content-Type", "application/json")
	for i := 0; i < b.N; i++ {
		_, _ = utils.DecodeValid[testMap](r)
	}
}
