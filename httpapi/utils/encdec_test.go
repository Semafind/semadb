package utils_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/semafind/semadb/httpapi/utils"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
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

type testMap map[string]any

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
		{
			name:    "msgpack decoding",
			body:    func() []byte { b, _ := msgpack.Marshal(testMap{"hello": "world"}); return b }(),
			content: "application/msgpack",
			fail:    false,
		},
		{
			name:    "msgpack decoding error",
			body:    func() []byte { b, _ := msgpack.Marshal("hello"); return b }(),
			content: "application/msgpack",
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

func randVector(size int) []float32 {
	vector := make([]float32, size)
	for i := 0; i < size; i++ {
		vector[i] = rand.Float32()
	}
	return vector
}

/* The following benchmark is commented out to avoid external dependencies.
 * Currently it doesn't show any significant difference between encoding/json and
 * goccy/go-json. */
/* func BenchmarkDecodeJSON(b *testing.B) {
	pointSize := 1000
	vectorSize := 1536
	points := make([]map[string]any, pointSize)
	for i := 0; i < 1000; i++ {
		points[i] = map[string]any{"vector": randVector(vectorSize)}
	}
	data := map[string]any{"points": points, "hello": "world"}
	payload, err := json.Marshal(data)
	if err != nil {
		b.Fatal(err)
	}
	for _, decodeLibrary := range []string{"encoding/json", "goccy/go-json"} {
		b.Run(decodeLibrary, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Unmarshal the payload
				var v map[string]any
				var err error
				switch decodeLibrary {
				case "encoding/json":
					err = json.Unmarshal(payload, &v)
				case "goccy/go-json":
					err = gojson.Unmarshal(payload, &v)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}*/

// Benchmark decodevalid
func BenchmarkDecodeValid(b *testing.B) {
	tests := []struct {
		name       string
		pointSize  int
		vectorSize int
	}{
		{"small", 1000, 1536},
		{"medium", 5000, 1536},
		{"large", 7000, 1536},
	}
	for _, tt := range tests {
		points := make([]map[string]any, tt.pointSize)
		for i := 0; i < tt.pointSize; i++ {
			points[i] = map[string]any{"vector": randVector(tt.vectorSize)}
		}
		data := map[string]any{"points": points, "hello": "world"}
		for _, encoding := range []string{"application/json", "application/msgpack"} {
			var payload []byte
			switch encoding {
			case "application/json":
				var err error
				payload, err = json.Marshal(data)
				if err != nil {
					b.Fatal(err)
				}
			case "application/msgpack":
				var err error
				payload, err = msgpack.Marshal(data)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.Run(tt.name+"-"+encoding, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
					r.Header.Set("Content-Type", encoding)
					_, err := utils.DecodeValid[testMap](r)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
