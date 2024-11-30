package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rs/zerolog/log"
)

// Validator is an object that can be validated.
type Validator interface {
	Validate() error
}

// Encode writes the object to the response writer. It is usually used as the
// last step in a handler.
func Encode[T any](w http.ResponseWriter, status int, v T) {
	w.Header().Set("Content-Type", "application/json")
	// Write to buffer first to ensure the object is json encodable
	// before writing to the response writer.
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Error().Err(err).Msg("could not encode response")
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(status)
	w.Write(buf.Bytes())
}

// DecodeValid decodes the request body into the object and then validates it.
// Look at problems to see if there are any issues.
func DecodeValid[T Validator](r *http.Request) (T, error) {
	var v T
	if r.Header.Get("Content-Type") != "application/json" {
		return v, fmt.Errorf("expected content type application/json, got %s", r.Header.Get("Content-Type"))
	}
	// ---------------------------
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		return v, fmt.Errorf("decode json: %w", err)
	}
	if err := v.Validate(); err != nil {
		return v, fmt.Errorf("validation error: %w", err)
	}
	// ---------------------------
	return v, nil
}
