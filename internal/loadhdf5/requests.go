package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
)

const baseURL = "http://localhost:8081/v1"

func createCollection(name string, vectorSize uint, distanceMetric string) {
	endpoint := "/collections"
	colReqBody := map[string]interface{}{
		"id":             name,
		"vectorSize":     vectorSize,
		"distanceMetric": distanceMetric,
	}
	jsonBody, _ := json.Marshal(colReqBody)
	req, err := http.NewRequest(http.MethodPost, baseURL+endpoint, bytes.NewReader(jsonBody))
	if err != nil {
		log.Fatal().Err(err).Msg("loadHDF5 - create collection")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Id", "benchmark")
	req.Header.Set("X-Package", "benchmark")
	client := &http.Client{Timeout: 5 * time.Minute}
	res, err := client.Do(req)
	if err != nil {
		log.Fatal().Err(err).Msg("loadHDF5 - create collection")
	}
	log.Info().Str("status", res.Status).Msg("loadHDF5 - create collection")
	// Print the response body
	resBody, _ := io.ReadAll(res.Body)
	log.Info().Str("body", string(resBody)).Msg("loadHDF5 - create collection")
	// ---------------------------
}

func createPoints(collectionName string, points []NewPointRequest) error {
	endpoint := "/collections/" + collectionName + "/points"
	reqBody := map[string]interface{}{
		"points": points,
	}
	jsonBody, _ := json.Marshal(reqBody)
	req, err := http.NewRequest(http.MethodPost, baseURL+endpoint, bytes.NewReader(jsonBody))
	if err != nil {
		log.Fatal().Err(err).Msg("loadHDF5 - create points")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Id", "benchmark")
	req.Header.Set("X-Package", "benchmark")
	client := &http.Client{Timeout: 5 * time.Minute}
	res, err := client.Do(req)
	if err != nil {
		log.Fatal().Err(err).Msg("loadHDF5 - create points")
	}
	log.Info().Str("status", res.Status).Msg("loadHDF5 - create points")
	// Print the response body
	resBody, _ := io.ReadAll(res.Body)
	log.Info().Str("body", string(resBody)).Msg("loadHDF5 - create points")
	// ---------------------------
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("create points: %s", res.Status)
	}
	return nil
}
