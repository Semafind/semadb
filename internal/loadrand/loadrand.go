package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/semafind/semadb/httpapi"
)

func randVector(size int) []float32 {
	vector := make([]float32, size)
	for i := 0; i < size; i++ {
		vector[i] = rand.Float32()
	}
	return vector
}

func randString(size int) string {
	str := make([]byte, size)
	for i := 0; i < size; i++ {
		str[i] = byte(rand.Intn(256))
	}
	return string(str)
}

func makeRequest(method string, path string, body any) {
	// Create request
	var encBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			log.Fatal(err)
		}
		encBody = bytes.NewReader(jsonBody)
	}
	req, err := http.NewRequest(method, ENDPOINT+path, encBody)
	log.Printf("Request: %s", req.URL)
	if err != nil {
		log.Fatal(err)
	}
	// Set headers
	req.Header.Set("X-User-Id", "loadrand")
	req.Header.Set("X-Plan-Id", "BASIC")
	req.Header.Set("Content-Type", "application/json")
	// Send request
	startTime := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Request took %s", time.Since(startTime))
	log.Printf("Response status: %s", resp.Status)
	// Close request
	defer resp.Body.Close()
	// Read response
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Print response
	fmt.Println(string(respBody))
}

const ENDPOINT = "http://localhost:8081/v1"
const VECTORSIZE = 768
const NUMVECTORS = 10000

func main() {
	// Create collection first
	makeRequest("POST", "/collections", httpapi.CreateCollectionRequest{
		Id:             "loadrand",
		VectorSize:     VECTORSIZE,
		DistanceMetric: "euclidean",
	})
	// Load vectors
	batchSize := 1000
	for i := 0; i < NUMVECTORS; i += batchSize {
		vectors := make([]httpapi.InsertSinglePointRequest, batchSize)
		for j := 0; j < batchSize; j++ {
			vectors[j] = httpapi.InsertSinglePointRequest{
				Vector:   randVector(VECTORSIZE),
				Metadata: randString(100),
			}
		}
		makeRequest("POST", "/collections/loadrand/points", httpapi.InsertPointsRequest{
			Points: vectors,
		})
	}
}
