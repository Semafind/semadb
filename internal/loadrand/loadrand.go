package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	httpapi "github.com/semafind/semadb/httpapi/v1"
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
		str[i] = byte(rand.IntN(256))
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
const VECTORSIZE = 1536
const NUMVECTORS = 10000
const BATCHSIZE = 10000

func main() {
	// Create collection first
	colName := strconv.Itoa(VECTORSIZE) + "d"
	makeRequest("DELETE", "/collections/"+colName, nil)
	makeRequest("POST", "/collections", httpapi.CreateCollectionRequest{
		Id:             colName,
		VectorSize:     VECTORSIZE,
		DistanceMetric: "euclidean",
	})
	// Load vectors
	for i := 0; i < NUMVECTORS; i += BATCHSIZE {
		vectors := make([]httpapi.InsertSinglePointRequest, BATCHSIZE)
		for j := 0; j < BATCHSIZE; j++ {
			vectors[j] = httpapi.InsertSinglePointRequest{
				Vector:   randVector(VECTORSIZE),
				Metadata: randString(100),
			}
		}
		makeRequest("POST", "/collections/"+colName+"/points", httpapi.InsertPointsRequest{
			Points: vectors,
		})
	}
}
