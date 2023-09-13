package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/semafind/semadb/cluster"
	"github.com/stretchr/testify/assert"
)

func setupClusterNode(t *testing.T) *cluster.ClusterNode {
	cnode, err := cluster.NewNode(cluster.ClusterNodeConfig{
		RootDir: t.TempDir(),
		Servers: []string{"localhost:9898"},
		// ---------------------------
		RpcHost:    "localhost",
		RpcPort:    9898,
		RpcTimeout: 5,
		RpcRetries: 2,
		// ---------------------------
		ShardTimeout:       30,
		MaxShardSize:       268435456, // 2GiB
		MaxShardPointCount: 250000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cnode
}

func setupTestRouter(t *testing.T) *gin.Engine {
	httpConfig := HttpApiConfig{
		Debug:    true,
		HttpHost: "localhost",
		HttpPort: 8081,
		UserPlans: map[string]UserPlan{
			"BASIC": {
				Name:              "BASIC",
				MaxCollections:    1,
				MaxCollectionSize: 1073741824, // 1GiB
				MaxMetadataSize:   1024,
			},
		},
	}
	return setupRouter(setupClusterNode(t), httpConfig)
}

func makeRequest(t *testing.T, router *gin.Engine, method string, endpoint string, body any) *httptest.ResponseRecorder {
	// ---------------------------
	var bodyReader io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		assert.NoError(t, err)
		bodyReader = bytes.NewReader(jsonBody)
	}
	req, err := http.NewRequest(method, endpoint, bodyReader)
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Id", "testy")
	req.Header.Set("X-Package", "BASIC")
	// ---------------------------
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func Test_pongHandler(t *testing.T) {
	router := setupTestRouter(t)
	req, err := http.NewRequest("GET", "/v1/ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	// With App Headers
	req.Header.Set("X-User-Id", "testy")
	req.Header.Set("X-Package", "BASIC")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "{\"message\":\"pong from semadb\"}", w.Body.String())
}

func Test_CreateCollection(t *testing.T) {
	router := setupTestRouter(t)
	// ---------------------------
	reqBody := NewCollectionRequest{
		Id:             "testy",
		VectorSize:     128,
		DistanceMetric: "euclidean",
	}
	resp := makeRequest(t, router, "POST", "/v1/collections", reqBody)
	assert.Equal(t, http.StatusOK, resp.Code)
	// ---------------------------
	// Duplicate request conflicts
	resp = makeRequest(t, router, "POST", "/v1/collections", reqBody)
	assert.Equal(t, http.StatusConflict, resp.Code)
}

func Test_ListCollections(t *testing.T) {
	router := setupTestRouter(t)
	// ---------------------------
	// Initially the user no collections
	resp := makeRequest(t, router, "GET", "/v1/collections", nil)
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody ListCollectionsResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	assert.NoError(t, err)
	assert.Len(t, respBody.Collections, 0)
	// ---------------------------
	// Create one collection
	newColBody := NewCollectionRequest{
		Id:             "gandalf",
		VectorSize:     42,
		DistanceMetric: "cosine",
	}
	resp = makeRequest(t, router, "POST", "/v1/collections", newColBody)
	assert.Equal(t, http.StatusOK, resp.Code)
	// ---------------------------
	// List user collections
	resp = makeRequest(t, router, "GET", "/v1/collections", nil)
	assert.Equal(t, http.StatusOK, resp.Code)
	err = json.Unmarshal(resp.Body.Bytes(), &respBody)
	assert.NoError(t, err)
	assert.Len(t, respBody.Collections, 1)
	assert.Equal(t, "gandalf", respBody.Collections[0].Id)
	assert.EqualValues(t, 42, respBody.Collections[0].VectorSize)
	assert.Equal(t, "cosine", respBody.Collections[0].DistanceMetric)
}

func Test_GetCollection(t *testing.T) {
	router := setupTestRouter(t)
	// ---------------------------
	// Unknown collection returns not found
	resp := makeRequest(t, router, "GET", "/v1/collections/gandalf", nil)
	assert.Equal(t, http.StatusNotFound, resp.Code)
	// ---------------------------
	// Create one collection and get it
	newColBody := NewCollectionRequest{
		Id:             "gandalf",
		VectorSize:     42,
		DistanceMetric: "cosine",
	}
	resp = makeRequest(t, router, "POST", "/v1/collections", newColBody)
	assert.Equal(t, http.StatusOK, resp.Code)
	// ---------------------------
	resp = makeRequest(t, router, "GET", "/v1/collections/gandalf", nil)
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody GetCollectionResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	assert.NoError(t, err)
	assert.Equal(t, "gandalf", respBody.Id)
	assert.Equal(t, "cosine", respBody.DistanceMetric)
	assert.EqualValues(t, 42, respBody.VectorSize)
	assert.Len(t, respBody.Shards, 0)
}
