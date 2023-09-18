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
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/assert"
)

type CollectionState struct {
	Collection models.Collection
	Points     []models.Point
}

type ClusterNodeState struct {
	Collections []CollectionState
}

func setupClusterNode(t *testing.T, nodeS ClusterNodeState) *cluster.ClusterNode {
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
	assert.NoError(t, err)
	// Setup state
	for _, colState := range nodeS.Collections {
		// ---------------------------
		err := cnode.CreateCollection(colState.Collection)
		assert.NoError(t, err)
		// ---------------------------
		failedRanges, err := cnode.InsertPoints(colState.Collection, colState.Points)
		assert.NoError(t, err)
		assert.Len(t, failedRanges, 0)
	}
	return cnode
}

func setupTestRouter(t *testing.T, nodeS ClusterNodeState) *gin.Engine {
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
	return setupRouter(setupClusterNode(t, nodeS), httpConfig, nil)
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
	router := setupTestRouter(t, ClusterNodeState{})
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
	router := setupTestRouter(t, ClusterNodeState{})
	// ---------------------------
	reqBody := CreateCollectionRequest{
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
	// ---------------------------
	// Initially the user no collections
	router := setupTestRouter(t, ClusterNodeState{})
	resp := makeRequest(t, router, "GET", "/v1/collections", nil)
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody ListCollectionsResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	assert.NoError(t, err)
	assert.Len(t, respBody.Collections, 0)
	// ---------------------------
	// List user collections
	nodeS := ClusterNodeState{
		Collections: []CollectionState{
			{
				Collection: models.Collection{
					Id:         "gandalf",
					UserId:     "testy",
					VectorSize: 42,
					DistMetric: "cosine",
				},
			},
		},
	}
	router = setupTestRouter(t, nodeS)
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
	nodeS := ClusterNodeState{
		Collections: []CollectionState{
			{
				Collection: models.Collection{
					UserId:     "testy",
					Id:         "gandalf",
					VectorSize: 42,
					DistMetric: "cosine",
				},
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	// Unknown collection returns not found
	resp := makeRequest(t, router, "GET", "/v1/collections/boromir", nil)
	assert.Equal(t, http.StatusNotFound, resp.Code)
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

// func Test_DeleteCollection(t *testing.T) {
// 	nodeS := ClusterNodeState{
// 		Collections: []CollectionState{
// 			{
// 				Collection: models.Collection{
// 					UserId:     "testy",
// 					Id:         "gandalf",
// 					VectorSize: 42,
// 					DistMetric: "cosine",
// 				},
// 			},
// 		},
// 	}
// 	router := setupTestRouter(t, nodeS)
// 	// ---------------------------
// 	// Unknown collection returns not found
// 	resp := makeRequest(t, router, "DELETE", "/v1/collections/boromir", nil)
// 	assert.Equal(t, http.StatusNotFound, resp.Code)
// 	// ---------------------------
// 	resp = makeRequest(t, router, "DELETE", "/v1/collections/gandalf", nil)
// 	assert.Equal(t, http.StatusOK, resp.Code)
// 	// ---------------------------
// 	// Collection no longer exists
// 	resp = makeRequest(t, router, "GET", "/v1/collections/gandalf", nil)
// 	assert.Equal(t, http.StatusNotFound, resp.Code)
// }
