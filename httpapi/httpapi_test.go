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

type RequestTest struct {
	Name    string
	Payload any
	Code    int
}

type CollectionState struct {
	Collection models.Collection
	Points     []models.Point
}

type ClusterNodeState struct {
	Collections []CollectionState
}

func setupClusterNode(t *testing.T, nodeS ClusterNodeState) *cluster.ClusterNode {
	tempDir := t.TempDir()
	cnode, err := cluster.NewNode(cluster.ClusterNodeConfig{
		RootDir: tempDir,
		Servers: []string{"localhost:9898"},
		// ---------------------------
		RpcHost:    "localhost",
		RpcPort:    9898,
		RpcTimeout: 5,
		RpcRetries: 2,
		// ---------------------------
		MaxShardSize:       268435456, // 2GiB
		MaxShardPointCount: 250000,
		ShardManager: cluster.ShardManagerConfig{
			RootDir:      tempDir,
			ShardTimeout: 30,
		},
	})
	assert.NoError(t, err)
	// Setup state
	for _, colState := range nodeS.Collections {
		// ---------------------------
		colState.Collection.UserPlan = models.UserPlan{
			Name:                    "BASIC",
			MaxCollections:          1,
			MaxCollectionPointCount: 2,
			MaxMetadataSize:         100,
			ShardBackupFrequency:    60,
			ShardBackupCount:        3,
		}
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
		UserPlans: map[string]models.UserPlan{
			"BASIC": {
				Name:                    "BASIC",
				MaxCollections:          1,
				MaxCollectionPointCount: 2,
				MaxMetadataSize:         100,
				ShardBackupFrequency:    60,
				ShardBackupCount:        3,
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
	req.Header.Set("X-Plan-Id", "BASIC")
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
	req.Header.Set("X-Plan-Id", "BASIC")
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
	// ---------------------------
	// Extra request trigger quota limit
	reqBody.Id = "testy2"
	resp = makeRequest(t, router, "POST", "/v1/collections", reqBody)
	assert.Equal(t, http.StatusForbidden, resp.Code)
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

func Test_DeleteCollection(t *testing.T) {
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
	resp := makeRequest(t, router, "DELETE", "/v1/collections/boromir", nil)
	assert.Equal(t, http.StatusNotFound, resp.Code)
	// ---------------------------
	resp = makeRequest(t, router, "DELETE", "/v1/collections/gandalf", nil)
	assert.Equal(t, http.StatusOK, resp.Code)
	// ---------------------------
	// Collection no longer exists
	resp = makeRequest(t, router, "GET", "/v1/collections/gandalf", nil)
	assert.Equal(t, http.StatusNotFound, resp.Code)
}

func Test_InsertPoints(t *testing.T) {
	nodeS := ClusterNodeState{
		Collections: []CollectionState{
			{
				Collection: models.Collection{
					UserId:     "testy",
					Id:         "gandalf",
					VectorSize: 2,
					DistMetric: "cosine",
					Parameters: models.DefaultVamanaParameters(),
				},
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	tests := []RequestTest{
		{
			Name: "Invalid vector size",
			Payload: InsertPointsRequest{
				Points: []InsertSinglePointRequest{
					{
						Vector: []float32{1, 2, 3},
					},
				},
			},
			Code: http.StatusBadRequest,
		},
		{
			Name: "Invalid metadata size",
			Payload: InsertPointsRequest{
				Points: []InsertSinglePointRequest{
					{
						Vector:   []float32{1, 2},
						Metadata: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
					},
				},
			},
			Code: http.StatusBadRequest,
		},
	}
	// ---------------------------
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			resp := makeRequest(t, router, "POST", "/v1/collections/gandalf/points", test.Payload)
			assert.Equal(t, test.Code, resp.Code)
		})
	}
	// ---------------------------
	// Can insert points
	reqBody := InsertPointsRequest{
		Points: []InsertSinglePointRequest{
			{
				Vector: []float32{1, 2},
			},
			{
				Vector: []float32{3, 4},
			},
		},
	}
	resp := makeRequest(t, router, "POST", "/v1/collections/gandalf/points", reqBody)
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody InsertPointsResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	assert.NoError(t, err)
	assert.Len(t, respBody.FailedRanges, 0)
	// Adding more points triggers quota limit
	resp = makeRequest(t, router, "POST", "/v1/collections/gandalf/points", reqBody)
	assert.Equal(t, http.StatusForbidden, resp.Code)
}
