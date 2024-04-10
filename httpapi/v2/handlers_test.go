package v2_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/httpapi/middleware"
	v2 "github.com/semafind/semadb/httpapi/v2"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type requestTest struct {
	Name    string
	Payload any
	Code    int
}

type pointState struct {
	Id   uuid.UUID
	Data models.PointAsMap
}

type collectionState struct {
	Collection models.Collection
	Points     []pointState
}

type clusterNodeState struct {
	Collections []collectionState
}

var sampleCollection models.Collection = models.Collection{
	Id:     "gandalf",
	UserId: "testy",
	IndexSchema: models.IndexSchema{
		"vector": {
			Type: "vectorVamana",
			VectorVamana: &models.IndexVectorVamanaParameters{
				VectorSize:     2,
				DistanceMetric: "euclidean",
				SearchSize:     75,
				DegreeBound:    64,
				Alpha:          1.2,
			},
		},
	},
}

func setupClusterNode(t *testing.T, nodeS clusterNodeState) *cluster.ClusterNode {
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
	require.NoError(t, err)
	// Setup state
	for _, colState := range nodeS.Collections {
		// ---------------------------
		colState.Collection.UserPlan = models.UserPlan{
			Name:                    "BASIC",
			MaxCollections:          1,
			MaxCollectionPointCount: 2,
			MaxPointSize:            100,
			ShardBackupFrequency:    60,
			ShardBackupCount:        3,
		}
		err := cnode.CreateCollection(colState.Collection)
		require.NoError(t, err)
		// ---------------------------
		if len(colState.Points) == 0 {
			continue
		}
		points := make([]models.Point, len(colState.Points))
		for i, p := range colState.Points {
			pointDataBytes, err := msgpack.Marshal(p.Data)
			require.NoError(t, err)
			points[i] = models.Point{
				Id:   p.Id,
				Data: pointDataBytes,
			}
		}
		failedRanges, err := cnode.InsertPoints(colState.Collection, points)
		require.NoError(t, err)
		require.Len(t, failedRanges, 0)
	}
	return cnode
}

func setupTestRouter(t *testing.T, nodeS clusterNodeState) *gin.Engine {
	router := gin.New()
	userPlans := map[string]models.UserPlan{
		"BASIC": {
			Name:                    "BASIC",
			MaxCollections:          1,
			MaxCollectionPointCount: 2,
			MaxPointSize:            100,
			ShardBackupFrequency:    60,
			ShardBackupCount:        3,
		},
	}
	v2g := router.Group("/v1", middleware.AppHeaderMiddleware(userPlans))
	v2.SetupV2Handlers(setupClusterNode(t, nodeS), v2g)
	return router
}

func makeRequest(t *testing.T, router *gin.Engine, method string, endpoint string, body any, resp any) int {
	t.Helper()
	// ---------------------------
	var bodyReader io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(jsonBody)
	}
	req, err := http.NewRequest(method, endpoint, bodyReader)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Id", "testy")
	req.Header.Set("X-Plan-Id", "BASIC")
	// ---------------------------
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	// ---------------------------
	if resp != nil {
		err = json.Unmarshal(recorder.Body.Bytes(), resp)
		require.NoError(t, err)
	}
	// ---------------------------
	return recorder.Code
}

func Test_pongHandler(t *testing.T) {
	router := setupTestRouter(t, clusterNodeState{})
	req, err := http.NewRequest("GET", "/v1/ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	// With App Headers
	req.Header.Set("X-User-Id", "testy")
	req.Header.Set("X-Plan-Id", "BASIC")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "{\"message\":\"pong from semadb\"}", w.Body.String())
}

func Test_CreateCollection(t *testing.T) {
	router := setupTestRouter(t, clusterNodeState{})
	// ---------------------------
	reqBody := v2.CreateCollectionRequest{
		Id: "testy",
		IndexSchema: models.IndexSchema{
			"vector": {
				Type: "vectorVamana",
				VectorVamana: &models.IndexVectorVamanaParameters{
					VectorSize:     42,
					DistanceMetric: "cosine",
					SearchSize:     75,
					DegreeBound:    64,
					Alpha:          1.2,
				},
			},
		},
	}
	resp := makeRequest(t, router, "POST", "/v1/collections", reqBody, nil)
	require.Equal(t, http.StatusOK, resp)
	// ---------------------------
	// Duplicate request conflicts
	resp = makeRequest(t, router, "POST", "/v1/collections", reqBody, nil)
	require.Equal(t, http.StatusConflict, resp)
	// ---------------------------
	// Extra request trigger quota limit
	reqBody.Id = "testy2"
	resp = makeRequest(t, router, "POST", "/v1/collections", reqBody, nil)
	require.Equal(t, http.StatusForbidden, resp)
}

func Test_ListCollections(t *testing.T) {
	// ---------------------------
	// Initially the user no collections
	router := setupTestRouter(t, clusterNodeState{})
	var respBody v2.ListCollectionsResponse
	resp := makeRequest(t, router, "GET", "/v1/collections", nil, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Len(t, respBody.Collections, 0)
	// ---------------------------
	// List user collections
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
			},
		},
	}
	router = setupTestRouter(t, nodeS)
	resp = makeRequest(t, router, "GET", "/v1/collections", nil, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Len(t, respBody.Collections, 1)
	require.Equal(t, "gandalf", respBody.Collections[0].Id)
}

func Test_GetCollection(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	// Unknown collection returns not found
	resp := makeRequest(t, router, "GET", "/v1/collections/boromir", nil, nil)
	require.Equal(t, http.StatusNotFound, resp)
	// ---------------------------
	var respBody v2.GetCollectionResponse
	resp = makeRequest(t, router, "GET", "/v1/collections/gandalf", nil, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Equal(t, "gandalf", respBody.Id)
	require.Len(t, respBody.Shards, 0)
}

func Test_DeleteCollection(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	// Unknown collection returns not found
	resp := makeRequest(t, router, "DELETE", "/v1/collections/boromir", nil, nil)
	require.Equal(t, http.StatusNotFound, resp)
	// ---------------------------
	resp = makeRequest(t, router, "DELETE", "/v1/collections/gandalf", nil, nil)
	require.Equal(t, http.StatusOK, resp)
	// ---------------------------
	// Collection no longer exists
	resp = makeRequest(t, router, "GET", "/v1/collections/gandalf", nil, nil)
	require.Equal(t, http.StatusNotFound, resp)
}

func Test_InsertPoints(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	tests := []requestTest{
		{
			Name: "Invalid vector size",
			Payload: v2.InsertPointsRequest{
				Points: []models.PointAsMap{
					{
						"vector": []float32{1, 2, 3},
					},
				},
			},
			Code: http.StatusBadRequest,
		},
		{
			Name: "Invalid metadata size",
			Payload: v2.InsertPointsRequest{
				Points: []models.PointAsMap{
					{
						"vector":   []float32{1, 2},
						"metadata": []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
					},
				},
			},
			Code: http.StatusBadRequest,
		},
	}
	// ---------------------------
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			resp := makeRequest(t, router, "POST", "/v1/collections/gandalf/points", test.Payload, nil)
			require.Equal(t, test.Code, resp)
		})
	}
	// ---------------------------
	// Can insert points
	reqBody := v2.InsertPointsRequest{
		Points: []models.PointAsMap{
			{
				"vector": []float32{1, 2},
			},
			{
				"vector": []float32{3, 4},
			},
		},
	}
	var respBody v2.InsertPointsResponse
	resp := makeRequest(t, router, "POST", "/v1/collections/gandalf/points", reqBody, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Len(t, respBody.FailedRanges, 0)
	// Adding more points triggers quota limit
	resp = makeRequest(t, router, "POST", "/v1/collections/gandalf/points", reqBody, nil)
	require.Equal(t, http.StatusForbidden, resp)
}
