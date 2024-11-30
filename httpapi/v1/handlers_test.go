package v1_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/httpapi/middleware"
	v1 "github.com/semafind/semadb/httpapi/v1"
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

func setupTestRouter(t *testing.T, nodeS clusterNodeState) http.Handler {
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
	handler := v1.SetupV1Handlers(setupClusterNode(t, nodeS))
	handler = middleware.AppHeaderMiddleware(userPlans, handler)
	return handler
}

func makeRequest(t *testing.T, router http.Handler, method string, endpoint string, body any) *httptest.ResponseRecorder {
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
	return recorder
}

func Test_pongHandler(t *testing.T) {
	router := setupTestRouter(t, clusterNodeState{})
	req, err := http.NewRequest("GET", "/ping", nil)
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
	require.Equal(t, "{\"message\":\"pong from semadb\"}\n", w.Body.String())
}

func Test_CreateCollection(t *testing.T) {
	router := setupTestRouter(t, clusterNodeState{})
	// ---------------------------
	reqBody := v1.CreateCollectionRequest{
		Id:             "testy",
		VectorSize:     128,
		DistanceMetric: "euclidean",
	}
	resp := makeRequest(t, router, "POST", "/collections", reqBody)
	require.Equal(t, http.StatusOK, resp.Code)
	// ---------------------------
	// Duplicate request conflicts
	resp = makeRequest(t, router, "POST", "/collections", reqBody)
	require.Equal(t, http.StatusConflict, resp.Code)
	// ---------------------------
	// Extra request trigger quota limit
	reqBody.Id = "testy2"
	resp = makeRequest(t, router, "POST", "/collections", reqBody)
	require.Equal(t, http.StatusForbidden, resp.Code)
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
				// Default values for the vamana algorithm
				SearchSize:  75,
				DegreeBound: 64,
				Alpha:       1.2,
			},
		},
	},
}

func Test_ListCollections(t *testing.T) {
	// ---------------------------
	// Initially the user no collections
	router := setupTestRouter(t, clusterNodeState{})
	resp := makeRequest(t, router, "GET", "/collections", nil)
	require.Equal(t, http.StatusOK, resp.Code)
	var respBody v1.ListCollectionsResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	require.NoError(t, err)
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
	resp = makeRequest(t, router, "GET", "/collections", nil)
	require.Equal(t, http.StatusOK, resp.Code)
	err = json.Unmarshal(resp.Body.Bytes(), &respBody)
	require.NoError(t, err)
	require.Len(t, respBody.Collections, 1)
	require.Equal(t, "gandalf", respBody.Collections[0].Id)
	require.EqualValues(t, 2, respBody.Collections[0].VectorSize)
	require.Equal(t, "euclidean", respBody.Collections[0].DistanceMetric)
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
	resp := makeRequest(t, router, "GET", "/collections/boromir", nil)
	require.Equal(t, http.StatusNotFound, resp.Code)
	// ---------------------------
	resp = makeRequest(t, router, "GET", "/collections/gandalf", nil)
	require.Equal(t, http.StatusOK, resp.Code)
	var respBody v1.GetCollectionResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	require.NoError(t, err)
	require.Equal(t, "gandalf", respBody.Id)
	require.Equal(t, "euclidean", respBody.DistanceMetric)
	require.EqualValues(t, 2, respBody.VectorSize)
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
	resp := makeRequest(t, router, "DELETE", "/collections/boromir", nil)
	require.Equal(t, http.StatusNotFound, resp.Code)
	// ---------------------------
	resp = makeRequest(t, router, "DELETE", "/collections/gandalf", nil)
	require.Equal(t, http.StatusOK, resp.Code)
	// ---------------------------
	// Collection no longer exists
	resp = makeRequest(t, router, "GET", "/collections/gandalf", nil)
	require.Equal(t, http.StatusNotFound, resp.Code)
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
			Payload: v1.InsertPointsRequest{
				Points: []v1.InsertSinglePointRequest{
					{
						Vector: []float32{1, 2, 3},
					},
				},
			},
			Code: http.StatusBadRequest,
		},
		{
			Name: "Invalid metadata size",
			Payload: v1.InsertPointsRequest{
				Points: []v1.InsertSinglePointRequest{
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
			resp := makeRequest(t, router, "POST", "/collections/gandalf/points", test.Payload)
			require.Equal(t, test.Code, resp.Code)
		})
	}
	// ---------------------------
	// Can insert points
	reqBody := v1.InsertPointsRequest{
		Points: []v1.InsertSinglePointRequest{
			{
				Vector: []float32{1, 2},
			},
			{
				Vector: []float32{3, 4},
			},
		},
	}
	resp := makeRequest(t, router, "POST", "/collections/gandalf/points", reqBody)
	fmt.Println(resp)
	require.Equal(t, http.StatusOK, resp.Code)
	var respBody v1.InsertPointsResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	require.NoError(t, err)
	require.Len(t, respBody.FailedRanges, 0)
	// Adding more points triggers quota limit
	resp = makeRequest(t, router, "POST", "/collections/gandalf/points", reqBody)
	require.Equal(t, http.StatusForbidden, resp.Code)
}

func Test_SearchPointsEmpty(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	sr := v1.SearchPointsRequest{
		Vector: []float32{1, 2},
	}
	resp := makeRequest(t, router, "POST", "/collections/gandalf/points/search", sr)
	require.Equal(t, http.StatusOK, resp.Code)
	var respBody v1.SearchPointsResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	require.NoError(t, err)
	require.Len(t, respBody.Points, 0)
}

func Test_SearchPoints(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
				Points: []pointState{
					{
						Id: uuid.New(),
						Data: models.PointAsMap{
							"vector":   []float32{1, 2},
							"metadata": "frodo",
						},
					},
					{
						Id: uuid.New(),
						Data: models.PointAsMap{
							"vector":   []float32{2, 3},
							"metadata": "sam",
						},
					},
				},
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	sr := v1.SearchPointsRequest{
		Vector: []float32{1, 2},
	}
	resp := makeRequest(t, router, "POST", "/collections/gandalf/points/search", sr)
	require.Equal(t, http.StatusOK, resp.Code)
	var respBody v1.SearchPointsResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	require.NoError(t, err)
	require.Len(t, respBody.Points, 2)
	require.Equal(t, "frodo", respBody.Points[0].Metadata)
}

func Test_UpdatePoints(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
				Points: []pointState{
					{
						Id: uuid.New(),
						Data: models.PointAsMap{
							"vector":   []float32{1, 2},
							"metadata": "frodo",
						},
					},
				},
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	// Large update fails
	r := v1.UpdatePointsRequest{
		Points: []v1.UpdateSinglePointRequest{
			{
				Id:       nodeS.Collections[0].Points[0].Id.String(),
				Vector:   []float32{3, 4},
				Metadata: make([]float64, 100),
			},
		},
	}
	resp := makeRequest(t, router, "PUT", "/collections/gandalf/points", r)
	require.Equal(t, http.StatusBadRequest, resp.Code)
	// ---------------------------
	// Update the point
	r = v1.UpdatePointsRequest{
		Points: []v1.UpdateSinglePointRequest{
			{
				Id:       nodeS.Collections[0].Points[0].Id.String(),
				Vector:   []float32{3, 4},
				Metadata: "sam",
			},
		},
	}
	// ---------------------------
	resp = makeRequest(t, router, "PUT", "/collections/gandalf/points", r)
	require.Equal(t, http.StatusOK, resp.Code)
	var respBody v1.UpdatePointsResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	require.NoError(t, err)
	require.Len(t, respBody.FailedPoints, 0)
	// ---------------------------
	// Has it been updated?
	sr := v1.SearchPointsRequest{
		Vector: []float32{3, 4},
	}
	resp = makeRequest(t, router, "POST", "/collections/gandalf/points/search", sr)
	require.Equal(t, http.StatusOK, resp.Code)
	var respBodySearch v1.SearchPointsResponse
	err = json.Unmarshal(resp.Body.Bytes(), &respBodySearch)
	require.NoError(t, err)
	require.Len(t, respBodySearch.Points, 1)
	require.Equal(t, "sam", respBodySearch.Points[0].Metadata)
}

func Test_DeletePoints(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
				Points: []pointState{
					{
						Id: uuid.New(),
						Data: models.PointAsMap{
							"vector":   []float32{1, 2},
							"metadata": "frodo",
						},
					},
					{
						Id: uuid.New(),
						Data: models.PointAsMap{
							"vector":   []float32{2, 3},
							"metadata": "sam",
						},
					},
				},
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	r := v1.DeletePointsRequest{
		Ids: []string{nodeS.Collections[0].Points[0].Id.String(), nodeS.Collections[0].Points[1].Id.String()},
	}
	resp := makeRequest(t, router, "DELETE", "/collections/gandalf/points", r)
	require.Equal(t, http.StatusOK, resp.Code)
	var respBody v1.DeletePointsResponse
	err := json.Unmarshal(resp.Body.Bytes(), &respBody)
	require.NoError(t, err)
	require.Len(t, respBody.FailedPoints, 0)
}
