package v2_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/httpapi/middleware"
	v2 "github.com/semafind/semadb/httpapi/v2"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

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

var sampleIndexSchema = models.IndexSchema{
	"vector": models.IndexSchemaValue{
		Type: models.IndexTypeVectorVamana,
		VectorVamana: &models.IndexVectorVamanaParameters{
			VectorSize:     2,
			DistanceMetric: "euclidean",
			SearchSize:     75,
			DegreeBound:    64,
			Alpha:          1.2,
		},
	},
	"flat": models.IndexSchemaValue{
		Type: models.IndexTypeVectorFlat,
		VectorFlat: &models.IndexVectorFlatParameters{
			VectorSize:     2,
			DistanceMetric: "euclidean",
		},
	},
	"description": models.IndexSchemaValue{
		Type: models.IndexTypeText,
		Text: &models.IndexTextParameters{
			Analyser: "standard",
		},
	},
	"category": models.IndexSchemaValue{
		Type: models.IndexTypeString,
		String: &models.IndexStringParameters{
			CaseSensitive: false,
		},
	},
	"labels": models.IndexSchemaValue{
		Type: models.IndexTypeStringArray,
		StringArray: &models.IndexStringArrayParameters{
			IndexStringParameters: models.IndexStringParameters{
				CaseSensitive: false,
			},
		},
	},
	"size": models.IndexSchemaValue{
		Type: models.IndexTypeInteger,
	},
	"price": models.IndexSchemaValue{
		Type: models.IndexTypeFloat,
	},
	"nonExistent": models.IndexSchemaValue{
		Type: models.IndexTypeInteger,
	},
}

var sampleCollection models.Collection = models.Collection{
	Id:          "gandalf",
	UserId:      "testy",
	IndexSchema: sampleIndexSchema,
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
			MaxPointSize:            200,
			ShardBackupFrequency:    60,
			ShardBackupCount:        3,
		},
	}
	handler := v2.SetupV2Handlers(setupClusterNode(t, nodeS))
	handler = middleware.AppHeaderMiddleware(userPlans, handler)
	return handler
}

func makeRequest(t *testing.T, router http.Handler, method string, endpoint string, body any, resp any) int {
	t.Helper()
	// ---------------------------
	var bodyReader io.Reader
	if body != nil {
		var bodyAsString string
		if bodyString, ok := body.(string); ok {
			bodyAsString = bodyString
		} else {
			jsonBody, err := json.Marshal(body)
			require.NoError(t, err)
			bodyAsString = string(jsonBody)
		}
		bodyReader = bytes.NewReader([]byte(bodyAsString))
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
	t.Log(recorder.Body.String())
	// ---------------------------
	return recorder.Code
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
	reqBody := v2.CreateCollectionRequest{
		Id:          "testy",
		IndexSchema: sampleIndexSchema,
	}
	resp := makeRequest(t, router, "POST", "/collections", reqBody, nil)
	require.Equal(t, http.StatusOK, resp)
	// ---------------------------
	// Duplicate request conflicts
	resp = makeRequest(t, router, "POST", "/collections", reqBody, nil)
	require.Equal(t, http.StatusConflict, resp)
	// ---------------------------
	// Extra request trigger quota limit
	reqBody.Id = "testy2"
	resp = makeRequest(t, router, "POST", "/collections", reqBody, nil)
	require.Equal(t, http.StatusForbidden, resp)
}

func Test_CreateCollectionInvalidSchema(t *testing.T) {
	router := setupTestRouter(t, clusterNodeState{})
	// ---------------------------
	tests := []struct {
		Name   string
		Schema models.IndexSchema
	}{
		{
			"Missing vector parameters",
			models.IndexSchema{
				"vector": {
					Type: "vectorVamana",
				},
			},
		},
		{
			"Wrong distance metric",
			models.IndexSchema{
				"vector": {
					Type: "vectorFlat",
					VectorFlat: &models.IndexVectorFlatParameters{
						VectorSize:     2,
						DistanceMetric: "gandalf",
					},
				},
			},
		},
		{
			"Wrong quantizer type",
			models.IndexSchema{
				"vector": {
					Type: "vectorFlat",
					VectorFlat: &models.IndexVectorFlatParameters{
						VectorSize:     2,
						DistanceMetric: "euclidean",
						Quantizer: &models.Quantizer{
							Type: "random",
						},
					},
				},
			},
		},
		{
			"Wrong haversine size",
			models.IndexSchema{
				"vector": {
					Type: "vectorFlat",
					VectorFlat: &models.IndexVectorFlatParameters{
						VectorSize:     3,
						DistanceMetric: "haversine",
					},
				},
			},
		},
	}
	// ---------------------------
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			reqBody := v2.CreateCollectionRequest{
				Id:          "testy",
				IndexSchema: test.Schema,
			}
			resp := makeRequest(t, router, "POST", "/collections", reqBody, nil)
			require.Equal(t, http.StatusBadRequest, resp)
		})
	}
}

func Test_ListCollections(t *testing.T) {
	// ---------------------------
	// Initially the user no collections
	router := setupTestRouter(t, clusterNodeState{})
	var respBody v2.ListCollectionsResponse
	resp := makeRequest(t, router, "GET", "/collections", nil, &respBody)
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
	resp = makeRequest(t, router, "GET", "/collections", nil, &respBody)
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
	resp := makeRequest(t, router, "GET", "/collections/boromir", nil, nil)
	require.Equal(t, http.StatusNotFound, resp)
	// ---------------------------
	var respBody v2.GetCollectionResponse
	resp = makeRequest(t, router, "GET", "/collections/gandalf", nil, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Equal(t, "gandalf", respBody.Id)
	require.Len(t, respBody.Shards, 0)
	require.Len(t, respBody.IndexSchema, 8)
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
	resp := makeRequest(t, router, "DELETE", "/collections/boromir", nil, nil)
	require.Equal(t, http.StatusNotFound, resp)
	// ---------------------------
	resp = makeRequest(t, router, "DELETE", "/collections/gandalf", nil, nil)
	require.Equal(t, http.StatusOK, resp)
	// ---------------------------
	// Collection no longer exists
	resp = makeRequest(t, router, "GET", "/collections/gandalf", nil, nil)
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
	reqBody := v2.InsertPointsRequest{
		Points: []models.PointAsMap{
			{
				"_id":     uuid.New().String(),
				"vector":  []float32{1, 2},
				"myfield": "gandalf",
			},
			{
				"vector":      []float32{3, 4},
				"flat":        []float32{3, 4},
				"description": "This is a description",
				"category":    "category",
				"labels":      []string{"label1", "label2"},
				"size":        42,
				"price":       42.42,
			},
		},
	}
	var respBody v2.InsertPointsResponse
	resp := makeRequest(t, router, "POST", "/collections/gandalf/points", reqBody, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Len(t, respBody.FailedRanges, 0)
	// Adding more points triggers quota limit
	resp = makeRequest(t, router, "POST", "/collections/gandalf/points", reqBody, nil)
	require.Equal(t, http.StatusForbidden, resp)
}

type requestTest struct {
	Name   string
	Points []models.PointAsMap
}

var invalidPoints = []requestTest{
	{
		Name: "Invalid vector size",
		Points: []models.PointAsMap{
			{
				"vector": []float32{1, 2, 3},
			},
		},
	},
	{
		Name: "Invalid id",
		Points: []models.PointAsMap{
			{
				"_id":    "thisaintnoid",
				"vector": []float32{1, 2},
			},
		},
	},
	{
		Name: "Invalid metadata size",
		Points: []models.PointAsMap{
			{
				"_id":      uuid.New().String(),
				"vector":   []float32{1, 2},
				"metadata": make([]float64, 200),
			},
		},
	},
	{
		Name: "Invalid vector type",
		Points: []models.PointAsMap{
			{
				"_id":    uuid.New().String(),
				"vector": []any{1.0, "gandalf"},
			},
		},
	},
}

func Test_InsertPointsInvalid(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	// ---------------------------
	for _, test := range invalidPoints {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			r := v2.InsertPointsRequest{
				Points: test.Points,
			}
			resp := makeRequest(t, router, "POST", "/collections/gandalf/points", r, nil)
			require.Equal(t, http.StatusBadRequest, resp)
		})
	}
	// ---------------------------
}

func Test_UpdatePointsInvalid(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	for _, test := range invalidPoints {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			r := v2.UpdatePointsRequest{
				Points: test.Points,
			}
			resp := makeRequest(t, router, "PUT", "/collections/gandalf/points", r, nil)
			require.Equal(t, http.StatusBadRequest, resp)
		})
	}
	// ---------------------------
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
							"vector":  []float32{1, 2},
							"myfield": "gandalf",
						},
					},
				},
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	reqBody := v2.UpdatePointsRequest{
		Points: []models.PointAsMap{
			{
				"_id":     nodeS.Collections[0].Points[0].Id.String(),
				"vector":  []float32{3, 4},
				"size":    42,
				"myfield": "frodo",
			},
			{
				"_id":    uuid.New().String(),
				"doesnt": "exist",
			},
		},
	}
	var respBody v2.UpdatePointsResponse
	resp := makeRequest(t, router, "PUT", "/collections/gandalf/points", reqBody, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Len(t, respBody.FailedPoints, 1)
	require.Equal(t, reqBody.Points[1]["_id"], respBody.FailedPoints[0].Id.String())
	// ---------------------------
	// Can we see the changes?
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "size",
			Integer: &models.SearchIntegerOptions{
				Value:    42,
				Operator: models.OperatorEquals,
			},
		},
		Select: []string{"*"},
		Limit:  10,
	}
	var searchResp v2.SearchPointsResponse
	resp = makeRequest(t, router, "POST", "/collections/gandalf/points/search", sr, &searchResp)
	require.Equal(t, http.StatusOK, resp)
	require.Len(t, searchResp.Points, 1)
	require.EqualValues(t, 42, searchResp.Points[0]["size"])
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
	r := v2.DeletePointsRequest{
		Ids: []string{uuid.New().String(), nodeS.Collections[0].Points[0].Id.String(), nodeS.Collections[0].Points[1].Id.String()},
	}
	var respBody v2.DeletePointsResponse
	resp := makeRequest(t, router, "DELETE", "/collections/gandalf/points", r, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Len(t, respBody.FailedPoints, 1)
	require.Equal(t, r.Ids[0], respBody.FailedPoints[0].Id.String())
}

func Test_SearchPoints_Empty(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "size",
			Integer: &models.SearchIntegerOptions{
				Value:    42,
				Operator: models.OperatorEquals,
			},
		},
		Limit: 10,
	}
	var respBody v2.SearchPointsResponse
	resp := makeRequest(t, router, "POST", "/collections/gandalf/points/search", sr, &respBody)
	require.Equal(t, http.StatusOK, resp)
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
							"vector":      []float32{1, 2},
							"description": "hobbit frodo",
						},
					},
					{
						Id: uuid.New(),
						Data: models.PointAsMap{
							"vector":      []float32{2, 3},
							"description": "hobbit sam",
						},
					},
				},
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "description",
			Text: &models.SearchTextOptions{
				Value:    "frodo",
				Operator: models.OperatorContainsAll,
				Limit:    10,
			},
		},
		Select: []string{"description"},
		Limit:  10,
	}
	var respBody v2.SearchPointsResponse
	resp := makeRequest(t, router, "POST", "/collections/gandalf/points/search", sr, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Len(t, respBody.Points, 1)
	require.Equal(t, "hobbit frodo", respBody.Points[0]["description"])
	require.Equal(t, float64(0), respBody.Points[0]["_hybridScore"])
	require.Equal(t, nodeS.Collections[0].Points[0].Id.String(), respBody.Points[0]["_id"])
}

func Test_SearchPoints_NonExistent(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
				Points: []pointState{
					{
						Id: uuid.New(),
						Data: models.PointAsMap{
							"vector": []float32{1, 2},
						},
					},
				},
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "nonExistent",
			Integer: &models.SearchIntegerOptions{
				Value:    42,
				Operator: models.OperatorEquals,
			},
		},
		Limit: 10,
	}
	var respBody v2.SearchPointsResponse
	resp := makeRequest(t, router, "POST", "/collections/gandalf/points/search", sr, &respBody)
	require.Equal(t, http.StatusOK, resp)
	require.Len(t, respBody.Points, 0)
}

func Test_SearchPoints_Invalid(t *testing.T) {
	nodeS := clusterNodeState{
		Collections: []collectionState{
			{
				Collection: sampleCollection,
				Points: []pointState{
					{
						Id: uuid.New(),
						Data: models.PointAsMap{
							"vector":      []float32{1, 2},
							"description": "hobbit frodo",
						},
					},
					{
						Id: uuid.New(),
						Data: models.PointAsMap{
							"vector":      []float32{2, 3},
							"description": "hobbit sam",
						},
					},
				},
			},
		},
	}
	router := setupTestRouter(t, nodeS)
	// ---------------------------
	tests := []struct {
		Name string
		Req  string
	}{
		{
			"Blank request",
			"",
		},
		{
			"Missing limit",
			`{
				"query": {
					"property": "description"
				}
			}`,
		},
		{
			"Missing search options",
			`{
				"query": {
					"property": "description"
				},
				"limit": 10
			}`,
		},
		{
			"Wrong vector size for flat index",
			`{
				"query": {
					"property": "flat",
					"vectorFlat": {
						"vector": [1, 2, 3],
						"operator": "near",
						"limit": 10
					}
				},
				"limit": 10
			}`,
		},
		{
			"Wrong vector size for vamana index",
			`{
				"query": {
					"property": "vector",
					"vectorVamana": {
						"vector": [1, 2, 3],
						"operator": "near",
						"searchSize": 75,
						"limit": 10
					}
				},
				"limit": 10
			}`,
		},
		{
			"Invalid sub options",
			`{
				"query": {
					"property": "vector",
					"vectorVamana": {
						"vector": [1, 2],
						"operator": "near",
						"searchSize": 200,
						"limit": 10
					}
				},
				"limit": 10
			}`,
		},
	}
	// ---------------------------
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			resp := makeRequest(t, router, "POST", "/collections/gandalf/points/search", test.Req, nil)
			require.Equal(t, http.StatusBadRequest, resp)
		})
	}
}
