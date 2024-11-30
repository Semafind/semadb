package v1

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/httpapi/middleware"
	"github.com/semafind/semadb/httpapi/utils"
	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack/v5"
)

// ---------------------------

func handlePing(w http.ResponseWriter, r *http.Request) {
	utils.Encode(w, http.StatusOK, map[string]string{"message": "pong from semadb"})
}

type SemaDBHandlers struct {
	clusterNode *cluster.ClusterNode
}

// Requires middleware.AppHeaderMiddleware to be used
func SetupV1Handlers(clusterNode *cluster.ClusterNode) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", handlePing)
	semaDBHandlers := &SemaDBHandlers{clusterNode: clusterNode}
	// https://stackoverflow.blog/2020/03/02/best-practices-for-rest-api-design/
	mux.HandleFunc("GET /collections", semaDBHandlers.HandleListCollections)
	mux.HandleFunc("POST /collections", semaDBHandlers.HandleCreateCollection)
	// ---------------------------
	withCol := func(next http.HandlerFunc) http.Handler {
		return semaDBHandlers.CollectionURIMiddleware(http.HandlerFunc(next))
	}
	mux.Handle("GET /collections/{collectionId}", withCol(semaDBHandlers.HandleGetCollection))
	mux.Handle("DELETE /collections/{collectionId}", withCol(semaDBHandlers.HandleDeleteCollection))
	// We're batching point requests for peformance reasons. Alternatively we
	// can provide points/:pointId endpoint in the future.
	mux.Handle("POST /collections/{collectionId}/points", withCol(semaDBHandlers.HandleInsertPoints))
	mux.Handle("PUT /collections/{collectionId}/points", withCol(semaDBHandlers.HandleUpdatePoints))
	mux.Handle("DELETE /collections/{collectionId}/points", withCol(semaDBHandlers.HandleDeletePoints))
	mux.Handle("POST /collections/{collectionId}/points/search", withCol(semaDBHandlers.HandleSearchPoints))
	// ---------------------------
	return mux
}

// ---------------------------

type CreateCollectionRequest struct {
	Id             string `json:"id" binding:"required,alphanum,min=3,max=16"`
	VectorSize     uint   `json:"vectorSize" binding:"required"`
	DistanceMetric string `json:"distanceMetric" binding:"required,oneof=euclidean cosine dot"`
}

func (req CreateCollectionRequest) Validate() error {
	if len(req.Id) < 3 || len(req.Id) > 16 {
		return fmt.Errorf("id must be between 3 and 16 characters")
	}
	// Check alphanum
	for _, r := range req.Id {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return fmt.Errorf("id must be alphanumeric")
		}
	}
	if req.VectorSize < 1 || req.VectorSize > 4096 {
		return fmt.Errorf("vectorSize must be between 1 and 4096, got %d", req.VectorSize)
	}
	if req.DistanceMetric != models.DistanceEuclidean && req.DistanceMetric != models.DistanceCosine && req.DistanceMetric != models.DistanceDot {
		return fmt.Errorf("distanceMetric must be one of euclidean, cosine, dot, got %s", req.DistanceMetric)
	}
	return nil
}

func (sdbh *SemaDBHandlers) HandleCreateCollection(w http.ResponseWriter, r *http.Request) {
	req, err := utils.DecodeValid[CreateCollectionRequest](r)
	if err != nil {
		utils.Encode(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	appHeaders := middleware.GetAppHeaders(r.Context())
	userPlan := middleware.GetUserPlan(r.Context())
	// ---------------------------
	vamanaCollection := models.Collection{
		UserId:    appHeaders.UserId,
		Id:        req.Id,
		Replicas:  1,
		Timestamp: time.Now().Unix(),
		CreatedAt: time.Now().Unix(),
		UserPlan:  userPlan,
		IndexSchema: models.IndexSchema{
			"vector": {
				Type: "vectorVamana",
				VectorVamana: &models.IndexVectorVamanaParameters{
					VectorSize:     req.VectorSize,
					DistanceMetric: req.DistanceMetric,
					// Default values for the vamana algorithm
					SearchSize:  75,
					DegreeBound: 64,
					Alpha:       1.2,
				},
			},
		},
	}
	log.Debug().Interface("collection", vamanaCollection).Msg("CreateCollection")
	// ---------------------------
	switch err := sdbh.clusterNode.CreateCollection(vamanaCollection); err {
	case nil:
		utils.Encode(w, http.StatusOK, map[string]string{"message": "collection created"})
	case cluster.ErrQuotaReached:
		utils.Encode(w, http.StatusForbidden, map[string]string{"error": "quota reached"})
	case cluster.ErrExists:
		utils.Encode(w, http.StatusConflict, map[string]string{"error": "collection exists"})
	default:
		utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		log.Error().Err(err).Str("id", vamanaCollection.Id).Msg("CreateCollection failed")
	}
}

type ListCollectionItem struct {
	Id             string `json:"id"`
	VectorSize     uint   `json:"vectorSize"`
	DistanceMetric string `json:"distanceMetric"`
}

type ListCollectionsResponse struct {
	Collections []ListCollectionItem `json:"collections"`
}

func (sdbh *SemaDBHandlers) HandleListCollections(w http.ResponseWriter, r *http.Request) {
	appHeaders := middleware.GetAppHeaders(r.Context())
	// ---------------------------
	collections, err := sdbh.clusterNode.ListCollections(appHeaders.UserId)
	if err != nil {
		utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		log.Error().Err(err).Msg("ListCollections failed")
		return
	}
	colItems := make([]ListCollectionItem, len(collections))
	for i, col := range collections {
		colItems[i] = ListCollectionItem{Id: col.Id, VectorSize: col.IndexSchema["vector"].VectorVamana.VectorSize, DistanceMetric: col.IndexSchema["vector"].VectorVamana.DistanceMetric}
	}
	resp := ListCollectionsResponse{Collections: colItems}
	utils.Encode(w, http.StatusOK, resp)
	// ---------------------------
}

// ---------------------------

type contextKey string

const collectionContextKey contextKey = "collection"

// Extracts collectionId from the URI and fetches the collection from the cluster.
func (sdbh *SemaDBHandlers) CollectionURIMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collectionId := r.PathValue("collectionId")
		if len(collectionId) < 3 || len(collectionId) > 16 {
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": "collectionId must be between 3 and 16 characters"})
			return
		}
		appHeaders := middleware.GetAppHeaders(r.Context())
		collection, err := sdbh.clusterNode.GetCollection(appHeaders.UserId, collectionId)
		if err == cluster.ErrNotFound {
			errMsg := fmt.Sprintf("collection %s not found", collectionId)
			utils.Encode(w, http.StatusNotFound, map[string]string{"error": errMsg})
			return
		}
		if err != nil {
			utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		// ---------------------------
		// Bind active user plan regardless of what is saved in the collection.
		// This is because the user plan might change and we want the latest
		// active one rather than the one saved in the collection. This means
		// any downstream operation will use the latest user plan.
		collection.UserPlan = middleware.GetUserPlan(r.Context())
		// ---------------------------
		newCtx := context.WithValue(r.Context(), collectionContextKey, collection)
		next.ServeHTTP(w, r.WithContext(newCtx))
	})
}

// ---------------------------

type ShardItem struct {
	Id         string `json:"id"`
	PointCount int64  `json:"pointCount"`
}

type GetCollectionResponse struct {
	Id             string      `json:"id"`
	VectorSize     uint        `json:"vectorSize"`
	DistanceMetric string      `json:"distanceMetric"`
	Shards         []ShardItem `json:"shards"`
}

func (sdbh *SemaDBHandlers) HandleGetCollection(w http.ResponseWriter, r *http.Request) {
	// ---------------------------
	collection := r.Context().Value(collectionContextKey).(models.Collection)
	// ---------------------------
	shards, err := sdbh.clusterNode.GetShardsInfo(collection)
	if errors.Is(err, cluster.ErrShardUnavailable) {
		utils.Encode(w, http.StatusServiceUnavailable, map[string]string{"error": "one or more shards are unavailable"})
		return
	}
	if err != nil {
		utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// ---------------------------
	shardItems := make([]ShardItem, len(shards))
	for i, shard := range shards {
		shardItems[i] = ShardItem{Id: shard.Id, PointCount: shard.PointCount}
	}
	resp := GetCollectionResponse{
		Id:             collection.Id,
		VectorSize:     collection.IndexSchema["vector"].VectorVamana.VectorSize,
		DistanceMetric: collection.IndexSchema["vector"].VectorVamana.DistanceMetric,
		Shards:         shardItems,
	}
	utils.Encode(w, http.StatusOK, resp)
}

// ---------------------------

func (sdbh *SemaDBHandlers) HandleDeleteCollection(w http.ResponseWriter, r *http.Request) {
	// ---------------------------
	collection := r.Context().Value(collectionContextKey).(models.Collection)
	// ---------------------------
	deletedShardIds, err := sdbh.clusterNode.DeleteCollection(collection)
	if err != nil {
		utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	status := http.StatusOK
	if len(deletedShardIds) != len(collection.ShardIds) {
		status = http.StatusAccepted
	}
	utils.Encode(w, status, map[string]string{"message": "collection deleted"})
}

// ---------------------------

type InsertSinglePointRequest struct {
	Id       string    `json:"id" binding:"omitempty,uuid"`
	Vector   []float32 `json:"vector" binding:"required,max=2000"`
	Metadata any       `json:"metadata"`
}

func (req InsertSinglePointRequest) Validate() error {
	if len(req.Id) > 0 {
		if _, err := uuid.Parse(req.Id); err != nil {
			return fmt.Errorf("id must be a valid uuid")
		}
	}
	if len(req.Vector) < 1 || len(req.Vector) > 2000 {
		return fmt.Errorf("vector size must be between 1 and 2000, got %d", len(req.Vector))
	}
	return nil
}

type InsertPointsRequest struct {
	Points []InsertSinglePointRequest `json:"points" binding:"required,max=10000,dive"`
}

func (req InsertPointsRequest) Validate() error {
	if len(req.Points) < 1 || len(req.Points) > 10000 {
		return fmt.Errorf("points size must be between 1 and 10000, got %d", len(req.Points))
	}
	for i, point := range req.Points {
		if err := point.Validate(); err != nil {
			return fmt.Errorf("point at index %d: %w", i, err)
		}
	}
	return nil
}

type InsertPointsResponse struct {
	Message      string                `json:"message"`
	FailedRanges []cluster.FailedRange `json:"failedRanges"`
}

func (sdbh *SemaDBHandlers) HandleInsertPoints(w http.ResponseWriter, r *http.Request) {
	// ---------------------------
	startTime := time.Now()
	req, err := utils.DecodeValid[InsertPointsRequest](r)
	if err != nil {
		utils.Encode(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	log.Debug().Str("bindTime", time.Since(startTime).String()).Msg("InsertPoints bind")
	// ---------------------------
	// Get corresponding collection
	collection := r.Context().Value(collectionContextKey).(models.Collection)
	// ---------------------------
	// Convert request points into internal points, doing checks along the way
	points := make([]models.Point, len(req.Points))
	for i, point := range req.Points {
		if len(point.Vector) != int(collection.IndexSchema["vector"].VectorVamana.VectorSize) {
			errMsg := fmt.Sprintf("invalid vector dimension, expected %d got %d for point at index %d", collection.IndexSchema["vector"].VectorVamana.VectorSize, len(point.Vector), i)
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		pointId := uuid.New()
		if len(point.Id) > 0 {
			pointId = uuid.MustParse(point.Id)
		}
		pointData := models.PointAsMap{"vector": point.Vector, "metadata": point.Metadata}
		binaryPointData, err := msgpack.Marshal(pointData)
		if err != nil {
			errMsg := fmt.Sprintf("failed to JSON encode point at index %d, please ensure all fields are JSON compatible", i)
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		if len(binaryPointData) > collection.UserPlan.MaxPointSize {
			errMsg := fmt.Sprintf("point %d exceeds maximum point size %d > %d", i, len(binaryPointData), collection.UserPlan.MaxPointSize)
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		points[i] = models.Point{
			Id:   pointId,
			Data: binaryPointData,
		}
	}
	// ---------------------------
	// Insert points returns a range of errors for failed shards
	failedRanges, err := sdbh.clusterNode.InsertPoints(collection, points)
	if errors.Is(err, cluster.ErrQuotaReached) {
		utils.Encode(w, http.StatusForbidden, map[string]string{"error": "quota reached"})
		return
	}
	if errors.Is(err, cluster.ErrShardUnavailable) {
		utils.Encode(w, http.StatusServiceUnavailable, map[string]string{"error": "one or more shards are unavailable"})
		return
	}
	if err != nil {
		utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	resp := InsertPointsResponse{Message: "success", FailedRanges: failedRanges}
	if len(failedRanges) > 0 {
		resp.Message = "partial success"
	}
	utils.Encode(w, http.StatusOK, resp)
	// ---------------------------
}

// ---------------------------

type UpdateSinglePointRequest struct {
	Id       string    `json:"id" binding:"required,uuid"`
	Vector   []float32 `json:"vector" binding:"required,max=2000"`
	Metadata any       `json:"metadata"`
}

func (req UpdateSinglePointRequest) Validate() error {
	if _, err := uuid.Parse(req.Id); err != nil {
		return fmt.Errorf("id must be a valid uuid, got %s", req.Id)
	}
	if len(req.Vector) < 1 || len(req.Vector) > 2000 {
		return fmt.Errorf("vector size must be between 1 and 2000, got %d", len(req.Vector))
	}
	return nil
}

type UpdatePointsRequest struct {
	Points []UpdateSinglePointRequest `json:"points" binding:"required,max=100,dive"`
}

func (req UpdatePointsRequest) Validate() error {
	if len(req.Points) < 1 || len(req.Points) > 100 {
		return fmt.Errorf("points size must be between 1 and 100, got %d", len(req.Points))
	}
	for i, point := range req.Points {
		if err := point.Validate(); err != nil {
			return fmt.Errorf("point at index %d: %w", i, err)
		}
	}
	return nil
}

type UpdatePointsResponse struct {
	Message      string                `json:"message"`
	FailedPoints []cluster.FailedPoint `json:"failedPoints"`
}

func (sdbh *SemaDBHandlers) HandleUpdatePoints(w http.ResponseWriter, r *http.Request) {
	// ---------------------------
	req, err := utils.DecodeValid[UpdatePointsRequest](r)
	if err != nil {
		utils.Encode(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	// ---------------------------
	// Get corresponding collection
	collection := r.Context().Value(collectionContextKey).(models.Collection)
	// ---------------------------
	// Convert request points into internal points, doing checks along the way
	points := make([]models.Point, len(req.Points))
	for i, point := range req.Points {
		if len(point.Vector) != int(collection.IndexSchema["vector"].VectorVamana.VectorSize) {
			errMsg := fmt.Sprintf("invalid vector dimension, expected %d got %d for point at index %d", collection.IndexSchema["vector"].VectorVamana.VectorSize, len(point.Vector), i)
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		points[i] = models.Point{
			Id: uuid.MustParse(point.Id),
		}
		pointData := models.PointAsMap{"vector": point.Vector, "metadata": point.Metadata}
		binaryPointData, err := msgpack.Marshal(pointData)
		if err != nil {
			errMsg := fmt.Sprintf("failed to JSON encode %d, please ensure all fields are JSON compatible", i)
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		if len(binaryPointData) > collection.UserPlan.MaxPointSize {
			errMsg := fmt.Sprintf("point %d exceeds maximum point size %d > %d", i, len(binaryPointData), collection.UserPlan.MaxPointSize)
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		points[i].Data = binaryPointData
	}
	// ---------------------------
	// Update points returns a list of failed points
	failedPoints, err := sdbh.clusterNode.UpdatePoints(collection, points)
	if err != nil {
		utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	resp := UpdatePointsResponse{Message: "success", FailedPoints: failedPoints}
	if len(failedPoints) > 0 {
		resp.Message = "partial success"
	}
	utils.Encode(w, http.StatusOK, resp)
}

// ---------------------------

type DeletePointsRequest struct {
	Ids []string `json:"ids" binding:"required,max=100,dive,uuid"`
}

func (req DeletePointsRequest) Validate() error {
	if len(req.Ids) < 1 || len(req.Ids) > 100 {
		return fmt.Errorf("ids size must be between 1 and 100, got %d", len(req.Ids))
	}
	for i, id := range req.Ids {
		if _, err := uuid.Parse(id); err != nil {
			return fmt.Errorf("id at index %d must be a valid uuid", i)
		}
	}
	return nil
}

type DeletePointsResponse struct {
	Message      string                `json:"message"`
	FailedPoints []cluster.FailedPoint `json:"failedPoints"`
}

func (sdbh *SemaDBHandlers) HandleDeletePoints(w http.ResponseWriter, r *http.Request) {
	// ---------------------------
	req, err := utils.DecodeValid[DeletePointsRequest](r)
	if err != nil {
		utils.Encode(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	// ---------------------------
	// Convert request ids into uuids
	pointIds := make([]uuid.UUID, len(req.Ids))
	for i, id := range req.Ids {
		pointIds[i] = uuid.MustParse(id)
	}
	// ---------------------------
	// Get corresponding collection
	collection := r.Context().Value(collectionContextKey).(models.Collection)
	// ---------------------------
	failedPoints, err := sdbh.clusterNode.DeletePoints(collection, pointIds)
	if err != nil {
		utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	resp := DeletePointsResponse{Message: "success", FailedPoints: failedPoints}
	if len(failedPoints) > 0 {
		resp.Message = "partial success"
	}
	utils.Encode(w, http.StatusOK, resp)
}

// ---------------------------

type SearchPointsRequest struct {
	Vector []float32 `json:"vector" binding:"required,max=2000"`
	Limit  int       `json:"limit" binding:"min=0,max=75"`
}

func (req SearchPointsRequest) Validate() error {
	if len(req.Vector) < 1 || len(req.Vector) > 2000 {
		return fmt.Errorf("vector size must be between 1 and 2000")
	}
	if req.Limit < 0 || req.Limit > 75 {
		return fmt.Errorf("limit must be between 0 and 75")
	}
	return nil
}

type SearchPointResult struct {
	Id       string  `json:"id"`
	Distance float32 `json:"distance"`
	Metadata any     `json:"metadata"`
}

type SearchPointsResponse struct {
	Points []SearchPointResult `json:"points"`
}

func (sdbh *SemaDBHandlers) HandleSearchPoints(w http.ResponseWriter, r *http.Request) {
	// ---------------------------
	req, err := utils.DecodeValid[SearchPointsRequest](r)
	if err != nil {
		utils.Encode(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	// Default limit is 10
	if req.Limit == 0 {
		req.Limit = 10
	}
	// ---------------------------
	// Get corresponding collection
	collection := r.Context().Value(collectionContextKey).(models.Collection)
	// ---------------------------
	// Check vector dimension
	if len(req.Vector) != int(collection.IndexSchema["vector"].VectorVamana.VectorSize) {
		errMsg := fmt.Sprintf("invalid vector dimension, expected %d got %d", collection.IndexSchema["vector"].VectorVamana.VectorSize, len(req.Vector))
		utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
		return
	}
	// ---------------------------
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "vector",
			VectorVamana: &models.SearchVectorVamanaOptions{
				Vector:     req.Vector,
				SearchSize: 75, // Default search size
				Limit:      req.Limit,
				Operator:   "near",
			},
		},
		Select: []string{"metadata"},
		Limit:  req.Limit,
	}
	points, err := sdbh.clusterNode.SearchPoints(collection, sr)
	if err != nil {
		utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	results := make([]SearchPointResult, len(points))
	for i, sp := range points {
		mdata := sp.DecodedData["metadata"]
		var dist float32
		if sp.Distance != nil {
			dist = *sp.Distance
		}
		results[i] = SearchPointResult{
			Id:       sp.Point.Id.String(),
			Distance: dist,
			Metadata: mdata,
		}
	}
	resp := SearchPointsResponse{Points: results}
	utils.Encode(w, http.StatusOK, resp)
	// ---------------------------
}
