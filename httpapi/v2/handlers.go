package v2

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
func SetupV2Handlers(clusterNode *cluster.ClusterNode) http.Handler {
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
	Id          string             `json:"id" binding:"required,alphanum,min=3,max=24"`
	IndexSchema models.IndexSchema `json:"indexSchema" binding:"required,dive"`
}

func (req CreateCollectionRequest) Validate() error {
	if len(req.Id) < 3 || len(req.Id) > 24 {
		return fmt.Errorf("id must be between 3 and 24 characters, got %d", len(req.Id))
	}
	// Check alpanum
	for _, r := range req.Id {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return fmt.Errorf("id must be alphanumeric, got %s", req.Id)
		}
	}
	// Validate index schema
	return req.IndexSchema.Validate()
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
		UserId:      appHeaders.UserId,
		Id:          req.Id,
		Replicas:    1,
		Timestamp:   time.Now().Unix(),
		CreatedAt:   time.Now().Unix(),
		UserPlan:    userPlan,
		IndexSchema: req.IndexSchema,
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
	Id string `json:"id"`
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
		colItems[i] = ListCollectionItem{Id: col.Id}
	}
	resp := ListCollectionsResponse{Collections: colItems}
	utils.Encode(w, http.StatusOK, resp)
	// ---------------------------
}

// ---------------------------

type contextKey string

const collectionContextKey contextKey = "collection"

func (sdbh *SemaDBHandlers) CollectionURIMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collectionId := r.PathValue("collectionId")
		if len(collectionId) < 3 || len(collectionId) > 24 {
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": "collectionId must be between 3 and 24 characters"})
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
		newReq := r.WithContext(context.WithValue(r.Context(), collectionContextKey, collection))
		next.ServeHTTP(w, newReq)
	})
}

// ---------------------------

type ShardItem struct {
	Id         string `json:"id"`
	PointCount int64  `json:"pointCount"`
}

type GetCollectionResponse struct {
	Id          string             `json:"id"`
	IndexSchema models.IndexSchema `json:"indexSchema"`
	Shards      []ShardItem        `json:"shards"`
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
		Id:          collection.Id,
		IndexSchema: collection.IndexSchema,
		Shards:      shardItems,
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

type InsertPointsRequest struct {
	Points []models.PointAsMap `json:"points" binding:"required,max=10000"`
}

func (req InsertPointsRequest) Validate() error {
	if len(req.Points) < 1 || len(req.Points) > 10000 {
		return fmt.Errorf("number of points must be between 1 and 10000, got %d", len(req.Points))
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
		if err := collection.IndexSchema.CheckCompatibleMap(point); err != nil {
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		pointId, err := point.ExtractIdField(true)
		if err != nil {
			errMsg := fmt.Sprintf("invalid id for point %d, %s", i, err.Error())
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		points[i] = models.Point{Id: pointId}
		pointData, err := msgpack.Marshal(point)
		if err != nil {
			errMsg := fmt.Sprintf("invalid point data for point %d, %s", i, err.Error())
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		if len(pointData) > collection.UserPlan.MaxPointSize {
			errMsg := fmt.Sprintf("point %d exceeds maximum point size %d > %d", i, len(pointData), collection.UserPlan.MaxPointSize)
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		points[i].Data = pointData
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

type UpdatePointsRequest struct {
	Points []models.PointAsMap `json:"points" binding:"required,max=100"`
}

func (req UpdatePointsRequest) Validate() error {
	if len(req.Points) < 1 || len(req.Points) > 100 {
		return fmt.Errorf("number of points must be between 1 and 100, got %d", len(req.Points))
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
		pointId, err := point.ExtractIdField(false)
		if err != nil {
			errMsg := fmt.Sprintf("invalid id for point %d, %s", i, err.Error())
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		if err := collection.IndexSchema.CheckCompatibleMap(point); err != nil {
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		points[i] = models.Point{Id: pointId}
		pointData, err := msgpack.Marshal(point)
		if err != nil {
			errMsg := fmt.Sprintf("invalid point data for point %d, %s", i, err.Error())
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		if len(pointData) > collection.UserPlan.MaxPointSize {
			errMsg := fmt.Sprintf("point %d exceeds maximum point size %d > %d", i, len(pointData), collection.UserPlan.MaxPointSize)
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errMsg})
			return
		}
		points[i].Data = pointData
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
		return fmt.Errorf("number of ids must be between 1 and 100, got %d", len(req.Ids))
	}
	for i, id := range req.Ids {
		if _, err := uuid.Parse(id); err != nil {
			return fmt.Errorf("invalid uuid at index %d", i)
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

type SearchPointsResponse struct {
	Points []models.PointAsMap `json:"points"`
}

func (sdbh *SemaDBHandlers) HandleSearchPoints(w http.ResponseWriter, r *http.Request) {
	// ---------------------------
	req, err := utils.DecodeValid[models.SearchRequest](r)
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
	// Validate query against schema, checks vector dimensions, query options etc.
	if err := req.Query.ValidateSchema(collection.IndexSchema); err != nil {
		utils.Encode(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	// ---------------------------
	points, err := sdbh.clusterNode.SearchPoints(collection, req)
	if err != nil {
		utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	results := make([]models.PointAsMap, len(points))
	for i, sp := range points {
		pointData := sp.DecodedData
		if sp.DecodedData == nil {
			pointData = models.PointAsMap{}
			if len(sp.Point.Data) > 0 {
				if err := msgpack.Unmarshal(sp.Point.Data, &pointData); err != nil {
					errMsg := fmt.Sprintf("could not decode point %s", sp.Point.Id.String())
					utils.Encode(w, http.StatusInternalServerError, map[string]string{"error": errMsg})
					return
				}
			}
		}
		// ---------------------------
		// We re-add the system _id and _distance fields to the point data
		pointData["_id"] = sp.Point.Id.String()
		if sp.Distance != nil {
			pointData["_distance"] = *sp.Distance
		}
		if sp.Score != nil {
			pointData["_score"] = *sp.Score
		}
		pointData["_hybridScore"] = sp.HybridScore
		results[i] = pointData
	}
	resp := SearchPointsResponse{Points: results}
	utils.Encode(w, http.StatusOK, resp)
	// ---------------------------
}
