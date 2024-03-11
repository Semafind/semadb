package v1

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/httpapi/middleware"
	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack/v5"
)

func pongHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong from semadb",
	})
}

// ---------------------------

type SemaDBHandlers struct {
	clusterNode *cluster.ClusterNode
}

// Requires middleware.AppHeaderMiddleware to be used
func SetupV1Handlers(clusterNode *cluster.ClusterNode, rgroup *gin.RouterGroup) {
	rgroup.GET("/ping", pongHandler)
	semaDBHandlers := &SemaDBHandlers{clusterNode: clusterNode}
	// https://stackoverflow.blog/2020/03/02/best-practices-for-rest-api-design/
	rgroup.POST("/collections", semaDBHandlers.CreateCollection)
	rgroup.GET("/collections", semaDBHandlers.ListCollections)
	colRoutes := rgroup.Group("/collections/:collectionId", semaDBHandlers.CollectionURIMiddleware())
	colRoutes.GET("", semaDBHandlers.GetCollection)
	colRoutes.DELETE("", semaDBHandlers.DeleteCollection)
	// We're batching point requests for peformance reasons. Alternatively we
	// can provide points/:pointId endpoint in the future.
	colRoutes.POST("/points", semaDBHandlers.InsertPoints)
	colRoutes.PUT("/points", semaDBHandlers.UpdatePoints)
	colRoutes.DELETE("/points", semaDBHandlers.DeletePoints)
	colRoutes.POST("/points/search", semaDBHandlers.SearchPoints)
}

// ---------------------------

type CreateCollectionRequest struct {
	Id             string `json:"id" binding:"required,alphanum,min=3,max=16"`
	VectorSize     uint   `json:"vectorSize" binding:"required"`
	DistanceMetric string `json:"distanceMetric" binding:"required,oneof=euclidean cosine dot"`
}

func (sdbh *SemaDBHandlers) CreateCollection(c *gin.Context) {
	var req CreateCollectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	appHeaders := c.MustGet("appHeaders").(middleware.AppHeaders)
	// ---------------------------
	vamanaCollection := models.Collection{
		UserId:    appHeaders.UserId,
		Id:        req.Id,
		Replicas:  1,
		Timestamp: time.Now().UnixMicro(),
		CreatedAt: time.Now().UnixMicro(),
		UserPlan:  c.MustGet("userPlan").(models.UserPlan),
		IndexSchema: models.IndexSchema{
			VectorVamana: map[string]models.IndexVectorVamanaParameters{
				"vector": {
					IndexVectorFlatParameters: models.IndexVectorFlatParameters{
						VectorSize:     req.VectorSize,
						DistanceMetric: req.DistanceMetric,
					},
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
	err := sdbh.clusterNode.CreateCollection(vamanaCollection)
	switch err {
	case nil:
		c.JSON(http.StatusOK, gin.H{"message": "collection created"})
	case cluster.ErrQuotaReached:
		c.JSON(http.StatusForbidden, gin.H{"error": "quota reached"})
	case cluster.ErrExists:
		c.JSON(http.StatusConflict, gin.H{"error": "collection exists"})
	default:
		c.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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

func (sdbh *SemaDBHandlers) ListCollections(c *gin.Context) {
	appHeaders := c.MustGet("appHeaders").(middleware.AppHeaders)
	// ---------------------------
	collections, err := sdbh.clusterNode.ListCollections(appHeaders.UserId)
	if err != nil {
		c.Error(err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		log.Error().Err(err).Msg("ListCollections failed")
		return
	}
	colItems := make([]ListCollectionItem, len(collections))
	for i, col := range collections {
		colItems[i] = ListCollectionItem{Id: col.Id, VectorSize: col.IndexSchema.VectorVamana["vector"].VectorSize, DistanceMetric: col.IndexSchema.VectorVamana["vector"].DistanceMetric}
	}
	resp := ListCollectionsResponse{Collections: colItems}
	c.JSON(http.StatusOK, resp)
	// ---------------------------
}

// ---------------------------

type GetCollectionUri struct {
	CollectionId string `uri:"collectionId" binding:"required,alphanum,min=3,max=16"`
}

func (sdbh *SemaDBHandlers) CollectionURIMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		var uri GetCollectionUri
		if err := c.ShouldBindUri(&uri); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		appHeaders := c.MustGet("appHeaders").(middleware.AppHeaders)
		collection, err := sdbh.clusterNode.GetCollection(appHeaders.UserId, uri.CollectionId)
		if err == cluster.ErrNotFound {
			errMsg := fmt.Sprintf("collection %s not found", uri.CollectionId)
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": errMsg})
			return
		}
		if err != nil {
			c.Error(err)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// ---------------------------
		// Bind active user plan regardless of what is saved in the collection.
		// This is because the user plan might change and we want the latest
		// active one rather than the one saved in the collection. This means
		// any downstream operation will use the latest user plan.
		collection.UserPlan = c.MustGet("userPlan").(models.UserPlan)
		// ---------------------------
		c.Set("collection", collection)
	}
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

func (sdbh *SemaDBHandlers) GetCollection(c *gin.Context) {
	// ---------------------------
	collection := c.MustGet("collection").(models.Collection)
	// ---------------------------
	shards, err := sdbh.clusterNode.GetShardsInfo(collection)
	if errors.Is(err, cluster.ErrShardUnavailable) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "one or more shards are unavailable"})
		return
	}
	if err != nil {
		c.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	shardItems := make([]ShardItem, len(shards))
	for i, shard := range shards {
		shardItems[i] = ShardItem{Id: shard.Id, PointCount: shard.PointCount}
	}
	resp := GetCollectionResponse{
		Id:             collection.Id,
		VectorSize:     collection.IndexSchema.VectorVamana["vector"].VectorSize,
		DistanceMetric: collection.IndexSchema.VectorVamana["vector"].DistanceMetric,
		Shards:         shardItems,
	}
	c.JSON(http.StatusOK, resp)
}

// ---------------------------

func (sdbh *SemaDBHandlers) DeleteCollection(c *gin.Context) {
	// ---------------------------
	collection := c.MustGet("collection").(models.Collection)
	// ---------------------------
	deletedShardIds, err := sdbh.clusterNode.DeleteCollection(collection)
	if err != nil {
		c.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	status := http.StatusOK
	if len(deletedShardIds) != len(collection.ShardIds) {
		status = http.StatusAccepted
	}
	c.JSON(status, gin.H{"message": "collection deleted"})
}

// ---------------------------

type InsertSinglePointRequest struct {
	Id       string    `json:"id" binding:"omitempty,uuid"`
	Vector   []float32 `json:"vector" binding:"required,max=2000"`
	Metadata any       `json:"metadata"`
}

type InsertPointsRequest struct {
	Points []InsertSinglePointRequest `json:"points" binding:"required,max=10000,dive"`
}

type InsertPointsResponse struct {
	Message      string                `json:"message"`
	FailedRanges []cluster.FailedRange `json:"failedRanges"`
}

func (sdbh *SemaDBHandlers) InsertPoints(c *gin.Context) {
	// ---------------------------
	var req InsertPointsRequest
	startTime := time.Now()
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	log.Debug().Str("bindTime", time.Since(startTime).String()).Msg("InsertPoints bind")
	// ---------------------------
	// Get corresponding collection
	collection := c.MustGet("collection").(models.Collection)
	// ---------------------------
	// Convert request points into internal points, doing checks along the way
	points := make([]models.Point, len(req.Points))
	for i, point := range req.Points {
		if len(point.Vector) != int(collection.IndexSchema.VectorVamana["vector"].VectorSize) {
			errMsg := fmt.Sprintf("invalid vector dimension, expected %d got %d for point at index %d", collection.IndexSchema.VectorVamana["vector"].VectorSize, len(point.Vector), i)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		pointId := uuid.New()
		if len(point.Id) > 0 {
			pointId = uuid.MustParse(point.Id)
		}
		pointData := map[string]any{"vector": point.Vector, "metadata": point.Metadata}
		binaryPointData, err := msgpack.Marshal(pointData)
		if err != nil {
			errMsg := fmt.Sprintf("failed to JSON encode point at index %d, please ensure all fields are JSON compatible", i)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		if len(binaryPointData) > collection.UserPlan.MaxMetadataSize {
			errMsg := fmt.Sprintf("point %d exceeds maximum metadata size %d > %d", i, len(binaryPointData), collection.UserPlan.MaxMetadataSize)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
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
		c.JSON(http.StatusForbidden, gin.H{"error": "quota reached"})
		return
	}
	if errors.Is(err, cluster.ErrShardUnavailable) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "one or more shards are unavailable"})
		return
	}
	if err != nil {
		c.Error(err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	resp := InsertPointsResponse{Message: "success", FailedRanges: failedRanges}
	if len(failedRanges) > 0 {
		resp.Message = "partial success"
	}
	c.JSON(http.StatusOK, resp)
	// ---------------------------
}

// ---------------------------

type UpdateSinglePointRequest struct {
	Id       string    `json:"id" binding:"required,uuid"`
	Vector   []float32 `json:"vector" binding:"required,max=2000"`
	Metadata any       `json:"metadata"`
}

type UpdatePointsRequest struct {
	Points []UpdateSinglePointRequest `json:"points" binding:"required,max=100,dive"`
}

func (sdbh *SemaDBHandlers) UpdatePoints(c *gin.Context) {
	// ---------------------------
	var req UpdatePointsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	// Get corresponding collection
	collection := c.MustGet("collection").(models.Collection)
	// ---------------------------
	// Convert request points into internal points, doing checks along the way
	points := make([]models.Point, len(req.Points))
	for i, point := range req.Points {
		if len(point.Vector) != int(collection.IndexSchema.VectorVamana["vector"].VectorSize) {
			errMsg := fmt.Sprintf("invalid vector dimension, expected %d got %d for point at index %d", collection.IndexSchema.VectorVamana["vector"].VectorSize, len(point.Vector), i)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		points[i] = models.Point{
			Id: uuid.MustParse(point.Id),
		}
		pointData := map[string]any{"vector": point.Vector, "metadata": point.Metadata}
		// TODO: Handle max point size for update case
		binaryPointData, err := msgpack.Marshal(pointData)
		if err != nil {
			errMsg := fmt.Sprintf("failed to JSON encode %d, please ensure all fields are JSON compatible", i)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		if len(binaryPointData) > collection.UserPlan.MaxMetadataSize {
			errMsg := fmt.Sprintf("point %d exceeds maximum metadata size %d > %d", i, len(binaryPointData), collection.UserPlan.MaxMetadataSize)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		points[i].Data = binaryPointData
	}
	// ---------------------------
	// Update points returns a list of failed points
	failedPoints, err := sdbh.clusterNode.UpdatePoints(collection, points)
	if err != nil {
		c.Error(err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	message := "success"
	if len(failedPoints) > 0 {
		message = "partial success"
	}
	c.JSON(http.StatusOK, gin.H{"message": message, "failedPoints": failedPoints})
}

// ---------------------------

type DeletePointsRequest struct {
	Ids []string `json:"ids" binding:"required,max=100,dive,uuid"`
}

func (sdbh *SemaDBHandlers) DeletePoints(c *gin.Context) {
	// ---------------------------
	var req DeletePointsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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
	collection := c.MustGet("collection").(models.Collection)
	// ---------------------------
	failedPoints, err := sdbh.clusterNode.DeletePoints(collection, pointIds)
	if err != nil {
		c.Error(err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	message := "success"
	if len(failedPoints) > 0 {
		message = "partial success"
	}
	c.JSON(http.StatusOK, gin.H{"message": message, "failedPoints": failedPoints})
}

// ---------------------------

type SearchPointsRequest struct {
	Vector []float32 `json:"vector" binding:"required,max=2000"`
	Limit  int       `json:"limit" binding:"min=0,max=75"`
}

type SearchPointResult struct {
	Id       string  `json:"id"`
	Distance float32 `json:"distance"`
	Metadata any     `json:"metadata"`
}

type SearchPointsResponse struct {
	Points []SearchPointResult `json:"points"`
}

func (sdbh *SemaDBHandlers) SearchPoints(c *gin.Context) {
	// ---------------------------
	var req SearchPointsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// Default limit is 10
	if req.Limit == 0 {
		req.Limit = 10
	}
	// ---------------------------
	// Get corresponding collection
	collection := c.MustGet("collection").(models.Collection)
	// ---------------------------
	// Check vector dimension
	if len(req.Vector) != int(collection.IndexSchema.VectorVamana["vector"].VectorSize) {
		errMsg := fmt.Sprintf("invalid vector dimension, expected %d got %d", collection.IndexSchema.VectorVamana["vector"].VectorSize, len(req.Vector))
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return
	}
	// ---------------------------
	points, err := sdbh.clusterNode.SearchPoints(collection, req.Vector, req.Limit)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	results := make([]SearchPointResult, len(points))
	for i, sp := range points {
		mdata, err := sp.Point.GetField("metadata")
		if err != nil {
			errMsg := fmt.Sprintf("failed to get metadata for point %s: %s", sp.Point.Id, err)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": errMsg})
			return
		}
		results[i] = SearchPointResult{
			Id:       sp.Point.Id.String(),
			Distance: sp.Distance,
			Metadata: mdata,
		}
	}
	resp := SearchPointsResponse{Points: results}
	c.JSON(http.StatusOK, resp)
	// ---------------------------
}
