package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack/v5"
)

type SemaDBHandlers struct {
	clusterNode *cluster.ClusterNode
}

func NewSemaDBHandlers(clusterNode *cluster.ClusterNode) *SemaDBHandlers {
	return &SemaDBHandlers{clusterNode: clusterNode}
}

// ---------------------------

type NewCollectionRequest struct {
	Id             string `json:"id" binding:"required,alphanum,min=3,max=16"`
	VectorSize     uint   `json:"vectorSize" binding:"required"`
	DistanceMetric string `json:"distanceMetric" binding:"required,oneof=euclidean cosine"`
}

func (sdbh *SemaDBHandlers) NewCollection(c *gin.Context) {
	var req NewCollectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	appHeaders := c.MustGet("appHeaders").(AppHeaders)
	// ---------------------------
	vamanaCollection := models.Collection{
		UserId:     appHeaders.UserID,
		Id:         req.Id,
		VectorSize: req.VectorSize,
		DistMetric: req.DistanceMetric,
		Replicas:   1,
		Algorithm:  "vamana",
		Timestamp:  time.Now().UnixMicro(),
		CreatedAt:  time.Now().UnixMicro(),
		Parameters: models.DefaultVamanaParameters(),
	}
	log.Debug().Interface("collection", vamanaCollection).Msg("NewCollection")
	// ---------------------------
	err := sdbh.clusterNode.CreateCollection(vamanaCollection)
	switch err {
	case nil:
		c.JSON(http.StatusOK, gin.H{"message": "collection created"})
	case cluster.ErrExists:
		c.JSON(http.StatusConflict, gin.H{"error": "collection exists"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
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
	appHeaders := c.MustGet("appHeaders").(AppHeaders)
	// ---------------------------
	collections, err := sdbh.clusterNode.ListCollections(appHeaders.UserID)
	switch err {
	case nil:
		colItems := make([]ListCollectionItem, len(collections))
		for i, col := range collections {
			colItems[i] = ListCollectionItem{Id: col.Id, VectorSize: col.VectorSize, DistanceMetric: col.DistMetric}
		}
		resp := ListCollectionsResponse{Collections: colItems}
		c.JSON(http.StatusOK, resp)
	case cluster.ErrNotFound:
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		log.Error().Err(err).Msg("ListCollections failed")
	}
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
		appHeaders := c.MustGet("appHeaders").(AppHeaders)
		collection, err := sdbh.clusterNode.GetCollection(appHeaders.UserID, uri.CollectionId)
		if err == cluster.ErrNotFound {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		c.Set("collection", collection)
		c.Next()
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
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "shard unavailable"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	// ---------------------------
	shardItems := make([]ShardItem, len(shards))
	for i, shard := range shards {
		shardItems[i] = ShardItem{Id: shard.Id, PointCount: shard.PointCount}
	}
	resp := GetCollectionResponse{
		Id:             collection.Id,
		VectorSize:     collection.VectorSize,
		DistanceMetric: collection.DistMetric,
		Shards:         shardItems,
	}
	c.JSON(http.StatusOK, resp)
}

// ---------------------------

func (sdbh *SemaDBHandlers) DeleteCollection(c *gin.Context) {
	// Not implemented
	c.AbortWithStatusJSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
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

func (sdbh *SemaDBHandlers) InsertPoints(c *gin.Context) {
	userPlan := c.MustGet("userPlan").(UserPlan)
	// ---------------------------
	var req InsertPointsRequest
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
		if len(point.Vector) != int(collection.VectorSize) {
			errMsg := fmt.Sprintf("invalid vector dimension, expected %d got %d for point at index %d", collection.VectorSize, len(point.Vector), i)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		points[i] = models.Point{
			Id:     uuid.New(),
			Vector: point.Vector,
		}
		if point.Metadata != nil {
			binaryMetadata, err := msgpack.Marshal(point.Metadata)
			if err != nil {
				errMsg := fmt.Sprintf("invalid metadata for point %d", i)
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
				return
			}
			if len(binaryMetadata) > userPlan.MaxMetadataSize {
				errMsg := fmt.Sprintf("point %d exceeds maximum metadata size %d > %d", i, len(binaryMetadata), userPlan.MaxMetadataSize)
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
				return
			}
			points[i].Metadata = binaryMetadata
		}
	}
	// ---------------------------
	// Insert points returns a range of errors for failed shards
	errRanges, err := sdbh.clusterNode.InsertPoints(collection, points)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(errRanges) > 0 {
		c.AbortWithStatusJSON(http.StatusAccepted, gin.H{"message": "partial success", "failed": errRanges})
		return
	}
	c.Status(http.StatusOK)
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
	userPlan := c.MustGet("userPlan").(UserPlan)
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
		if len(point.Vector) != int(collection.VectorSize) {
			errMsg := fmt.Sprintf("invalid vector dimension, expected %d got %d for point at index %d", collection.VectorSize, len(point.Vector), i)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		points[i] = models.Point{
			Id:     uuid.MustParse(point.Id),
			Vector: point.Vector,
		}
		if point.Metadata != nil {
			binaryMetadata, err := msgpack.Marshal(point.Metadata)
			if err != nil {
				errMsg := fmt.Sprintf("invalid metadata for point %d", i)
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
				return
			}
			if len(binaryMetadata) > userPlan.MaxMetadataSize {
				errMsg := fmt.Sprintf("point %d exceeds maximum metadata size %d > %d", i, len(binaryMetadata), userPlan.MaxMetadataSize)
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
				return
			}
			points[i].Metadata = binaryMetadata
		}
	}
	// ---------------------------
	// Update points returns a list of failed point ids
	failedIds, err := sdbh.clusterNode.UpdatePoints(collection, points)
	if err != nil {
		c.Error(err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(failedIds) > 0 {
		c.AbortWithStatusJSON(http.StatusAccepted, gin.H{"message": "partial success", "failedIds": failedIds})
		return
	}
	c.Status(http.StatusOK)
}

// ---------------------------

func (sdbh *SemaDBHandlers) DeletePoints(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
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
	if len(req.Vector) != int(collection.VectorSize) {
		errMsg := fmt.Sprintf("invalid vector dimension, expected %d got %d", collection.VectorSize, len(req.Vector))
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
		var mdata any
		if sp.Point.Metadata != nil {
			if err := msgpack.Unmarshal(sp.Point.Metadata, &mdata); err != nil {
				log.Err(err).Interface("meta", sp.Point.Metadata).Msg("msgpack.Unmarshal failed")
			}
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
