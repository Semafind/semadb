package main

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack/v5"
)

// ---------------------------

func pongHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong from semadb",
	})
}

// ---------------------------

type SemaDBHandlers struct {
	clusterNode *cluster.ClusterNode
}

func NewSemaDBHandlers(clusterNode *cluster.ClusterNode) *SemaDBHandlers {
	return &SemaDBHandlers{clusterNode: clusterNode}
}

// ---------------------------
/* Common headers */
type AppHeaders struct {
	UserID  string `header:"X-User-Id" binding:"required"`
	Package string `header:"X-Package" binding:"required"`
}

func AppHeaderMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		var appHeaders AppHeaders
		if err := c.ShouldBindHeader(&appHeaders); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.Set("appHeaders", appHeaders)
		log.Debug().Interface("appHeaders", appHeaders).Msg("AppHeaderMiddleware")
		// Extract user plan
		userPlan, ok := config.Cfg.UserPlans[appHeaders.Package]
		if !ok {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "unknown package"})
			return
		}
		c.Set("userPlan", userPlan)
		c.Next()
	}
}

// ---------------------------
/* Collection handlers */

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
		c.JSON(http.StatusCreated, gin.H{"message": "collection created"})
	case cluster.ErrExists:
		c.JSON(http.StatusConflict, gin.H{"error": "collection exists"})
	case cluster.ErrConflict:
		c.JSON(http.StatusConflict, gin.H{"error": "conflict"})
	case cluster.ErrTimeout:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "timeout"})
	case cluster.ErrPartialSuccess:
		c.JSON(http.StatusAccepted, gin.H{"message": "collection accepted"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}
}

func (sdbh *SemaDBHandlers) ListCollections(c *gin.Context) {
	appHeaders := c.MustGet("appHeaders").(AppHeaders)
	// ---------------------------
	collections, err := sdbh.clusterNode.ListCollections(appHeaders.UserID)
	switch err {
	case nil:
		c.JSON(http.StatusOK, gin.H{"collections": collections})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}
	// ---------------------------
}

type GetCollectionUri struct {
	CollectionId string `uri:"collectionId" binding:"required,alphanum,min=3,max=16"`
}

func (sdbh *SemaDBHandlers) GetCollection(c *gin.Context) {
	var uri GetCollectionUri
	if err := c.ShouldBindUri(&uri); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	appHeaders := c.MustGet("appHeaders").(AppHeaders)
	// ---------------------------
	collection, err := sdbh.clusterNode.GetCollection(appHeaders.UserID, uri.CollectionId)
	switch err {
	case nil:
		c.JSON(http.StatusOK, gin.H{"collection": collection})
	case cluster.ErrNotFound:
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}
	// ---------------------------
}

type InsertSinglePointRequest struct {
	Id       string    `json:"id" binding:"uuid"`
	Vector   []float32 `json:"vector" binding:"required"`
	Metadata any       `json:"metadata"`
}

type InsertPointsRequest struct {
	Points []InsertSinglePointRequest `json:"points" binding:"required"`
}

func (sdbh *SemaDBHandlers) InsertPoints(c *gin.Context) {
	appHeaders := c.MustGet("appHeaders").(AppHeaders)
	userPlan := c.MustGet("userPlan").(config.UserPlan)
	// ---------------------------
	var uri GetCollectionUri
	if err := c.ShouldBindUri(&uri); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	var req InsertPointsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	// Get corresponding collection
	collection, err := sdbh.clusterNode.GetCollection(appHeaders.UserID, uri.CollectionId)
	switch err {
	case nil:
		// Pass
	case cluster.ErrNotFound:
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "collection not found"})
		return
	default:
		log.Err(err).Msg("GetCollection failed")
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
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
		c.AbortWithStatusJSON(http.StatusAccepted, gin.H{"message": "accepted", "failed": errRanges})
		return
	}
	c.Status(http.StatusOK)
	// ---------------------------
}

type UpdatePointRequest struct {
	Id       string    `json:"id" binding:"required,uuid"`
	Vector   []float32 `json:"vector" binding:"required"`
	Metadata any       `json:"metadata"`
}

// TODO(nuric): implement update points endpoint

// ---------------------------

type SearchPointsRequest struct {
	Vector []float32 `json:"vector" binding:"required"`
	Limit  int       `json:"limit" binding:"required,min=1,max=100"`
}

type SearchPointResult struct {
	Id       string  `json:"id"`
	Distance float32 `json:"distance"`
	Metadata any     `json:"metadata"`
}

func (sdbh *SemaDBHandlers) SearchPoints(c *gin.Context) {
	appHeaders := c.MustGet("appHeaders").(AppHeaders)
	// ---------------------------
	var uri GetCollectionUri
	if err := c.ShouldBindUri(&uri); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	var req SearchPointsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	// Get corresponding collection
	collection, err := sdbh.clusterNode.GetCollection(appHeaders.UserID, uri.CollectionId)
	switch err {
	case nil:
		// TODO: refactor this processing
	case cluster.ErrNotFound:
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "collection not found"})
		return
	default:
		log.Err(err).Msg("GetCollection failed")
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
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
	c.JSON(http.StatusOK, gin.H{"points": results})
	// ---------------------------
}

// ---------------------------

func runHTTPServer(clusterState *cluster.ClusterNode) *http.Server {
	router := gin.Default()
	v1 := router.Group("/v1", AppHeaderMiddleware())
	v1.GET("/ping", pongHandler)
	// ---------------------------
	semaDBHandlers := NewSemaDBHandlers(clusterState)
	v1.POST("/collections", semaDBHandlers.NewCollection)
	v1.GET("/collections", semaDBHandlers.ListCollections)
	v1.GET("/collections/:collectionId", semaDBHandlers.GetCollection)
	v1.POST("/collections/:collectionId/points", semaDBHandlers.InsertPoints)
	v1.POST("/collections/:collectionId/points/search", semaDBHandlers.SearchPoints)
	// ---------------------------
	server := &http.Server{
		Addr:    config.Cfg.HttpHost + ":" + strconv.Itoa(config.Cfg.HttpPort),
		Handler: router,
	}
	go func() {
		log.Info().Str("httpAddr", server.Addr).Msg("HTTPAPI.Serve")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("failed to start http server")
		}
	}()
	return server
}
