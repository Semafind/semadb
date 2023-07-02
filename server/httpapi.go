package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
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
		c.Next()
	}
}

// ---------------------------
/* Collection handlers */

type NewCollectionRequest struct {
	Id         string `json:"id" binding:"required,alphanum,min=3,max=16"`
	VectorSize uint   `json:"vectorSize" binding:"required"`
	DistMetric string `json:"distMetric" binding:"required,oneof=euclidean cosine"`
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
		Id:         req.Id,
		VectorSize: req.VectorSize,
		DistMetric: req.DistMetric,
		Shards:     1,
		Replicas:   1,
		Algorithm:  "vamana",
		Version:    time.Now().UnixMicro(),
		CreatedAt:  time.Now().UnixMicro(),
		Parameters: models.DefaultVamanaParameters(),
	}
	log.Debug().Interface("collection", vamanaCollection).Msg("NewCollection")
	// ---------------------------
	err := sdbh.clusterNode.CreateCollection(appHeaders.UserID, vamanaCollection)
	switch err {
	case nil:
		c.JSON(http.StatusCreated, gin.H{"message": "collection created"})
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
	case cluster.ErrTimeout:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "timeout"})
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
	switch {
	case err == nil || errors.Is(err, cluster.ErrPartialSuccess):
		c.JSON(http.StatusOK, gin.H{"collection": collection})
	case errors.Is(err, cluster.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
	case errors.Is(err, cluster.ErrTimeout):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "timeout"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}
	// ---------------------------
}

type PointRequest struct {
	Id       string    `json:"id" binding:"required,alphanum,max=16"`
	Vector   []float32 `json:"vector" binding:"required"`
	Metadata any       `json:"metadata"`
}

type CreatePointsRequest struct {
	Points []PointRequest `json:"points" binding:"required"`
}

func (sdbh *SemaDBHandlers) CreatePoints(c *gin.Context) {
	appHeaders := c.MustGet("appHeaders").(AppHeaders)
	// ---------------------------
	var uri GetCollectionUri
	if err := c.ShouldBindUri(&uri); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	var req CreatePointsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	// Get corresponding collection
	collection, err := sdbh.clusterNode.GetCollection(appHeaders.UserID, uri.CollectionId)
	switch {
	case err == nil || errors.Is(err, cluster.ErrPartialSuccess):
		// TODO: refactor this processing
	case errors.Is(err, cluster.ErrNotFound):
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "collection not found"})
		return
	case errors.Is(err, cluster.ErrTimeout):
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "collection timeout"})
		return
	default:
		log.Err(err).Msg("GetCollection failed")
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	// ---------------------------
	// Convert request points into internal points, doing checks along the way
	points := make([]models.Point, len(req.Points))
	maxMetadataSize := 1024 // TODO: make this configurable
	for i, point := range req.Points {
		if len(point.Vector) != int(collection.VectorSize) {
			errMsg := fmt.Sprintf("invalid vector dimension expected %d got %d for point %s", collection.VectorSize, len(point.Vector), point.Id)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		binaryMetadata, err := msgpack.Marshal(point.Metadata)
		if err != nil {
			errMsg := fmt.Sprintf("invalid metadata for point %s", point.Id)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		if len(binaryMetadata) > maxMetadataSize {
			errMsg := fmt.Sprintf("point %s exceeds maximum metadata size %d > %d", point.Id, len(binaryMetadata), maxMetadataSize)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		points[i] = models.Point{
			Id:       point.Id,
			Vector:   point.Vector,
			Version:  time.Now().UnixMicro(),
			Metadata: binaryMetadata,
		}
	}
	// ---------------------------
	// err := sdbh.clusterNode.CreatePoints(cluster.CreatePointsRequest{
	// 	UserId:      headers.UserID,
	// 	Collection:  uri.CollectionId,
	// 	Points:      req.Points,
	// 	Shard:       req.Shard,
	// 	Replica:     req.Replica,
	// 	WaitForSync: req.WaitForSync,
	// })
	switch err {
	case nil:
		c.JSON(http.StatusCreated, gin.H{"message": "points created"})
	case cluster.ErrConflict:
		c.JSON(http.StatusConflict, gin.H{"error": "conflict"})
	case cluster.ErrTimeout:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "timeout"})
	case cluster.ErrPartialSuccess:
		c.JSON(http.StatusAccepted, gin.H{"message": "points accepted"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}
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
	v1.POST("/collections/:collectionId/points", semaDBHandlers.CreatePoints)
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
