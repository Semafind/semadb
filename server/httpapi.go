package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/models"
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
type CommonHeaders struct {
	UserID  string `header:"X-User-Id" binding:"required"`
	Package string `header:"X-Package" binding:"required"`
}

// ---------------------------
/* Collection handlers */

type NewCollectionRequest struct {
	Id         string `json:"id" binding:"required,alphanum,min=3,max=16"`
	EmbedSize  uint   `json:"embedSize" binding:"required"`
	DistMetric string `json:"distMetric" default:"euclidean"`
}

func (sdbh *SemaDBHandlers) NewCollection(c *gin.Context) {
	var req NewCollectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var headers CommonHeaders
	if err := c.ShouldBindHeader(&headers); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	log.Debug().Interface("req", req).Interface("headers", headers).Msg("NewCollection")
	// ---------------------------
	vamanaCollection := models.VamanaCollection{
		Collection: models.Collection{
			Id:         req.Id,
			EmbedSize:  req.EmbedSize,
			DistMetric: req.DistMetric,
			Shards:     1,
			Replicas:   1,
			Algorithm:  "vamana",
			Version:    time.Now().UnixNano(),
			CreatedAt:  time.Now().UnixNano(),
		},
		Parameters: models.DefaultVamanaParameters(),
	}
	log.Debug().Interface("vamanaCollection", vamanaCollection).Msg("NewCollection")
	// ---------------------------
	err := sdbh.clusterNode.CreateCollection(cluster.CreateCollectionRequest{
		UserId:     headers.UserID,
		Collection: vamanaCollection.Collection,
	})
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
	var headers CommonHeaders
	if err := c.ShouldBindHeader(&headers); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	log.Debug().Interface("headers", headers).Msg("ListCollections")
	// ---------------------------
	collections, err := sdbh.clusterNode.ListCollections(headers.UserID)
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

// ---------------------------

func runHTTPServer(clusterState *cluster.ClusterNode) *http.Server {
	router := gin.Default()
	v1 := router.Group("/v1")
	v1.GET("/ping", pongHandler)
	// ---------------------------
	semaDBHandlers := NewSemaDBHandlers(clusterState)
	v1.POST("/collections", semaDBHandlers.NewCollection)
	v1.GET("/collections", semaDBHandlers.ListCollections)
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
