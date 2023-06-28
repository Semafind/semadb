package main

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
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
	clusterState *ClusterState
	rpcApi       *RPCAPI
}

func NewSemaDBHandlers(clusterState *ClusterState, rpcApi *RPCAPI) *SemaDBHandlers {
	return &SemaDBHandlers{clusterState: clusterState, rpcApi: rpcApi}
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
	Name       string `json:"name" binding:"required"`
	EmbedSize  uint   `json:"embedSize" binding:"required"`
	DistMetric string `json:"distMetric" default:"euclidean"`
}

func (sdbh *SemaDBHandlers) NewCollection(c *gin.Context) {
	var req NewCollectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var headers CommonHeaders
	if err := c.ShouldBindHeader(&headers); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	log.Debug().Interface("req", req).Interface("headers", headers).Msg("NewCollection")
	// ---------------------------
	newUUID, err := uuid.NewRandom()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	vamanaCollection := models.VamanaCollection{
		Collection: models.Collection{
			Id:         newUUID,
			Name:       req.Name,
			EmbedSize:  req.EmbedSize,
			DistMetric: req.DistMetric,
			Owner:      headers.UserID,
			Package:    headers.Package,
			Shards:     1,
			Replicas:   1,
			Algorithm:  "vamana",
		},
		Parameters: models.DefaultVamanaParameters(),
	}
	log.Debug().Interface("vamanaCollection", vamanaCollection).Msg("NewCollection")
	// ---------------------------
	repCount := config.Cfg.GeneralReplication
	targetServers := RendezvousHash(vamanaCollection.Id.String(), sdbh.clusterState.Servers, repCount)
	// These servers will be responsible for the collection under the current cluster configuration
	// for collection operations, we are looking for strong consistency so all of them should be up
	// and achnowledge the collection creation, otherwise the user might not see the collection
	// if the request lands on a target server that is stale.
	// ---------------------------
	// Let's coordinate the collection creation with the target servers
	log.Debug().Interface("targetServers", targetServers).Msg("NewCollection")
	c.JSON(http.StatusOK, gin.H{
		"collectionId": vamanaCollection.Id.String(),
	})
}

// ---------------------------

func runHTTPServer(clusterState *ClusterState, rpcApi *RPCAPI) *http.Server {
	router := gin.Default()
	v1 := router.Group("/v1")
	v1.GET("/ping", pongHandler)
	// ---------------------------
	semaDBHandlers := NewSemaDBHandlers(clusterState, rpcApi)
	v1.POST("/collections", semaDBHandlers.NewCollection)
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
