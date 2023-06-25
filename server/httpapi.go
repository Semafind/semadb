package main

import (
	"net/http"

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

func newCollectionHandler(c *gin.Context) {
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
	log.Info().Interface("req", req).Interface("headers", headers).Msg("newCollectionHandler")
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
			Algorithm:  "vamana",
		},
		Parameters: models.DefaultVamanaParameters(),
	}
	log.Info().Interface("vamanaCollection", vamanaCollection).Msg("newCollectionHandler")
	// ---------------------------
	repCount := config.GetInt("SEMADB_GENERAL_REPLICATION", 1)
	targetServers := RendezvousHash(vamanaCollection.Id.String(), []string{"localhost"}, repCount)
	// These servers will be responsible for the collection under the current cluster configuration
	// for collection operations, we are looking for strong consistency so all of them should be up
	// and achnowledge the collection creation, otherwise the user might not see the collection
	// if the request lands on a target server that is stale.
	// ---------------------------
	// Let's coordinate the collection creation with the target servers
	log.Info().Interface("targetServers", targetServers).Msg("newCollectionHandler")
	c.JSON(http.StatusOK, gin.H{
		"collectionId": vamanaCollection.Id.String(),
	})
}

// ---------------------------

func runHTTPServer() {
	router := gin.Default()
	v1 := router.Group("/v1")
	v1.GET("/ping", pongHandler)
	v1.POST("/collections", newCollectionHandler)
	router.Run(":8080")
}
