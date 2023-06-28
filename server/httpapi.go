package main

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/kvstore"
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
	binaryId, err := vamanaCollection.Id.MarshalBinary()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	collectionKey := append(kvstore.COLLECTION_PREFIX, binaryId...)
	collectionValue, err := msgpack.Marshal(vamanaCollection)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	repCount := config.Cfg.GeneralReplication
	// These servers are responsible for the collection under the current cluster configuration
	targetServers := RendezvousHash(collectionKey, sdbh.clusterState.Servers, repCount)
	// ---------------------------
	// Let's coordinate the collection creation with the target servers
	log.Debug().Interface("targetServers", targetServers).Msg("NewCollection")
	results := make(chan error, len(targetServers))
	for _, server := range targetServers {
		go func(dest string) {
			writeKVReq := &WriteKVRequest{
				RequestArgs: RequestArgs{
					Source: "",
					Dest:   dest,
				},
				Key:   collectionKey,
				Value: collectionValue,
			}
			writeKVResp := &WriteKVResponse{}
			results <- sdbh.rpcApi.WriteKV(writeKVReq, writeKVResp)
		}(server.Server)
	}
	// ---------------------------
	// Check if at least one server succeeded
	successCount := 0
	conflictCount := 0
	timeoutCount := 0
	for i := 0; i < len(targetServers); i++ {
		err := <-results
		if err == nil {
			successCount++
		} else if errors.Is(err, kvstore.ErrStaleData) {
			conflictCount++
		} else if errors.Is(err, ErrRPCTimeout) {
			timeoutCount++
		} else {
			log.Error().Err(err).Msg("NewCollection")
		}
	}
	log.Debug().Int("successCount", successCount).Int("conflictCount", conflictCount).Int("timeoutCount", timeoutCount).Msg("NewCollection")
	// ---------------------------
	// Conflict case should not happen for new collection but just in case, we'll handle it here.
	if conflictCount > 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "conflict"})
	} else if timeoutCount == len(targetServers) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "timeout"})
	} else if successCount == 0 {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	} else {
		c.JSON(http.StatusOK, gin.H{"collectionId": vamanaCollection.Id.String()})
	}
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
