package httpapi

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/config"
)

// ---------------------------

func pongHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong from semadb",
	})
}

// ---------------------------

func setupRouter(cnode *cluster.ClusterNode) *gin.Engine {
	router := gin.New()
	router.Use(ZerologLogger(), gin.Recovery())
	v1 := router.Group("/v1", AppHeaderMiddleware())
	v1.GET("/ping", pongHandler)
	// ---------------------------
	// https://stackoverflow.blog/2020/03/02/best-practices-for-rest-api-design/
	semaDBHandlers := NewSemaDBHandlers(cnode)
	v1.POST("/collections", semaDBHandlers.NewCollection)
	v1.GET("/collections", semaDBHandlers.ListCollections)
	colRoutes := v1.Group("/collections/:collectionId", semaDBHandlers.CollectionURIMiddleware())
	colRoutes.GET("/", semaDBHandlers.GetCollection)
	colRoutes.DELETE("/", semaDBHandlers.DeleteCollection)
	// We're batching point requests for peformance reasons. Alternatively we
	// can provide points/:pointId endpoint in the future.
	colRoutes.POST("/points", semaDBHandlers.InsertPoints)
	colRoutes.PUT("/points", semaDBHandlers.UpdatePoints)
	colRoutes.DELETE("/points", semaDBHandlers.DeletePoints)
	colRoutes.POST("/points/search", semaDBHandlers.SearchPoints)
	return router
}

func RunHTTPServer(cnode *cluster.ClusterNode) *http.Server {
	// ---------------------------
	if !config.Cfg.Debug {
		gin.SetMode(gin.ReleaseMode)
	}
	// ---------------------------
	server := &http.Server{
		Addr:    config.Cfg.HttpHost + ":" + strconv.Itoa(config.Cfg.HttpPort),
		Handler: setupRouter(cnode),
	}
	go func() {
		log.Info().Str("httpAddr", server.Addr).Msg("HTTPAPI.Serve")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("failed to start http server")
		}
	}()
	return server
}
