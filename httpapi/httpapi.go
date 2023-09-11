package httpapi

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
)

// ---------------------------

func pongHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong from semadb",
	})
}

// ---------------------------

type UserPlan struct {
	Name              string `yaml:"name"`
	MaxCollections    int    `yaml:"maxCollections"`
	MaxCollectionSize int64  `yaml:"maxCollectionSize"`
	MaxMetadataSize   int    `yaml:"maxMetadataSize"`
}

type HttpApiConfig struct {
	Debug    bool   `yaml:"debug"`
	HttpHost string `yaml:"httpHost"`
	HttpPort int    `yaml:"httpPort"`
	// User plans
	UserPlans map[string]UserPlan `yaml:"userPlans"`
}

// ---------------------------

func setupRouter(cnode *cluster.ClusterNode, cfg HttpApiConfig) *gin.Engine {
	router := gin.New()
	router.Use(ZerologLogger(), gin.Recovery())
	v1 := router.Group("/v1", AppHeaderMiddleware(cfg))
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

func RunHTTPServer(cnode *cluster.ClusterNode, cfg HttpApiConfig) *http.Server {
	// ---------------------------
	if !cfg.Debug {
		gin.SetMode(gin.ReleaseMode)
	}
	// ---------------------------
	server := &http.Server{
		Addr:    cfg.HttpHost + ":" + strconv.Itoa(cfg.HttpPort),
		Handler: setupRouter(cnode, cfg),
	}
	go func() {
		log.Info().Str("httpAddr", server.Addr).Msg("HTTPAPI.Serve")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("failed to start http server")
		}
	}()
	return server
}
