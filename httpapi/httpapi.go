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

func RunHTTPServer(cnode *cluster.ClusterNode) *http.Server {
	router := gin.Default()
	v1 := router.Group("/v1", AppHeaderMiddleware())
	v1.GET("/ping", pongHandler)
	// ---------------------------
	semaDBHandlers := NewSemaDBHandlers(cnode)
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
