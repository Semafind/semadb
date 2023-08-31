package httpapi

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
)

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

func ZerologLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		// ---------------------------
		// Start timer
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery
		// ---------------------------
		// Process request
		c.Next()
		// ---------------------------
		// Stop timer and gather information
		timeStamp := time.Now()
		latency := timeStamp.Sub(start)

		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()
		errorMessage := c.Errors.ByType(gin.ErrorTypePrivate).String()

		bodySize := c.Writer.Size()

		if raw != "" {
			path = path + "?" + raw
		}
		// ---------------------------
		logEvent := log.Info().Time("timeStamp", timeStamp).
			Dur("latency", latency).
			Str("clientIP", clientIP).
			Str("method", method).Str("path", path).
			Int("statusCode", statusCode).
			Str("errorMessage", errorMessage).
			Int("bodySize", bodySize).
			Str("path", path)
		// Extract app headers if any
		appH, ok := c.Keys["appHeaders"]
		if ok {
			appHeaders := appH.(AppHeaders)
			// We are not logging the user ID for privacy reasons
			logEvent = logEvent.Str("package", appHeaders.Package)
		}
		logEvent.Msg("HTTPAPI")
	}
}
