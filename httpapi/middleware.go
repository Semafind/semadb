package httpapi

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ---------------------------
/* Common headers */
type AppHeaders struct {
	UserID  string `header:"X-User-Id" binding:"required"`
	Package string `header:"X-Package" binding:"required"`
}

func AppHeaderMiddleware(config HttpApiConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		var appHeaders AppHeaders
		if err := c.ShouldBindHeader(&appHeaders); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.Set("appHeaders", appHeaders)
		log.Debug().Interface("appHeaders", appHeaders).Msg("AppHeaderMiddleware")
		// Extract user plan
		userPlan, ok := config.UserPlans[appHeaders.Package]
		if !ok {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "unknown package"})
			return
		}
		c.Set("userPlan", userPlan)
		c.Next()
	}
}

// ---------------------------

func ZerologLoggerMetrics(metrics *httpMetrics) gin.HandlerFunc {
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
		latency := time.Since(start)

		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()
		lastError := c.Errors.ByType(gin.ErrorTypePrivate).Last()

		bodySize := c.Writer.Size()

		if raw != "" {
			path = path + "?" + raw
		}
		// ---------------------------
		var logEvent *zerolog.Event
		if statusCode == 500 || lastError != nil {
			logEvent = log.Error()
		} else {
			logEvent = log.Info()
		}
		logEvent.Err(lastError).
			Dur("latency", latency).
			Str("clientIP", clientIP).
			Str("method", method).Str("path", path).
			Int("statusCode", statusCode).
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
		// ---------------------------
		if metrics != nil {
			// Example handler names
			// github.com/semafind/semadb/httpapi.(*SemaDBHandlers).ListCollections-fm
			// github.com/semafind/semadb/httpapi.(*SemaDBHandlers).CreateCollection-fm
			fullHName := c.HandlerName()
			parts := strings.Split(fullHName, ".")
			hname := parts[len(parts)-1][:len(parts[len(parts)-1])-3]
			ssCode := strconv.Itoa(statusCode)
			metrics.requestCount.WithLabelValues(ssCode, method, hname).Inc()
			metrics.requestDuration.WithLabelValues(ssCode, method, hname).Observe(latency.Seconds())
			metrics.requestSize.WithLabelValues(ssCode, method, hname).Observe(float64(c.Request.ContentLength))
			metrics.responseSize.WithLabelValues(ssCode, method, hname).Observe(float64(bodySize))
		}
	}
}

// ---------------------------

func WhiteListIPMiddleware(whitelist []string) gin.HandlerFunc {
	slices.Sort(whitelist)
	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		_, found := slices.BinarySearch(whitelist, clientIP)
		if !found {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		}
	}
}
