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
	"github.com/semafind/semadb/httpapi/middleware"
)

// ---------------------------

func ZerologLoggerMetrics(metrics *httpMetrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		// ---------------------------
		// Start timer
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery
		reqSize := c.Request.ContentLength
		// ---------------------------
		// Process request
		c.Next()
		// ---------------------------
		// Stop timer and gather information
		latency := time.Since(start)

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
			Str("clientIP", c.ClientIP()).
			Str("remoteIP", c.RemoteIP()).
			Str("method", method).Str("path", path).
			Int("statusCode", statusCode).
			Int64("requestSize", reqSize).
			Int("bodySize", bodySize).
			Str("path", path)
		// Extract app headers if any
		appH, ok := c.Keys["appHeaders"]
		if ok {
			appHeaders := appH.(middleware.AppHeaders)
			// We are not logging the user ID for privacy reasons
			logEvent = logEvent.Str("planId", appHeaders.PlanId)
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
			metrics.requestSize.WithLabelValues(ssCode, method, hname).Observe(float64(reqSize))
			metrics.responseSize.WithLabelValues(ssCode, method, hname).Observe(float64(bodySize))
		}
	}
}

// ---------------------------

func ProxySecretMiddleware(secret string) gin.HandlerFunc {
	log.Debug().Str("proxySecret", secret).Msg("ProxySecretMiddleware")
	return func(c *gin.Context) {
		if c.GetHeader("X-Proxy-Secret") != secret {
			c.AbortWithStatusJSON(http.StatusProxyAuthRequired, gin.H{"error": "forbidden"})
		}
	}
}

func WhiteListIPMiddleware(whitelist []string) gin.HandlerFunc {
	slices.Sort(whitelist)
	return func(c *gin.Context) {
		remoteIP := c.RemoteIP()
		_, found := slices.BinarySearch(whitelist, remoteIP)
		if !found {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		}
	}
}
