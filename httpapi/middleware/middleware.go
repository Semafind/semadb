package middleware

import (
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"time"

	"github.com/semafind/semadb/httpapi/utils"
)

var collectionsRegex = regexp.MustCompile(`collections/[a-zA-Z0-9]+`)

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(b)
	lrw.bytesWritten += n
	return n, err
}

func (lrw *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return lrw.ResponseWriter
}

// ---------------------------
// Standard slog based middleware for logging HTTP requests and metrics
func LoggerMetrics(metrics *HttpMetrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(lrw, r)
		duration := time.Since(start)

		slog.Info("",
			"method", r.Method,
			"url", r.URL.String(),
			"status", lrw.statusCode,
			"size", lrw.bytesWritten,
			"duration", duration,
		)

		if metrics != nil {
			// Canonicalize the URL by removing url parameters
			// Replace anything of the form collections/mycol23 with collections/{collectionId}
			canonical := collectionsRegex.ReplaceAll([]byte(r.URL.Path), []byte("collections/{collectionId}"))
			hname := string(canonical)
			ssCode := strconv.Itoa(lrw.statusCode)
			metrics.requestCount.WithLabelValues(ssCode, r.Method, hname).Inc()
			metrics.requestDuration.WithLabelValues(ssCode, r.Method, hname).Observe(duration.Seconds())
			metrics.requestSize.WithLabelValues(ssCode, r.Method, hname).Observe(float64(lrw.bytesWritten))
		}
	})
}

// ---------------------------

func ProxySecret(secret string, next http.Handler) http.Handler {
	if len(secret) == 0 {
		slog.Warn("ProxySecretMiddleware is disabled")
		return next
	}
	slog.Debug("ProxySecretMiddleware", "proxySecret", secret)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Proxy-Secret") != secret {
			utils.Encode(w, http.StatusProxyAuthRequired, map[string]string{"error": "forbidden"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func WhiteListIP(whitelist []string, next http.Handler) http.Handler {
	if whitelist == nil || (len(whitelist) == 1 && whitelist[0] == "*") {
		slog.Warn("WhiteListIPMiddleware is disabled", "whiteListIPs", whitelist)
		return next
	}
	slog.Debug("WhiteListIPMiddleware", "whiteListIPs", whitelist)
	slices.Sort(whitelist)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, found := slices.BinarySearch(whitelist, r.RemoteAddr)
		if !found {
			utils.Encode(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				slog.Error("panic recovered", "error", err)
				slog.Error("stack trace", "stack", string(debug.Stack()))
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
