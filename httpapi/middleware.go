package httpapi

import (
	"net/http"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"time"

	"github.com/rs/zerolog/hlog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/httpapi/utils"
)

// ---------------------------
// Zerolog based middleware for logging HTTP requests
func ZeroLoggerMetrics(metrics *httpMetrics, next http.Handler) http.Handler {
	handler := hlog.AccessHandler(func(r *http.Request, status, size int, duration time.Duration) {
		hlog.FromRequest(r).Info().
			Str("method", r.Method).
			Stringer("url", r.URL).
			Int("status", status).
			Int("size", size).
			Dur("duration", duration).
			Msg("")
		if metrics != nil {
			// Canonicalize the URL by removing url parameters
			// Replace anything of the form collections/mycol23 with collections/:id
			re := regexp.MustCompile(`collections/[a-zA-Z0-9]+`)
			canonical := re.ReplaceAll([]byte(r.URL.Path), []byte("collections/{collectionId}"))
			hname := string(canonical)
			ssCode := strconv.Itoa(status)
			metrics.requestCount.WithLabelValues(ssCode, r.Method, hname).Inc()
			metrics.requestDuration.WithLabelValues(ssCode, r.Method, hname).Observe(duration.Seconds())
			metrics.requestSize.WithLabelValues(ssCode, r.Method, hname).Observe(float64(size))
			// metrics.responseSize.WithLabelValues(ssCode, r.Method, hname).Observe(float64(bodySize))
		}
	})(next)
	handler = hlog.NewHandler(log.Logger)(handler)
	return handler
}

// ---------------------------

func ProxySecretMiddleware(secret string, next http.Handler) http.Handler {
	log.Debug().Str("proxySecret", secret).Msg("ProxySecretMiddleware")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Proxy-Secret") != secret {
			utils.Encode(w, http.StatusProxyAuthRequired, map[string]string{"error": "forbidden"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func WhiteListIPMiddleware(whitelist []string, next http.Handler) http.Handler {
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

func RecoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Error().Interface("error", err).Msg("panic recovered")
				log.Error().Str("stack", string(debug.Stack())).Msg("stack trace")
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
