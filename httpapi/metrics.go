package httpapi

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

type httpMetrics struct {
	// ---------------------------
	requestCount    *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	requestSize     *prometheus.HistogramVec
	responseSize    *prometheus.HistogramVec
	// ---------------------------
}

func setupAndListenMetrics(cfg HttpApiConfig) *httpMetrics {
	reg := prometheus.NewRegistry()
	// reg.MustRegister(collectors.NewGoCollector())
	// ---------------------------
	metrics := &httpMetrics{
		requestCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_request_count",
				Help: "Total number of HTTP requests made.",
			},
			[]string{"code", "method", "handler"},
		),
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "HTTP request latencies in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"code", "method", "handler"},
		),
		requestSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_size_bytes",
				Help:    "HTTP request sizes in bytes.",
				Buckets: []float64{0, 1 << 10, 1 << 15, 1 << 20},
			},
			[]string{"code", "method", "handler"},
		),
		responseSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_response_size_bytes",
				Help:    "HTTP request sizes in bytes.",
				Buckets: []float64{0, 1 << 10, 1 << 15, 1 << 20},
			},
			[]string{"code", "method", "handler"},
		),
	}
	reg.MustRegister(metrics.requestCount)
	reg.MustRegister(metrics.requestDuration)
	reg.MustRegister(metrics.requestSize)
	reg.MustRegister(metrics.responseSize)
	// ---------------------------
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	metricsServer := &http.Server{
		Addr:    cfg.HttpHost + ":" + strconv.Itoa(cfg.MetricsHttpPort),
		Handler: mux,
	}
	// ---------------------------
	// We start the server in the background. We can in the future add a
	// graceful shutdown here.
	go func() {
		log.Info().Str("httpAddr", metricsServer.Addr).Msg("HTTPAPI.ServeMetrics")
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("failed to start http server")
		}
	}()
	// ---------------------------
	return metrics
}

// ---------------------------
