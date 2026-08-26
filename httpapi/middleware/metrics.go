package middleware

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HttpMetrics struct {
	// ---------------------------
	requestCount    *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	requestSize     *prometheus.HistogramVec
	// ---------------------------
}

func SetupAndListenMetrics(host string, port int, reg *prometheus.Registry) *HttpMetrics {
	// ---------------------------
	metrics := &HttpMetrics{
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
	}
	reg.MustRegister(metrics.requestCount)
	reg.MustRegister(metrics.requestDuration)
	reg.MustRegister(metrics.requestSize)
	// ---------------------------
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	metricsServer := &http.Server{
		Addr:    host + ":" + strconv.Itoa(port),
		Handler: mux,
	}
	// ---------------------------
	// We start the server in the background. We can in the future add a
	// graceful shutdown here.
	go func() {
		slog.Info("HTTPAPI.ServeMetrics", "httpAddr", metricsServer.Addr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("failed to start http server", "error", err)
			os.Exit(1)
		}
	}()
	// ---------------------------
	return metrics
}

// ---------------------------
