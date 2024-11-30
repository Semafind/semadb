package httpapi

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/httpapi/middleware"
	httpv1 "github.com/semafind/semadb/httpapi/v1"
	httpv2 "github.com/semafind/semadb/httpapi/v2"
	"github.com/semafind/semadb/models"
)

// ---------------------------

type HttpApiConfig struct {
	Debug           bool   `yaml:"debug"`
	HttpHost        string `yaml:"httpHost"`
	HttpPort        int    `yaml:"httpPort"`
	EnableMetrics   bool   `yaml:"enableMetrics"`
	MetricsHttpHost string `yaml:"metricsHttpHost"`
	MetricsHttpPort int    `yaml:"metricsHttpPort"`
	// Proxy secret can be used to restrict access to the API with the
	// X-Proxy-Secret header
	ProxySecret string `yaml:"proxySecret"`
	// Whitelist of IP addresses, ["*"] means any IP address
	WhiteListIPs []string `yaml:"whiteListIPs"`
	// User plans
	UserPlans map[string]models.UserPlan `yaml:"userPlans"`
}

// ---------------------------

func setupRouter(cnode *cluster.ClusterNode, cfg HttpApiConfig, reg *prometheus.Registry) http.Handler {
	// ---------------------------
	var metrics *httpMetrics
	if cfg.EnableMetrics && reg != nil {
		metrics = setupAndListenMetrics(cfg, reg)
	}
	// ---------------------------
	mux := http.NewServeMux()
	mux.Handle("/v1/", http.StripPrefix("/v1", httpv1.SetupV1Handlers(cnode)))
	mux.Handle("/v2/", http.StripPrefix("/v2", httpv2.SetupV2Handlers(cnode)))
	// ---------------------------
	var handler http.Handler = mux
	handler = middleware.AppHeaderMiddleware(cfg.UserPlans, handler)
	if cfg.WhiteListIPs == nil || (len(cfg.WhiteListIPs) == 1 && cfg.WhiteListIPs[0] == "*") {
		log.Warn().Strs("whiteListIPs", cfg.WhiteListIPs).Msg("WhiteListIPMiddleware is disabled")
	} else {
		handler = WhiteListIPMiddleware(cfg.WhiteListIPs, handler)
	}
	if len(cfg.ProxySecret) > 0 {
		log.Info().Msg("ProxySecretMiddleware is enabled")
		handler = ProxySecretMiddleware(cfg.ProxySecret, handler)
	}
	handler = ZeroLoggerMetrics(metrics, handler)
	handler = RecoverMiddleware(handler)
	// ---------------------------
	return handler
}

func RunHTTPServer(cnode *cluster.ClusterNode, cfg HttpApiConfig, reg *prometheus.Registry) *http.Server {
	// ---------------------------
	server := &http.Server{
		Addr:    cfg.HttpHost + ":" + strconv.Itoa(cfg.HttpPort),
		Handler: setupRouter(cnode, cfg, reg),
	}
	go func() {
		log.Info().Str("httpAddr", server.Addr).Msg("HTTPAPI.Serve")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("failed to start http server")
		}
	}()
	// ---------------------------
	return server
}
