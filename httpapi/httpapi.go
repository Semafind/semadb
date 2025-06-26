package httpapi

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/httpapi/middleware"
	"github.com/semafind/semadb/httpapi/utils"
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
	var metrics *middleware.HttpMetrics
	if cfg.EnableMetrics && reg != nil {
		metrics = middleware.SetupAndListenMetrics(cfg.MetricsHttpHost, cfg.MetricsHttpPort, reg)
	}
	// ---------------------------
	apiMux := http.NewServeMux()
	apiMux.Handle("/v1/", http.StripPrefix("/v1", httpv1.SetupV1Handlers(cnode)))
	apiMux.Handle("/v2/", http.StripPrefix("/v2", httpv2.SetupV2Handlers(cnode)))
	// ---------------------------
	var handler http.Handler = apiMux
	handler = middleware.AppHeaderMiddleware(cfg.UserPlans, handler)
	handler = middleware.WhiteListIP(cfg.WhiteListIPs, handler)
	handler = middleware.ProxySecret(cfg.ProxySecret, handler)
	handler = middleware.ZeroLoggerMetrics(metrics, handler)
	// ---------------------------
	/* We're moving health check outside of logging and proxy to make it more
	 * accessible in deployments. It will also not be logged to not be swamped
	 * by k8s healthchecks. */
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		utils.Encode(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("/", handler)
	var globalHandler http.Handler = mux
	globalHandler = middleware.Recover(globalHandler)
	// ---------------------------
	return globalHandler
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
