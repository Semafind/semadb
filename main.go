package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/httpapi"
)

// ---------------------------

func setupLogging(cfg config.ConfigMap) {
	// UNIX Time is faster and smaller than most timestamps
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	// ---------------------------
	// Default level for this example is info, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if cfg.PrettyLogOutput {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}
	if cfg.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().Interface("config", cfg).Msg("Configuration")
	}
	// ---------------------------
	log.Debug().Msg("Debug mode enabled")
}

// ---------------------------

func main() {
	cfg := config.LoadConfig()
	// ---------------------------
	setupLogging(cfg)
	// ---------------------------
	log.Info().Str("version", "0.0.1").Msg("Starting semadb")
	log.Info().Int("cpu_count", runtime.NumCPU()).Msg("Detected CPU count")
	// ---------------------------
	reg := prometheus.NewRegistry()
	// reg.MustRegister(collectors.NewGoCollector())
	// ---------------------------
	// Setup cluster state
	clusterNode, err := cluster.NewNode(cfg.ClusterNode)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create cluster state")
	}
	clusterNode.RegisterMetrics(reg)
	if err := clusterNode.Serve(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start cluster node")
	}
	// ---------------------------
	httpServer := httpapi.RunHTTPServer(clusterNode, cfg.HttpApi, reg)
	// ---------------------------
	// Conditional imports are not possible in Go, so we comment this out.
	// import _ "net/http/pprof" and then start the pprof server
	// access using http://localhost:8070/debug/pprof/
	// Useful commands:
	// go tool pprof -http=:8000 http://localhost:8071/debug/pprof/profile?seconds=20
	// go tool pprof -http=:8000 http://localhost:8071/debug/pprof/heap
	// ---------------------------
	// go func() {
	// 	debugPort := cfg.HttpApi.HttpPort - 10
	// 	err := http.ListenAndServe(":"+strconv.Itoa(debugPort), nil)
	// 	log.Info().Err(err).Msg("pprof")
	// }()
	// ---------------------------
	quit := make(chan os.Signal, 1)
	// kill (no param) default send syscanll.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall. SIGKILL but can"t be catch, so don't need add it
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	sig := <-quit
	// ---------------------------
	log.Info().Str("signal", sig.String()).Msg("Shutting down server...")
	// The default kubernetes grace period is 30 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("HTTP server forced to shut")
	}
	cancel()
	// ---------------------------
	clusterNode.Close()
	// ---------------------------
}
