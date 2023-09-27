package main

import (
	"context"
	"os"
	"os/signal"
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
	if cfg.Debug {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
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
	clusterNode.Serve()
	// ---------------------------
	httpServer := httpapi.RunHTTPServer(clusterNode, cfg.HttpApi, reg)
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
