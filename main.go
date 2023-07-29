package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/config"
)

// ---------------------------

func setupLogging() {
	// UNIX Time is faster and smaller than most timestamps
	// zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	// ---------------------------
	// Default level for this example is info, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if config.Cfg.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().Interface("config", config.Cfg).Msg("Configuration")
	}
	// ---------------------------
	log.Debug().Msg("Debug mode enabled")
}

func init() {
	setupLogging()
	// ---------------------------
	// Setup gin
	// ginMode := os.Getenv("GIN_MODE")
	// if ginMode == "release" {
	// 	gin.SetMode(gin.ReleaseMode)
	// }
	// ---------------------------
}

// ---------------------------

func main() {
	log.Info().Str("version", "0.0.1").Msg("Starting semadb")
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get hostname")
	}
	log.Info().Str("hostname", hostname).Msg("Initial parameters")
	// ---------------------------
	// Setup cluster state
	clusterNode, err := cluster.NewNode()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create cluster state")
	}
	clusterNode.Serve()
	// ---------------------------
	httpServer := runHTTPServer(clusterNode)
	// ---------------------------
	quit := make(chan os.Signal, 1)
	// kill (no param) default send syscanll.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall. SIGKILL but can"t be catch, so don't need add it
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	sig := <-quit
	log.Info().Str("signal", sig.String()).Msg("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("HTTP server forced to shut")
	}
	cancel()
	// TODO: Gracefully shutdown cluster
	clusterNode.Close()
	// ---------------------------
}
