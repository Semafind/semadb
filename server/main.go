package main

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
)

// ---------------------------

// Cluster state that this nodes knows about
// it is initialised in the init function
var clusterState *ClusterState

func setupLogging() {
	// UNIX Time is faster and smaller than most timestamps
	// zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	// ---------------------------
	// Default level for this example is info, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if config.GetBool("SEMADB_DEBUG", false) {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		config.DumpPrefix("SEMADB")
	}
	// ---------------------------
	log.Debug().Msg("Debug mode enabled")
}

func init() {
	setupLogging()
	// ---------------------------
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get hostname")
	}
	// ---------------------------
	// Setup cluster state
	clusterState = &ClusterState{
		servers: []string{hostname}, // we are one of the servers
	}
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
	// Setup kvstore
	kvstore, err := NewKVStore()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create kvstore")
	}
	log.Info().Msg("KVStore created")
	// ---------------------------
	runHTTPServer()
	// ---------------------------
	// TODO: graceful shutdown
	kvstore.Close()
}
