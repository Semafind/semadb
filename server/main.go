package main

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
	// Setup cluster state
	clusterState, err := newClusterState()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create cluster state")
	}
	log.Info().Interface("clusterState", clusterState).Msg("Cluster state")
	// ---------------------------
	// Setup RPC API
	rpcAPI := NewRPCAPI(clusterState, kvstore)
	rpcAPI.Serve()
	// ---------------------------
	time.Sleep(5 * time.Second)
	log.Debug().Msg("Testing RPCAPI.Ping")
	if rpcAPI.MyHostname == "localhost:11001" {
		pingRequest := &PingRequest{RequestArgs: RequestArgs{Source: rpcAPI.MyHostname, Destination: "localhost:11002"}, Message: "hi"}
		pingResponse := &PingResponse{}
		err = rpcAPI.Ping(pingRequest, pingResponse)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to ping")
		}
		log.Debug().Interface("pingResponse", pingResponse).Msg("Ping response")
	}
	// ---------------------------
	// runHTTPServer()
	// ---------------------------
	// TODO: graceful shutdown
	kvstore.Close()
}
