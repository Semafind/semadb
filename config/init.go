package config

import (
	"github.com/caarlos0/env/v8"
	"github.com/rs/zerolog/log"
)

// ---------------------------

type Config struct {
	Debug bool `envDefault:"false"`
	// General replication count determines how many times a regular key (non-vector)
	// is replicated these may include user and collection information
	GeneralReplication int `envDefault:"3"`
	// Key value store directory, if not set it will be set to an temporary directory
	KVDir string `envDefault:""`
	// List of known servers at the beginning
	Servers []string `envDefault:""`
	// RPC Parameters
	RpcHost    string `envDefault:"localhost"`
	RpcPort    int    `envDefault:"9898"`
	RpcTimeout int    `envDefault:"1000"` // milliseconds
	// HTTP Parameters
	HttpHost string `envDefault:"localhost"`
	HttpPort int    `envDefault:"8080"`
}

var Cfg Config

// ---------------------------

func init() {
	// Load .env file
	Cfg = Config{}
	opts := env.Options{RequiredIfNoDef: true, Prefix: "SEMADB_", UseFieldNameByDefault: true}
	if err := env.ParseWithOptions(&Cfg, opts); err != nil {
		log.Fatal().Err(err).Msg("Failed to parse env")
	}
}
