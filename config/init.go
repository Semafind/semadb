package config

import (
	"os"
	"path/filepath"

	"github.com/caarlos0/env/v8"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// ---------------------------

type configMap struct {
	Debug bool `yaml:"debug"`
	// Root directory refers to the start of the filesystem where all data is stored
	RootDir string `yaml:"rootDir"`
	// Maximum size of shards in bytes
	MaxShardSize int64 `yaml:"maxShardSize"`
	// List of known servers at the beginning
	Servers []string `yaml:"servers"`
	// RPC Parameters
	RpcHost    string `yaml:"rpcHost"`
	RpcPort    int    `yaml:"rpcPort"`
	RpcTimeout int    `yaml:"rpcTimeout"`
	// HTTP Parameters
	HttpHost string `yaml:"httpHost"`
	HttpPort int    `yaml:"httpPort"`
}

var Cfg configMap

// ---------------------------

func init() {
	Cfg = configMap{}
	// First parse yaml file
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get working directory")
	}
	cFilePath := filepath.Join(cwd, "config.yaml")
	cFile, err := os.Open(cFilePath)
	if err != nil {
		log.Error().Err(err).Str("path", cFilePath).Msg("Failed to open config file")
	} else {
		decoder := yaml.NewDecoder(cFile)
		err = decoder.Decode(&Cfg)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to parse config file")
		}
	}
	// Then parse environment variables
	opts := env.Options{Prefix: "SEMADB_", UseFieldNameByDefault: true}
	if err := env.ParseWithOptions(&Cfg, opts); err != nil {
		log.Fatal().Err(err).Msg("Failed to parse env")
	}
}
