package config

import (
	"os"
	"path/filepath"

	"github.com/caarlos0/env/v8"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// ---------------------------

type UserPlan struct {
	Name              string `yaml:"name"`
	MaxCollections    int    `yaml:"maxCollections"`
	MaxCollectionSize int64  `yaml:"maxCollectionSize"`
	MaxMetadataSize   int    `yaml:"maxMetadataSize"`
}

type ConfigMap struct {
	Debug bool `yaml:"debug"`
	// Root directory refers to the start of the filesystem where all data is stored
	RootDir string `yaml:"rootDir"`
	// Maximum size of shards in bytes
	MaxShardSize int64 `yaml:"maxShardSize"`
	// Shard timeout in seconds
	ShardTimeout int `yaml:"shardTimeout"`
	// List of known servers at the beginning
	Servers []string `yaml:"servers"`
	// RPC Parameters
	RpcHost    string `yaml:"rpcHost"`
	RpcPort    int    `yaml:"rpcPort"`
	RpcTimeout int    `yaml:"rpcTimeout"`
	// HTTP Parameters
	HttpHost string `yaml:"httpHost"`
	HttpPort int    `yaml:"httpPort"`
	// User plans
	UserPlans map[string]UserPlan `yaml:"userPlans"`
}

var Cfg ConfigMap

// ---------------------------

func init() {
	Cfg = LoadConfig()
}

func LoadConfig() ConfigMap {
	configMap := ConfigMap{}
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
		err = decoder.Decode(&configMap)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to parse config file")
		}
	}
	// Then parse environment variables
	opts := env.Options{Prefix: "SEMADB_", UseFieldNameByDefault: true}
	if err := env.ParseWithOptions(&configMap, opts); err != nil {
		log.Fatal().Err(err).Msg("Failed to parse env")
	}
	return configMap
}
