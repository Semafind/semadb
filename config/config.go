package config

import (
	"os"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/httpapi"
	"gopkg.in/yaml.v3"
)

// ---------------------------

type ConfigMap struct {
	// Global debug flag
	Debug bool `yaml:"debug"`
	// Pretty log output
	PrettyLogOutput bool `yaml:"prettyLogOutput"`
	// Cluster parameters
	ClusterNode cluster.ClusterNodeConfig `yaml:"clusterNode"`
	// HTTP Parameters
	HttpApi httpapi.HttpApiConfig `yaml:"httpApi"`
}

func LoadConfig() ConfigMap {
	configMap := ConfigMap{}
	// Load the file path from the environment variable
	cFilePath, ok := os.LookupEnv("SEMADB_CONFIG")
	if !ok {
		log.Fatal().Msg("SEMADB_CONFIG environment variable is not set")
	}
	// Parse the config file
	cFile, err := os.Open(cFilePath)
	if err != nil {
		log.Fatal().Err(err).Str("path", cFilePath).Msg("Failed to open config file")
	} else {
		decoder := yaml.NewDecoder(cFile)
		if err := decoder.Decode(&configMap); err != nil {
			log.Fatal().Err(err).Msg("Failed to parse config file")
		}
	}
	return configMap
}
