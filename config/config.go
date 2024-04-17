package config

import (
	"fmt"
	"os"

	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/httpapi"
	"gopkg.in/yaml.v3"
)

// ---------------------------

const SEMADB_CONFIG = "SEMADB_CONFIG"

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

func LoadConfig() (ConfigMap, error) {
	configMap := ConfigMap{}
	// Load the file path from the environment variable
	cFilePath, ok := os.LookupEnv(SEMADB_CONFIG)
	if !ok {
		return configMap, fmt.Errorf("config environment variable %s is not set", SEMADB_CONFIG)
	}
	// Parse the config file
	cFile, err := os.Open(cFilePath)
	if err != nil {
		return configMap, fmt.Errorf("failed to open config file %s: %w", cFilePath, err)
	} else {
		decoder := yaml.NewDecoder(cFile)
		if err := decoder.Decode(&configMap); err != nil {
			return configMap, fmt.Errorf("failed to parse config file %s: %w", cFilePath, err)
		}
	}
	return configMap, nil
}
