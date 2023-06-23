package config

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
)

// ---------------------------

func init() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Warn().Msg("No .env file found")
	}
}

// ---------------------------

func DumpPrefix(prefix string) {
	// Print environment config
	config := make(map[string]string)
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "SEMADB_") {
			pair := strings.Split(e, "=")
			config[pair[0]] = pair[1]
		}
	}
	log.Debug().Interface("config", config).Msg("Environment config")
}

// ---------------------------
