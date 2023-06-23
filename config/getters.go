package config

import (
	"os"
	"strconv"

	"github.com/rs/zerolog/log"
)

func GetBool(key string, defaultValue bool) bool {
	if val, ok := os.LookupEnv(key); ok {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			log.Warn().Err(err).Str(key, val).Msg("Failed to convert to bool")
			return defaultValue
		}
		return boolVal
	}
	return defaultValue
}

func GetString(key string, defaultValue string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultValue
}

func GetInt(key string, defaultValue int) int {
	if val, ok := os.LookupEnv(key); ok {
		intVal, err := strconv.Atoi(val)
		if err != nil {
			log.Warn().Err(err).Str(key, val).Msg("Failed to convert to int")
			return defaultValue
		}
		return intVal
	}
	return defaultValue
}
