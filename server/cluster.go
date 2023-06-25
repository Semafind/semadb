package main

import (
	"strings"
	"sync"

	"github.com/semafind/semadb/config"
)

type ClusterState struct {
	Servers []string
	mu      sync.RWMutex
}

func newClusterState() (*ClusterState, error) {
	envServers := config.GetString("SEMADB_SERVERS", "")
	return &ClusterState{Servers: strings.Split(envServers, ",")}, nil
}
