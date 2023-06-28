package main

import (
	"sync"

	"github.com/semafind/semadb/config"
)

type ClusterState struct {
	Servers []string
	mu      sync.RWMutex
}

func newClusterState() (*ClusterState, error) {
	return &ClusterState{Servers: config.Cfg.Servers}, nil
}
