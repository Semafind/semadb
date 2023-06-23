package main

import "sync"

type ClusterState struct {
	servers []string
	mu      sync.RWMutex
}
