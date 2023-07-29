package cluster

import (
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/shard"
)

type ClusterNode struct {
	Servers    []string
	MyHostname string
	serversMu  sync.RWMutex
	// ---------------------------
	rpcClients   map[string]*rpc.Client
	rpcClientsMu sync.Mutex
	rpcServer    *http.Server
	// ---------------------------
	shardStore map[string]*shard.Shard
	shardLock  sync.Mutex
}

func NewNode() (*ClusterNode, error) {
	// ---------------------------
	// Determine hostname
	envHostname := config.Cfg.RpcHost
	{
		if envHostname == "" {
			hostname, err := os.Hostname()
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to get hostname")
			}
			log.Warn().Str("hostname", hostname).Msg("host not set, using hostname")
			envHostname = hostname
		}
		rpcPort := config.Cfg.RpcPort
		envHostname = envHostname + ":" + strconv.Itoa(rpcPort)
	}
	// ---------------------------
	cluster := &ClusterNode{
		Servers:    config.Cfg.Servers,
		MyHostname: envHostname,
		rpcClients: make(map[string]*rpc.Client),
		shardStore: make(map[string]*shard.Shard),
	}
	return cluster, nil
}

func (c *ClusterNode) Serve() {
	// ---------------------------
	// Setup RPC server
	rpc.Register(c)
	rpc.HandleHTTP()
	c.rpcServer = &http.Server{
		Addr:         c.MyHostname,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		// service connections
		log.Info().Str("rpcHost", c.MyHostname).Msg("rpcServe")
		if err := c.rpcServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("Failed to listen and serve RPC")
		}
	}()
	// ---------------------------
	// go c.startRepLogService()
}

func (c *ClusterNode) Close() error {
	return nil
}

// ---------------------------
