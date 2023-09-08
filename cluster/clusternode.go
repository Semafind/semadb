package cluster

import (
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
	"go.etcd.io/bbolt"
)

type ClusterNode struct {
	logger zerolog.Logger
	// ---------------------------
	Servers    []string
	MyHostname string
	// ---------------------------
	rpcClients   map[string]*rpc.Client
	rpcClientsMu sync.Mutex
	rpcServer    *http.Server
	// ---------------------------
	nodedb *bbolt.DB
	// ---------------------------
	shardStore map[string]*loadedShard
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
	logger := log.With().Str("hostname", envHostname).Str("component", "clusterNode").Logger()
	// ---------------------------
	rootDir := config.Cfg.RootDir
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("could not create root dir %s: %w", rootDir, err)
	}
	nodedb, err := bbolt.Open(filepath.Join(rootDir, "nodedb.bbolt"), 0666, &bbolt.Options{Timeout: 1 * time.Minute})
	if err != nil {
		return nil, fmt.Errorf("could not open node db: %w", err)
	}
	// ---------------------------
	cluster := &ClusterNode{
		logger:     logger,
		Servers:    config.Cfg.Servers,
		MyHostname: envHostname,
		rpcClients: make(map[string]*rpc.Client),
		nodedb:     nodedb,
		shardStore: make(map[string]*loadedShard),
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
		c.logger.Info().Str("rpcHost", c.MyHostname).Msg("rpcServe")
		if err := c.rpcServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			c.logger.Fatal().Err(err).Msg("Failed to listen and serve RPC")
		}
	}()
	// ---------------------------
}

func (c *ClusterNode) Close() error {
	if err := c.nodedb.Close(); err != nil {
		return fmt.Errorf("could not close node db: %w", err)
	}
	return nil
}

// ---------------------------
