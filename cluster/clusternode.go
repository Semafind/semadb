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
	"go.etcd.io/bbolt"
)

var USERCOLSKEY = []byte("userCollections")

const DBDELIMITER = "/"

type ClusterNodeConfig struct {
	// Root directory for all data
	RootDir string `yaml:"rootDir"`
	// ---------------------------
	RpcHost string `yaml:"rpcHost"`
	RpcPort int    `yaml:"rpcPort"`
	// Timeout in seconds
	RpcTimeout int `yaml:"rpcTimeout"`
	RpcRetries int `yaml:"rpcRetries"`
	// ---------------------------
	// Initial set of known servers
	Servers []string `yaml:"servers"`
	// Shard manager configuration
	ShardManager ShardManagerConfig `yaml:"shardManager"`
	// ---------------------------
	// Maximum size of shards in bytes
	MaxShardSize int64 `yaml:"maxShardSize"`
	// Maximum number of points in a shard
	MaxShardPointCount int64 `yaml:"maxShardPointCount"`
	// Maximum number of points to search
	MaxSearchLimit int `yaml:"maxSearchLimit"`
}

type ClusterNode struct {
	logger zerolog.Logger
	// ---------------------------
	cfg ClusterNodeConfig
	// ---------------------------
	Servers    []string
	MyHostname string
	// ---------------------------
	rpcClients   map[string]*rpc.Client
	rpcClientsMu sync.Mutex
	rpcServer    *http.Server
	// ---------------------------
	metrics *clusterNodeMetrics
	// ---------------------------
	nodedb *bbolt.DB
	// ---------------------------
	shardManager *ShardManager
}

func NewNode(config ClusterNodeConfig) (*ClusterNode, error) {
	// ---------------------------
	// Determine hostname
	envHostname := config.RpcHost
	{
		if envHostname == "" {
			hostname, err := os.Hostname()
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to get hostname")
			}
			log.Warn().Str("hostname", hostname).Msg("host not set, using hostname")
			envHostname = hostname
		}
		rpcPort := config.RpcPort
		envHostname = envHostname + ":" + strconv.Itoa(rpcPort)
	}
	// ---------------------------
	logger := log.With().Str("hostname", envHostname).Str("component", "clusterNode").Logger()
	// ---------------------------
	// Setup local node database
	rootDir := config.RootDir
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("could not create root dir %s: %w", rootDir, err)
	}
	nodedb, err := bbolt.Open(filepath.Join(rootDir, "nodedb.bbolt"), 0666, &bbolt.Options{Timeout: 1 * time.Minute})
	if err != nil {
		return nil, fmt.Errorf("could not open node db: %w", err)
	}
	// Check if user collections bucket exists
	err = nodedb.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(USERCOLSKEY)
		if err != nil {
			return fmt.Errorf("could not create bucket: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not create user collections bucket: %w", err)
	}
	// ---------------------------
	shardManager := NewShardManager(config.ShardManager)
	// ---------------------------
	cluster := &ClusterNode{
		logger:       logger,
		cfg:          config,
		Servers:      config.Servers,
		MyHostname:   envHostname,
		rpcClients:   make(map[string]*rpc.Client),
		metrics:      newClusterNodeMetrics(),
		nodedb:       nodedb,
		shardManager: shardManager,
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
