package cluster

import (
	"context"
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
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/utils"
)

// ---------------------------
const CURRRENTNODEVERSION = 1

var INTERNALBUCKETKEY = "internal"
var USERCOLSBUCKETKEY = "userCollections"

// ---------------------------
var NODEVERSIONKEY = []byte("clusterNodeVersion")

// ---------------------------

const DBDELIMITER = "/"

type ClusterNodeConfig struct {
	// Root directory for all data
	RootDir string `yaml:"rootDir"`
	// ---------------------------
	RpcHost string `yaml:"rpcHost"`
	// RpcDomain gets appended to the hostname when determining the full
	// hostname for the node. The full hostname is used to identify the node to
	// other nodes in the cluster.
	RpcDomain string `yaml:"rpcDomain"`
	RpcPort   int    `yaml:"rpcPort"`
	// Timeout in seconds
	RpcTimeout int `yaml:"rpcTimeout"`
	RpcRetries int `yaml:"rpcRetries"`
	// ---------------------------
	// Initial set of known servers
	Servers []string `yaml:"servers"`
	// Shard manager configuration
	ShardManager ShardManagerConfig `yaml:"shardManager"`
	// ---------------------------
	// Backup frequency of node database in seconds
	BackupFrequency int `yaml:"backupFrequency"`
	// Number of node database backups to keep
	BackupCount int `yaml:"backupCount"`
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
	// ---------------------------
	metrics *clusterNodeMetrics
	// ---------------------------
	nodedb diskstore.DiskStore
	// ---------------------------
	shardManager *ShardManager
	// ---------------------------
	// The done channel is used to signal goroutines to stop via the Close
	// method. The close method then waits for them to exit.
	doneCh      chan struct{}
	bgWaitGroup sync.WaitGroup
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
		envHostname = envHostname + config.RpcDomain + ":" + strconv.Itoa(config.RpcPort)
		log.Info().Str("hostname", envHostname).Msg("Full hostname")
	}
	// ---------------------------
	logger := log.With().Str("hostname", envHostname).Str("component", "clusterNode").Logger()
	// ---------------------------
	// Setup local node database
	rootDir := config.RootDir
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("could not create root dir %s: %w", rootDir, err)
	}
	nodedb, err := openNodeDB(filepath.Join(rootDir, "nodedb.bbolt"))
	if err != nil {
		return nil, fmt.Errorf("could not open node db: %w", err)
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
		doneCh:       make(chan struct{}),
	}
	return cluster, nil
}

// ---------------------------

func openNodeDB(dbPath string) (diskstore.DiskStore, error) {
	db, err := diskstore.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("could not open db %s: %w", dbPath, err)
	}
	// ---------------------------
	return db, nil
}

// ---------------------------

func (c *ClusterNode) Serve() {
	// ---------------------------
	// Setup RPC server
	rpc.Register(c)
	rpc.HandleHTTP()
	rpcServer := &http.Server{
		Addr:         c.cfg.RpcHost + ":" + strconv.Itoa(c.cfg.RpcPort),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	// ---------------------------
	go func() {
		// service connections
		c.logger.Info().Str("rpcHost", c.cfg.RpcHost).Msg("rpcServe")
		defer c.logger.Info().Msg("rpcServe stopped")
		if err := rpcServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			c.logger.Fatal().Err(err).Msg("Failed to listen and serve RPC")
		}
	}()
	// ---------------------------
	c.bgWaitGroup.Add(1)
	go func() {
		<-c.doneCh
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		c.logger.Debug().Msg("rpcServer.Shutdown")
		if err := rpcServer.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msg("RPC server forced to shut")
		}
		cancel()
		c.bgWaitGroup.Done()
	}()
	// ---------------------------
	// Setup periodic node database backups
	if c.cfg.BackupFrequency <= 0 {
		return
	}
	c.bgWaitGroup.Add(1)
	go func() {
		c.logger.Info().Int("backupFrequency", c.cfg.BackupFrequency).Int("backupCount", c.cfg.BackupCount).Msg("backupNodeDB")
		defer c.logger.Info().Msg("backupNodeDB stopped")
		ticker := time.NewTicker(time.Duration(c.cfg.BackupFrequency) * time.Second)
		for {
			select {
			case <-c.doneCh:
				ticker.Stop()
				c.bgWaitGroup.Done()
				return
			case <-ticker.C:
				// ---------------------------
				err := utils.BackupBBolt(c.nodedb, c.cfg.BackupFrequency, c.cfg.BackupCount)
				if err != nil {
					c.logger.Error().Err(err).Msg("Failed to backup node database")
				}
				// ---------------------------
			}
		}
	}()
}

func (c *ClusterNode) Close() error {
	// ---------------------------
	// Signal goroutines to stop
	close(c.doneCh)
	// ---------------------------
	// Wait for goroutines to stop
	c.bgWaitGroup.Wait()
	// ---------------------------
	// Close node database
	if err := c.nodedb.Close(); err != nil {
		return fmt.Errorf("could not close node db: %w", err)
	}
	// ---------------------------
	return nil
}

// ---------------------------
