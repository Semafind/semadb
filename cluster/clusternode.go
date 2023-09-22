package cluster

import (
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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
	// Backup frequency of node database in seconds
	backupFrequency int `yaml:"backupFrequency"`
	// Number of node database backups to keep
	backupCount int `yaml:"backupCount"`
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
	}
	return cluster, nil
}

// ---------------------------

func openNodeDB(dbPath string) (*bbolt.DB, error) {
	db, err := bbolt.Open(dbPath, 0644, &bbolt.Options{Timeout: 1 * time.Minute})
	if err != nil {
		return nil, fmt.Errorf("could not open db %s: %w", dbPath, err)
	}
	// ---------------------------
	// Check if user collections bucket exists
	err = db.Update(func(tx *bbolt.Tx) error {
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
	return db, nil
}

// ---------------------------

func (c *ClusterNode) Serve() {
	// ---------------------------
	// Setup RPC server
	rpc.Register(c)
	rpc.HandleHTTP()
	rpcServer := &http.Server{
		Addr:         c.MyHostname,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	// ---------------------------
	go func() {
		// service connections
		c.logger.Info().Str("rpcHost", c.MyHostname).Msg("rpcServe")
		defer c.logger.Info().Msg("rpcServe stopped")
		if err := rpcServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			c.logger.Fatal().Err(err).Msg("Failed to listen and serve RPC")
		}
	}()
	// ---------------------------
	// Setup periodic node database backups
	if c.cfg.backupFrequency <= 0 {
		return
	}
	go func() {
		c.logger.Info().Int("backupFrequency", c.cfg.backupFrequency).Int("backupCount", c.cfg.backupCount).Msg("backupNodeDB")
		defer c.logger.Info().Msg("backupNodeDB stopped")
		ticker := time.NewTicker(time.Duration(c.cfg.backupFrequency) * time.Second)
		for {
			select {
			case <-ticker.C:
				// ---------------------------
				// Create backup
				backupPath := fmt.Sprintf("%v-nodedb.bbolt.backup", time.Now().Unix())
				backupPath = filepath.Join(c.cfg.RootDir, backupPath)
				err := c.nodedb.View(func(tx *bbolt.Tx) error {
					return tx.CopyFile(backupPath, 0644)
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to create backup")
				}
				// ---------------------------
				if dirContent, err := os.ReadDir(c.cfg.RootDir); err != nil {
					c.logger.Error().Err(err).Msg("Failed to read backup files")
				} else {
					backupFiles := make([]string, 0, len(dirContent)-1)
					for _, file := range dirContent {
						if strings.HasSuffix(file.Name(), ".backup") {
							backupFiles = append(backupFiles, file.Name())
						}
					}
					slices.Sort(backupFiles)
					// Clean up old backup files by keeping only the last N backups
					for i := 0; i < len(backupFiles)-c.cfg.backupCount; i++ {
						backupFile := filepath.Join(c.cfg.RootDir, backupFiles[i])
						if err := os.Remove(backupFile); err != nil {
							c.logger.Error().Err(err).Str("backupFile", backupFile).Msg("Failed to delete backup file")
						} else {
							c.logger.Debug().Str("backupFile", backupFile).Msg("Deleted old backup file")
						}
					}
				}
				// ---------------------------
			}
		}
	}()
}

func (c *ClusterNode) Close() error {
	if err := c.nodedb.Close(); err != nil {
		return fmt.Errorf("could not close node db: %w", err)
	}
	return nil
}

// ---------------------------
