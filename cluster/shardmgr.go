package cluster

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard"
)

type loadedShard struct {
	shard  *shard.Shard
	doneCh chan bool
	mu     sync.RWMutex // This locks stops the cleanup goroutine from unloading the shard while it is being used
}

type ShardManagerConfig struct {
	// Shard timeout in seconds
	ShardTimeout int `yaml:"shardTimeout"`
}

type ShardManager struct {
	logger zerolog.Logger
	cfg    ShardManagerConfig
	// ---------------------------
	shardStore map[string]*loadedShard
	shardLock  sync.Mutex
}

func NewShardManager(config ShardManagerConfig) *ShardManager {
	logger := log.With().Str("component", "shardManager").Logger()
	return &ShardManager{
		logger:     logger,
		cfg:        config,
		shardStore: make(map[string]*loadedShard),
	}
}

// Load a shard into memory. If the shard is already loaded, the shard is
// returned from the local cache. The shard is unloaded after a timeout if it is
// not used.
func (sm *ShardManager) loadShard(collection models.Collection, shardId string) (*loadedShard, error) {
	shardDir := filepath.Join("userCollections", collection.UserId, collection.Id, shardId)
	sm.logger.Debug().Str("shardDir", shardDir).Msg("LoadShard")
	sm.shardLock.Lock()
	defer sm.shardLock.Unlock()
	if ls, ok := sm.shardStore[shardDir]; ok {
		// We reset the timer here so that the shard is not unloaded prematurely
		sm.logger.Debug().Str("shardDir", shardDir).Msg("Returning cached shard")
		ls.doneCh <- false
		return ls, nil
	}
	// ---------------------------
	// Open shard
	shard, err := shard.NewShard(shardDir, collection)
	if err != nil {
		return nil, fmt.Errorf("could not open shard: %w", err)
	}
	ls := &loadedShard{
		shard:  shard,
		doneCh: make(chan bool),
	}
	sm.shardStore[shardDir] = ls
	// ---------------------------
	// Setup cleanup goroutine
	go func() {
		timeoutDuration := time.Duration(sm.cfg.ShardTimeout) * time.Second
		timer := time.NewTimer(timeoutDuration)
		for {
			shutdown := false
			select {
			case isDone := <-ls.doneCh:
				// The following condition comes from the documentation of timer.Stop()
				if !timer.Stop() {
					<-timer.C
				}
				if isDone {
					shutdown = true
				} else {
					sm.logger.Debug().Str("shardDir", shardDir).Msg("Resetting shard timeout")
					// Timer must be stopped or expired before it can be reset
					timer.Reset(timeoutDuration)
				}
			case <-timer.C:
				shutdown = true
			}
			if shutdown {
				sm.logger.Debug().Str("shardDir", shardDir).Msg("Unloading shard")
				sm.shardLock.Lock()
				delete(sm.shardStore, shardDir)
				sm.shardLock.Unlock()
				ls.mu.Lock()
				if err := ls.shard.Close(); err != nil {
					sm.logger.Error().Err(err).Str("shardDir", shardDir).Msg("Failed to close shard")
				} else {
					sm.logger.Debug().Str("shardDir", shardDir).Msg("Closed shard")
				}
				ls.mu.Unlock()
				break
			}
		}
	}()
	return ls, nil
}

// DoWithShard executes a function with a shard. The shard is loaded if it is
// not already loaded and prevents the shard from being cleaned up while the
// function is executing.
func (sm *ShardManager) DoWithShard(collection models.Collection, shardId string, f func(*shard.Shard) error) error {
	ls, err := sm.loadShard(collection, shardId)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return f(ls.shard)
}
