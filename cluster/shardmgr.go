package cluster

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/shard"
)

type loadedShard struct {
	shard  *shard.Shard
	doneCh chan bool
	mu     sync.RWMutex // This locks stops the cleanup goroutine from unloading the shard while it is being used
}

func (c *ClusterNode) loadShard(shardDir string) (*loadedShard, error) {
	c.logger.Debug().Str("shardDir", shardDir).Msg("LoadShard")
	c.shardLock.Lock()
	defer c.shardLock.Unlock()
	if ls, ok := c.shardStore[shardDir]; ok {
		// We reset the timer here so that the shard is not unloaded prematurely
		c.logger.Debug().Str("shardDir", shardDir).Msg("Returning cached shard")
		ls.doneCh <- false
		return ls, nil
	}
	// ---------------------------
	// Load corresponding collection
	colPath := filepath.Dir(shardDir)
	collectionId := filepath.Base(colPath)
	userPath := filepath.Dir(colPath)
	userId := filepath.Base(userPath)
	col, err := c.GetCollection(userId, collectionId)
	if err != nil {
		return nil, fmt.Errorf("could not load collection: %w", err)
	}
	// ---------------------------
	// Open shard
	shard, err := shard.NewShard(shardDir, col)
	if err != nil {
		return nil, fmt.Errorf("could not open shard: %w", err)
	}
	ls := &loadedShard{
		shard:  shard,
		doneCh: make(chan bool),
	}
	c.shardStore[shardDir] = ls
	// ---------------------------
	// Setup cleanup goroutine
	go func() {
		timeoutDuration := time.Duration(config.Cfg.ShardTimeout) * time.Second
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
					c.logger.Debug().Str("shardDir", shardDir).Msg("Resetting shard timeout")
					// Timer must be stopped or expired before it can be reset
					timer.Reset(timeoutDuration)
				}
			case <-timer.C:
				shutdown = true
			}
			if shutdown {
				c.logger.Debug().Str("shardDir", shardDir).Msg("Unloading shard")
				c.shardLock.Lock()
				delete(c.shardStore, shardDir)
				c.shardLock.Unlock()
				ls.mu.Lock()
				if err := ls.shard.Close(); err != nil {
					c.logger.Error().Err(err).Str("shardDir", shardDir).Msg("Failed to close shard")
				} else {
					c.logger.Debug().Str("shardDir", shardDir).Msg("Closed shard")
				}
				ls.mu.Unlock()
				break
			}
		}
	}()
	return ls, nil
}

func (c *ClusterNode) DoWithShard(shardDir string, f func(*shard.Shard) error) error {
	c.logger.Debug().Str("shardDir", shardDir).Msg("DoWithShard")
	ls, err := c.loadShard(shardDir)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return f(ls.shard)
}
