package cluster

import (
	"fmt"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/shard"
)

func (c *ClusterNode) LoadShard(shardDir string) (*shard.Shard, error) {
	log.Debug().Str("shardDir", shardDir).Str("host", c.MyHostname).Msg("LoadShard")
	c.shardLock.Lock()
	defer c.shardLock.Unlock()
	if shard, ok := c.shardStore[shardDir]; ok {
		return shard, nil
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
	c.shardStore[shardDir] = shard
	return shard, nil
}
