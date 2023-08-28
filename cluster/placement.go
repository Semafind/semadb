package cluster

import (
	"fmt"

	"github.com/semafind/semadb/models"
)

func distributePoints(shards []shardInfo, points []models.Point, maxShardSize, maxShardPointCount int64, createShardFn func() (string, error)) (map[string][2]int, error) {
	// ---------------------------
	shardAssignments := make(map[string][2]int)
	// ---------------------------
	if len(shards) == 0 && len(points) > 0 {
		newShard, err := createShardFn()
		if err != nil {
			return nil, fmt.Errorf("could not create shard: %w", err)
		}
		shards = append(shards, shardInfo{
			ShardDir: newShard,
		}) // empty shard
	}
	// ---------------------------
	for lastPointIndex, i := 0, 0; i < len(shards); i++ {
		j := lastPointIndex
		runningSize := shards[i].Size
		runningPointCount := shards[i].PointCount
		for ; j < len(points); j++ {
			runningSize += int64(len(points[j].Metadata) + len(points[j].Vector)*4 + 8) // 8 bytes for id
			runningPointCount++
			if runningSize > maxShardSize || runningPointCount > maxShardPointCount {
				break
			}
		}
		// Did we take any points?
		if j > lastPointIndex {
			shardAssignments[shards[i].ShardDir] = [2]int{lastPointIndex, j}
		}
		lastPointIndex = j
		// If we are the last shard and there are more points, create a new shard
		if i == len(shards)-1 && lastPointIndex < len(points) {
			newShard, err := createShardFn()
			if err != nil {
				return nil, fmt.Errorf("could not create shard: %w", err)
			}
			shards = append(shards, shardInfo{
				ShardDir: newShard,
			}) // empty shard
		}
	}
	// ---------------------------
	return shardAssignments, nil
}
