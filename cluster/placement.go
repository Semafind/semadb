package cluster

import (
	"fmt"

	"github.com/semafind/semadb/models"
)

func distributePoints(shards []shardInfo, points []models.Point, maxShardSize int64, createShardFn func() (string, error)) (map[string][2]int, error) {
	shardAssignments := make(map[string][2]int)
	lastShardIndex, lastPointIndex := 0, 0
	runningSize := shards[lastShardIndex].Size
	for i, point := range points {
		runningSize += int64(len(point.Metadata)+len(point.Vector)*4) + 8 // 8 bytes for id
		if runningSize > maxShardSize {
			// This shard can't take any more points, did it take any points?
			if i > lastPointIndex {
				// This shard took some points
				shardAssignments[shards[lastShardIndex].ShardDir] = [2]int{lastPointIndex, i}
				lastPointIndex = i
			}
			// Did we reach the last shard?
			if lastShardIndex == len(shards)-1 {
				// We need more shards to take the rest of the points
				newShard, err := createShardFn()
				if err != nil {
					return nil, fmt.Errorf("could not create shard: %w", err)
				}
				shards = append(shards, shardInfo{
					ShardDir: newShard,
					Size:     0,
				}) // empty shard
			}
			lastShardIndex++
			runningSize = shards[lastShardIndex].Size
		}
	}
	if lastPointIndex < len(points) {
		shardAssignments[shards[lastShardIndex].ShardDir] = [2]int{lastPointIndex, len(points)}
	}
	return shardAssignments, nil
}
