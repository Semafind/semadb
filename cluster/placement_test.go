package cluster

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
)

func randPoints(size int) []models.Point {
	points := make([]models.Point, size)
	for i := 0; i < size; i++ {
		randVector := make([]float32, 2)
		randVector[0] = rand.Float32()
		randVector[1] = rand.Float32()
		points[i] = models.Point{
			Id:     uuid.New(),
			Vector: randVector,
		}
	}
	return points
}

func randShardInfo(size int64) shardInfo {
	return shardInfo{
		Id:   uuid.New().String(),
		Size: size,
	}
}

func Test_distributePoints(t *testing.T) {
	maxShardSize := int64(16) // 16 bytes is the size of a single point
	maxShardPointCount := int64(1)
	testCases := []struct {
		shardCount      int
		pointCount      int
		initShardSize   int64
		wantAssLen      int
		wantCreateCount int
	}{
		{0, 0, 16, 0, 0}, // 0 shards 0 points
		{1, 0, 16, 0, 0}, // 1 shard 0 points
		{0, 1, 16, 1, 1}, // 0 shards 1 point
		{0, 2, 0, 2, 2},  // 0 shards 2 point
		{1, 1, 0, 1, 0},  // 1 empty shard, 1 point
		{1, 2, 0, 2, 1},  // 1 empty shard, 2 point
		{1, 1, 16, 1, 1}, // 1 full shard, 1 point
		{1, 2, 16, 2, 2}, // 1 full shard, 2 points
		{2, 1, 16, 1, 1}, // 2 full shards, 1 point
		{2, 2, 16, 2, 2}, // 2 full shards, 2 points
		{2, 3, 16, 3, 3}, // 2 full shards, 3 points
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v shards %v points (init size %v) ", tc.shardCount, tc.pointCount, tc.initShardSize), func(t *testing.T) {
			createCount := 0
			createShardFn := func() (string, error) {
				createCount++
				return uuid.New().String(), nil
			}
			shards := make([]shardInfo, tc.shardCount)
			for i := 0; i < tc.shardCount; i++ {
				shards[i] = randShardInfo(tc.initShardSize)
			}
			points := randPoints(tc.pointCount)
			ass, err := distributePoints(shards, points, maxShardSize, maxShardPointCount, createShardFn)
			require.NoError(t, err)
			require.Len(t, ass, tc.wantAssLen)
			for _, assRange := range ass {
				require.Equal(t, 1, assRange[1]-assRange[0], fmt.Sprintf("Assignment range was %v", assRange))
			}
			require.Equal(t, tc.wantCreateCount, createCount, "Create count")
		})
	}
}
