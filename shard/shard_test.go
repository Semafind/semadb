package shard

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var sampleCol models.Collection = models.Collection{
	UserId:     "test",
	Id:         "test",
	VectorSize: 2,
	DistMetric: "euclidean",
	Replicas:   1,
	Algorithm:  "vamana",
	Parameters: models.DefaultVamanaParameters(),
}

func getShardSize(shard *Shard) int {
	size := 0
	shard.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("points"))
		b.ForEach(func(k, v []byte) error {
			if k[len(k)-1] == 'v' {
				size++
			}
			return nil
		})
		return nil
	})
	return size - 1 // -1 for the start point
}

func randPoint() models.Point {
	randVector := make([]float32, 2)
	randVector[0] = rand.Float32()
	randVector[1] = rand.Float32()
	return models.Point{
		Id:     uuid.New(),
		Vector: randVector,
	}
}

func randPoints(size int) []models.Point {
	points := make([]models.Point, size)
	for i := 0; i < size; i++ {
		points[i] = randPoint()
	}
	return points
}

func TestShard_CreatePoint(t *testing.T) {
	shard, err := NewShard(t.TempDir(), sampleCol)
	assert.NoError(t, err)
	assert.Equal(t, 0, getShardSize(shard))
	err = shard.UpsertPoints(randPoints(2))
	assert.NoError(t, err)
	assert.Equal(t, 2, getShardSize(shard))
	assert.NoError(t, shard.Close())
}

func TestShard_Persistence(t *testing.T) {
	shardDir := t.TempDir()
	shard, _ := NewShard(shardDir, sampleCol)
	err := shard.UpsertPoints(randPoints(7))
	assert.NoError(t, err)
	assert.NoError(t, shard.Close())
	shard, err = NewShard(shardDir, sampleCol)
	assert.NoError(t, err)
	assert.Equal(t, 7, getShardSize(shard))
	assert.NoError(t, shard.Close())
}
