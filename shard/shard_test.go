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
		b := tx.Bucket(POINTSKEY)
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

func TestShard_CreatePoint(t *testing.T) {
	shard, err := NewShard(t.TempDir(), sampleCol)
	assert.NoError(t, err)
	assert.Equal(t, 0, getShardSize(shard))
	err = shard.InsertPoints(randPoints(2))
	assert.NoError(t, err)
	assert.Equal(t, 2, getShardSize(shard))
	assert.NoError(t, shard.Close())
}

func TestShard_Persistence(t *testing.T) {
	shardDir := t.TempDir()
	shard, _ := NewShard(shardDir, sampleCol)
	err := shard.InsertPoints(randPoints(7))
	assert.NoError(t, err)
	assert.NoError(t, shard.Close())
	shard, err = NewShard(shardDir, sampleCol)
	assert.NoError(t, err)
	assert.Equal(t, 7, getShardSize(shard))
	assert.NoError(t, shard.Close())
}

func TestShard_DuplicatePointId(t *testing.T) {
	shard, _ := NewShard(t.TempDir(), sampleCol)
	points := randPoints(2)
	points[0].Id = points[1].Id
	err := shard.InsertPoints(points)
	assert.Error(t, err)
	assert.Equal(t, 0, getShardSize(shard))
	assert.NoError(t, shard.Close())
}

func TestShard_BasicSearch(t *testing.T) {
	shard, _ := NewShard(t.TempDir(), sampleCol)
	points := randPoints(2)
	points[0].Metadata = []byte("test")
	shard.InsertPoints(points)
	res, err := shard.SearchPoints(points[0].Vector, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, points[0].Id, res[0].Id)
	assert.Equal(t, points[0].Vector, res[0].Vector)
	assert.Equal(t, points[0].Metadata, res[0].Metadata)
	assert.NoError(t, shard.Close())
}

func TestShard_UpdatePoint(t *testing.T) {
	shard, _ := NewShard(t.TempDir(), sampleCol)
	points := randPoints(2)
	err := shard.InsertPoints(points[:1])
	assert.NoError(t, err)
	updateRes, err := shard.UpdatePoints(points)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(updateRes))
	_, ok := updateRes[points[0].Id]
	assert.False(t, ok)
	_, ok = updateRes[points[1].Id]
	assert.True(t, ok)
	assert.NoError(t, shard.Close())
}
