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

func getPointCount(shard *Shard) (count int64) {
	shard.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSKEY)
		b.ForEach(func(k, v []byte) error {
			if k[len(k)-1] == 'v' {
				count++
			}
			return nil
		})
		return nil
	})
	count-- // Subtract one for the start point
	return
}

func checkPointCount(t *testing.T, shard *Shard, expected int64) {
	assert.Equal(t, expected, getPointCount(shard))
	si, err := shard.Info()
	assert.NoError(t, err)
	assert.Equal(t, expected, si.PointCount)
}

func randPoints(size int) []models.Point {
	points := make([]models.Point, size)
	for i := 0; i < size; i++ {
		randVector := make([]float32, 2)
		randVector[0] = rand.Float32()
		randVector[1] = rand.Float32()
		id := uuid.New()
		randIndex := rand.Intn(16)
		// We're using a slice of the id as random metadata
		points[i] = models.Point{
			Id:       id,
			Vector:   randVector,
			Metadata: id[:randIndex],
		}
	}
	return points
}

func TestShard_CreatePoint(t *testing.T) {
	shard, err := NewShard(t.TempDir(), sampleCol)
	assert.NoError(t, err)
	// Check that the shard is empty
	checkPointCount(t, shard, 0)
	points := randPoints(2)
	err = shard.InsertPoints(points)
	assert.NoError(t, err)
	// Check that the shard has two points
	checkPointCount(t, shard, 2)
	assert.NoError(t, shard.Close())
}

func TestShard_Persistence(t *testing.T) {
	shardDir := t.TempDir()
	shard, _ := NewShard(shardDir, sampleCol)
	points := randPoints(7)
	err := shard.InsertPoints(points)
	assert.NoError(t, err)
	assert.NoError(t, shard.Close())
	shard, err = NewShard(shardDir, sampleCol)
	assert.NoError(t, err)
	// Does the shard still have the points?
	checkPointCount(t, shard, 7)
	assert.NoError(t, shard.Close())
}

func TestShard_DuplicatePointId(t *testing.T) {
	shard, _ := NewShard(t.TempDir(), sampleCol)
	points := randPoints(2)
	points[0].Id = points[1].Id
	err := shard.InsertPoints(points)
	// Insert expects unique ids and should fail
	assert.Error(t, err)
	checkPointCount(t, shard, 0)
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
	assert.Equal(t, points[0].Id, res[0].Point.Id)
	assert.Equal(t, points[0].Vector, res[0].Point.Vector)
	assert.Equal(t, points[0].Metadata, res[0].Point.Metadata)
	assert.EqualValues(t, 0, res[0].Distance)
	assert.NoError(t, shard.Close())
}

func TestShard_SearchMaxLimit(t *testing.T) {
	shard, _ := NewShard(t.TempDir(), sampleCol)
	points := randPoints(2)
	shard.InsertPoints(points)
	res, err := shard.SearchPoints(points[0].Vector, 7)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res))
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

func TestShard_DeletePoint(t *testing.T) {
	shard, _ := NewShard(t.TempDir(), sampleCol)
	points := randPoints(2)
	shard.InsertPoints(points)
	deleteSet := make(map[uuid.UUID]struct{})
	deleteSet[points[0].Id] = struct{}{}
	// delete one point
	err := shard.DeletePoints(deleteSet)
	assert.NoError(t, err)
	checkPointCount(t, shard, 1)
	// Try deleting the same point again
	err = shard.DeletePoints(deleteSet)
	assert.NoError(t, err)
	checkPointCount(t, shard, 1)
	// Delete other point too
	deleteSet[points[1].Id] = struct{}{}
	err = shard.DeletePoints(deleteSet)
	assert.NoError(t, err)
	checkPointCount(t, shard, 0)
	assert.NoError(t, shard.Close())
}
