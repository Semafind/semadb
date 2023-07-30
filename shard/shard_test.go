package shard

import (
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/assert"
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

func TestShard_CreatePoint(t *testing.T) {
	shard, err := NewShard(t.TempDir(), sampleCol)
	assert.NoError(t, err)
	p := models.Point{
		Id:     uuid.New(),
		Vector: []float32{1.0, 2.0},
	}
	ps := make([]models.Point, 1)
	ps[0] = p
	err = shard.UpsertPoints(ps)
	assert.NoError(t, err)
}
