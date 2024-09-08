package pointstore_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/pointstore"
	"github.com/stretchr/testify/require"
)

func checkCount(t *testing.T, b diskstore.Bucket, expected int) {
	var count int
	err := b.ForEach(func(k, v []byte) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, expected, count)
}

func Test_CRUDPoint(t *testing.T) {
	b := diskstore.NewMemBucket(false)
	// ---------------------------
	// Set
	p := pointstore.ShardPoint{
		Point: models.Point{
			Id:   uuid.New(),
			Data: []byte("data"),
		},
		NodeId: 1,
	}
	err := pointstore.SetPoint(b, p)
	require.NoError(t, err)
	checkCount(t, b, 3)
	p.Data = nil
	err = pointstore.SetPoint(b, p)
	require.NoError(t, err)
	checkCount(t, b, 2)
	p.Data = []byte("more data")
	err = pointstore.SetPoint(b, p)
	require.NoError(t, err)
	checkCount(t, b, 3)
	// ---------------------------
	exists, err := pointstore.CheckPointExists(b, p.Id)
	require.NoError(t, err)
	require.True(t, exists)
	// ---------------------------
	nodeId, err := pointstore.GetPointNodeIdByUUID(b, p.Id)
	require.NoError(t, err)
	require.Equal(t, p.NodeId, nodeId)
	_, err = pointstore.GetPointNodeIdByUUID(b, uuid.New())
	require.Error(t, err)
	require.Equal(t, pointstore.ErrPointDoesNotExist, err)
	// ---------------------------
	sp, err := pointstore.GetPointByUUID(b, p.Id)
	require.NoError(t, err)
	require.Equal(t, p.Id, sp.Id)
	require.Equal(t, p.NodeId, sp.NodeId)
	require.Equal(t, p.Data, sp.Data)
	// ---------------------------
	sp, err = pointstore.GetPointByNodeId(b, p.NodeId, true)
	require.NoError(t, err)
	require.Equal(t, p.Id, sp.Id)
	require.Equal(t, p.NodeId, sp.NodeId)
	require.Equal(t, p.Data, sp.Data)
	// ---------------------------
	err = pointstore.DeletePoint(b, p.Id, p.NodeId)
	require.NoError(t, err)
	err = pointstore.DeletePoint(b, p.Id, p.NodeId)
	require.NoError(t, err)
	checkCount(t, b, 0)
}
