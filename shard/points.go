package shard

/* You can think of points as an index on the UUIDs of the points. This is why it
 * is placed under the index package. */

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
)

var ErrPointDoesNotExist = errors.New("point does not exist")

/* The reason we have a ShardPoint struct is because we need to store the node
 * id in the database. Having uint64 ids helps us use more efficient data
 * structures compared to raw UUIDs when traversing the graph. */

// A shard point wraps a point with a node id.
type ShardPoint struct {
	models.Point
	NodeId uint64
}

/* Storage map:
 * points:
 * - n<node_id>i: point UUID
 * - n<node_id>d: data
 * - p<point_uuid>i: node id
 */

func PointKey(id uuid.UUID, suffix byte) []byte {
	key := [18]byte{}
	key[0] = 'p'
	copy(key[1:], id[:])
	key[17] = suffix
	return key[:]
}

func SetPoint(bucket diskstore.Bucket, point ShardPoint) error {
	// ---------------------------
	// Set matching ids
	if err := bucket.Put(cache.NodeKey(point.NodeId, 'i'), point.Id[:]); err != nil {
		return fmt.Errorf("could not set point id: %w", err)
	}
	if err := bucket.Put(PointKey(point.Id, 'i'), cache.Uint64ToBytes(point.NodeId)); err != nil {
		return fmt.Errorf("could not set node id: %w", err)
	}
	// ---------------------------
	// Handle point data
	if len(point.Data) > 0 {
		if err := bucket.Put(cache.NodeKey(point.NodeId, 'd'), point.Data); err != nil {
			return fmt.Errorf("could not set point data: %w", err)
		}
	} else {
		if err := bucket.Delete(cache.NodeKey(point.NodeId, 'd')); err != nil {
			return fmt.Errorf("could not delete empty point data: %w", err)
		}
	}
	return nil
}

func CheckPointExists(bucket diskstore.ReadOnlyBucket, pointId uuid.UUID) (bool, error) {
	v := bucket.Get(PointKey(pointId, 'i'))
	return v != nil, nil
}

func GetPointNodeIdByUUID(bucket diskstore.ReadOnlyBucket, pointId uuid.UUID) (uint64, error) {
	nodeIdBytes := bucket.Get(PointKey(pointId, 'i'))
	if nodeIdBytes == nil {
		return 0, ErrPointDoesNotExist
	}
	nodeId := cache.BytesToUint64(nodeIdBytes)
	return nodeId, nil
}

func GetPointByUUID(bucket diskstore.ReadOnlyBucket, pointId uuid.UUID) (ShardPoint, error) {
	nodeId, err := GetPointNodeIdByUUID(bucket, pointId)
	if err != nil {
		return ShardPoint{}, err
	}
	data := bucket.Get(cache.NodeKey(nodeId, 'd'))
	sp := ShardPoint{
		Point: models.Point{
			Id:   pointId,
			Data: data,
		},
		NodeId: nodeId,
	}
	return sp, nil
}

func GetPointByNodeId(bucket diskstore.ReadOnlyBucket, nodeId uint64) (ShardPoint, error) {
	pointIdBytes := bucket.Get(cache.NodeKey(nodeId, 'i'))
	if pointIdBytes == nil {
		return ShardPoint{}, ErrPointDoesNotExist
	}
	pointId, err := uuid.FromBytes(pointIdBytes)
	if err != nil {
		return ShardPoint{}, fmt.Errorf("could not parse point id: %w", err)
	}
	data := bucket.Get(cache.NodeKey(nodeId, 'd'))
	sp := ShardPoint{
		Point: models.Point{
			Id:   pointId,
			Data: data,
		},
		NodeId: nodeId,
	}
	return sp, nil
}

func DeletePoint(bucket diskstore.Bucket, pointId uuid.UUID, nodeId uint64) error {
	if err := bucket.Delete(PointKey(pointId, 'i')); err != nil {
		return fmt.Errorf("could not delete point id: %w", err)
	}
	if err := bucket.Delete(cache.NodeKey(nodeId, 'i')); err != nil {
		return fmt.Errorf("could not delete node id: %w", err)
	}
	if err := bucket.Delete(cache.NodeKey(nodeId, 'd')); err != nil {
		return fmt.Errorf("could not delete point data: %w", err)
	}
	return nil
}
