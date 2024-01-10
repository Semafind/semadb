package cache

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"go.etcd.io/bbolt"
)

/* The reason we have a ShardPoint struct is because we need to store the node
 * id in the database. The node id is not part of the point struct. Having
 * uint64 ids helps us use more efficient data structures compared to raw UUIDs
 * when traversing the graph. */

// Represents a single point in the shard graph structure.
type ShardPoint struct {
	NodeId uint64
	models.Point
	edges []uint64
}

var ErrNotFound = errors.New("not found")

// ---------------------------
/* What is a node vs a point? A point is a unit of data that is stored in the
 * shard as the user sees. They have unique UUIDs. A node wraps a point to be
 * indexed in a graph structure. A node has a unique node id and edges to other
 * nodes. */

/* Storage map:
 * - n<node_id>v: vector
 * - n<node_id>e: edges
 * - n<node_id>m: metadata
 * - n<node_id>i: point UUID
 * - p<point_uuid>i: node id
 */
// ---------------------------

func PointKey(id uuid.UUID, suffix byte) []byte {
	key := [18]byte{}
	key[0] = 'p'
	copy(key[1:], id[:])
	key[17] = suffix
	return key[:]
}

func NodeKey(id uint64, suffix byte) []byte {
	key := [10]byte{}
	key[0] = 'n'
	binary.LittleEndian.PutUint64(key[1:], id)
	key[9] = suffix
	return key[:]
}

// ---------------------------

func getNode(b *bbolt.Bucket, nodeId uint64) (ShardPoint, error) {
	// ---------------------------
	shardPoint := ShardPoint{NodeId: nodeId}
	// ---------------------------
	// Get vector
	vecVal := b.Get(NodeKey(nodeId, 'v'))
	if vecVal == nil {
		return shardPoint, fmt.Errorf("could not get vector %d", nodeId)
	}
	shardPoint.Vector = bytesToFloat32(vecVal)
	// ---------------------------
	// Get edges
	edgeVal := b.Get(NodeKey(nodeId, 'e'))
	if edgeVal == nil {
		return shardPoint, fmt.Errorf("could not get edges %d", nodeId)
	}
	shardPoint.edges = bytesToEdgeList(edgeVal)
	// ---------------------------
	// Get point id
	pointIdBytes := b.Get(NodeKey(nodeId, 'i'))
	if pointIdBytes == nil {
		return shardPoint, fmt.Errorf("could not get point id %d", nodeId)
	}
	shardPoint.Id = uuid.UUID(pointIdBytes)
	// ---------------------------
	return shardPoint, nil
}

func getPointByUUID(b *bbolt.Bucket, id uuid.UUID) (ShardPoint, error) {
	// ---------------------------
	// Get node id
	nodeIdBytes := b.Get(PointKey(id, 'i'))
	if nodeIdBytes == nil {
		return ShardPoint{}, fmt.Errorf("could not get node id %s", id)
	}
	nodeId := BytesToUint64(nodeIdBytes)
	// ---------------------------
	sp, err := getNode(b, nodeId)
	if err != nil {
		return ShardPoint{}, err
	}
	if sp.Id != id {
		return ShardPoint{}, fmt.Errorf("point id mismatch %s != %s", sp.Id, id)
	}
	return sp, nil
}

func setPointEdges(b *bbolt.Bucket, point ShardPoint) error {
	// ---------------------------
	// Set edges
	if err := b.Put(NodeKey(point.NodeId, 'e'), edgeListToBytes(point.edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	return nil
}

func setPoint(b *bbolt.Bucket, point ShardPoint) error {
	// ---------------------------
	/* Sharing suffix keys with the same point id does not work because the
	 * underlying array the slice points to gets modified and badger does not
	 * like that. For example, suffixedKey[17] = 'e' and re-use, will not work. */
	// ---------------------------
	// Set vector
	if err := b.Put(NodeKey(point.NodeId, 'v'), float32ToBytes(point.Vector)); err != nil {
		return fmt.Errorf("could not set vector: %w", err)
	}
	// ---------------------------
	// Set edges
	if err := b.Put(NodeKey(point.NodeId, 'e'), edgeListToBytes(point.edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	// Set external UUID
	if err := b.Put(PointKey(point.Id, 'i'), Uint64ToBytes(point.NodeId)); err != nil {
		return fmt.Errorf("could not set point id: %w", err)
	}
	if err := b.Put(NodeKey(point.NodeId, 'i'), point.Id[:]); err != nil {
		return fmt.Errorf("could not set node id: %w", err)
	}
	// ---------------------------
	// Set metadata if any
	if point.Metadata != nil {
		if err := b.Put(NodeKey(point.NodeId, 'm'), point.Metadata); err != nil {
			return fmt.Errorf("could not set metadata: %w", err)
		}
	} else {
		if err := b.Delete(NodeKey(point.NodeId, 'm')); err != nil {
			return fmt.Errorf("could not delete metadata on nil: %w", err)
		}
	}
	return nil
}

func deletePoint(b *bbolt.Bucket, point ShardPoint) error {
	// ---------------------------
	// Delete vector
	if err := b.Delete(NodeKey(point.NodeId, 'v')); err != nil {
		return fmt.Errorf("could not delete vector: %w", err)
	}
	// ---------------------------
	// Delete edges
	if err := b.Delete(NodeKey(point.NodeId, 'e')); err != nil {
		return fmt.Errorf("could not delete edges: %w", err)
	}
	// ---------------------------
	// Delete metadata
	if err := b.Delete(NodeKey(point.NodeId, 'm')); err != nil {
		return fmt.Errorf("could not delete metadata: %w", err)
	}
	// ---------------------------
	// Delete external UUID
	if err := b.Delete(PointKey(point.Id, 'i')); err != nil {
		return fmt.Errorf("could not delete point id: %w", err)
	}
	if err := b.Delete(NodeKey(point.NodeId, 'i')); err != nil {
		return fmt.Errorf("could not delete node id: %w", err)
	}
	// ---------------------------
	return nil
}

func getPointMetadata(b *bbolt.Bucket, nodeId uint64) ([]byte, error) {
	// ---------------------------
	// Get metadata
	metadataVal := b.Get(NodeKey(nodeId, 'm'))
	// ---------------------------
	return metadataVal, nil
}

// This function is used to check if the edges of a point are valid. It is
// called after a delete operation to make sure that the edges pointing to the
// deleted items are removed.
func scanPointEdges(b *bbolt.Bucket, deleteSet map[uint64]struct{}) ([]uint64, error) {
	// ---------------------------
	// We set capacity to the length of the delete set because we guess there is
	// at least one node pointing to each deleted node.
	toPrune := make([]uint64, 0, len(deleteSet))
	// ---------------------------
	// Scan all edges
	err := b.ForEach(func(k, v []byte) error {
		if k[len(k)-1] == 'e' {
			// Check if the point is in the delete set
			nodeId := binary.LittleEndian.Uint64(k[1 : len(k)-1])
			if _, ok := deleteSet[nodeId]; ok {
				return nil
			}
			// Check if the edges are in the delete set
			edges := bytesToEdgeList(v)
			for _, edgeId := range edges {
				if _, ok := deleteSet[edgeId]; ok {
					toPrune = append(toPrune, nodeId)
					return nil
				}
			}
		}
		return nil
	})
	// ---------------------------
	return toPrune, err
}
