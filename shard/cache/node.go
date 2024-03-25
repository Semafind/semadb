package cache

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
)

// Represents a single point / node in the shard graph structure.
type GraphNode struct {
	NodeId uint64
	Vector []float32
	edges  []uint64
}

var ErrNotFound = errors.New("not found")

// ---------------------------
/* What is a node vs a point? A point is a unit of data that is stored in the
 * shard as the user sees. They have unique UUIDs. A node is graph node that is
 * indexed in a graph structure. A node has a unique node id and edges to other
 * nodes. */

/* Storage map:
 * graphBucket:
 * - n<node_id>v: vector
 * - n<node_id>e: edges
 *
 * We are placing vector and edges as suffixes so when we fetch one, the other
 * is likely to be cached by the memory mapped file as they would in the same
 * page.
 */
// ---------------------------

func NodeKey(id uint64, suffix byte) []byte {
	key := [10]byte{}
	key[0] = 'n'
	binary.LittleEndian.PutUint64(key[1:], id)
	key[9] = suffix
	return key[:]
}

func NodeIdFromKey(key []byte) uint64 {
	return binary.LittleEndian.Uint64(key[1 : len(key)-1])
}

// ---------------------------

func getNode(graphBucket diskstore.ReadOnlyBucket, nodeId uint64) (GraphNode, error) {
	// ---------------------------
	node := GraphNode{NodeId: nodeId}
	// ---------------------------
	// Get vector
	vecVal := graphBucket.Get(NodeKey(nodeId, 'v'))
	if vecVal != nil {
		node.Vector = conversion.BytesToFloat32(vecVal)
	}
	// ---------------------------
	// Get edges
	edgeVal := graphBucket.Get(NodeKey(nodeId, 'e'))
	if edgeVal != nil {
		node.edges = conversion.BytesToEdgeList(edgeVal)
	}
	// ---------------------------
	if len(node.Vector) == 0 && len(node.edges) == 0 {
		return node, ErrNotFound
	}
	return node, nil
}

func setNodeEdges(graphBucket diskstore.Bucket, node GraphNode) error {
	// ---------------------------
	// Set edges
	if err := graphBucket.Put(NodeKey(node.NodeId, 'e'), conversion.EdgeListToBytes(node.edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	return nil
}

func setNode(graphBucket diskstore.Bucket, node GraphNode) error {
	// ---------------------------
	// Set vector
	if err := graphBucket.Put(NodeKey(node.NodeId, 'v'), conversion.Float32ToBytes(node.Vector)); err != nil {
		return fmt.Errorf("could not set vector: %w", err)
	}
	// ---------------------------
	// Set edges
	if err := graphBucket.Put(NodeKey(node.NodeId, 'e'), conversion.EdgeListToBytes(node.edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	return nil
}

func deleteNode(graphBucket diskstore.Bucket, node GraphNode) error {
	// ---------------------------
	// Delete vector
	if err := graphBucket.Delete(NodeKey(node.NodeId, 'v')); err != nil {
		return fmt.Errorf("could not delete vector: %w", err)
	}
	// ---------------------------
	// Delete edges
	if err := graphBucket.Delete(NodeKey(node.NodeId, 'e')); err != nil {
		return fmt.Errorf("could not delete edges: %w", err)
	}
	// ---------------------------
	return nil
}
