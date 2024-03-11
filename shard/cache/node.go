package cache

// TODO: rename file to nodes.go

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
	if vecVal == nil {
		return node, fmt.Errorf("could not get vector %d", nodeId)
	}
	node.Vector = conversion.BytesToFloat32(vecVal)
	// ---------------------------
	// Get edges
	edgeVal := graphBucket.Get(NodeKey(nodeId, 'e'))
	if edgeVal == nil {
		return node, fmt.Errorf("could not get edges %d", nodeId)
	}
	node.edges = conversion.BytesToEdgeList(edgeVal)
	// ---------------------------
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
	/* Sharing suffix keys with the same point id does not work because the
	 * underlying array the slice points to gets modified and badger does not
	 * like that. For example, suffixedKey[17] = 'e' and re-use, will not work. */
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

// This function is used to check if the edges of a point are valid. That is,
// are any of the nodes have edges to deletedSet.
func scanNodeEdges(graphBucket diskstore.ReadOnlyBucket, deleteSet map[uint64]struct{}) (toPrune, toSave []uint64, err error) {
	// ---------------------------
	/* toPrune is a list of nodes that have edges to nodes in the delete set.
	 * toSave are nodes that have no inbound edges left.
	 * For example, A -> B -> C, if B is in the delete set, A is in toPrune and C
	 * is in toSave. */
	// We set capacity to the length of the delete set because we guess there is
	// at least one node pointing to each deleted node.
	toPrune = make([]uint64, 0, len(deleteSet))
	validNodes := make(map[uint64]struct{})
	hasInbound := make(map[uint64]struct{})
	// ---------------------------
	// Scan all edges
	err = graphBucket.ForEach(func(k, v []byte) error {
		if k[len(k)-1] == 'e' {
			// Check if the point is in the delete set
			nodeId := NodeIdFromKey(k)
			if _, ok := deleteSet[nodeId]; ok {
				return nil
			}
			// We skip the start node because it will never need saving
			if nodeId != 1 {
				validNodes[nodeId] = struct{}{}
			}
			// Check if the edges are in the delete set
			edges := conversion.BytesToEdgeList(v)
			addedToPrune := false
			for _, edgeId := range edges {
				hasInbound[edgeId] = struct{}{}
				if !addedToPrune {
					if _, ok := deleteSet[edgeId]; ok {
						toPrune = append(toPrune, nodeId)
						addedToPrune = true
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return
	}
	// ---------------------------
	toSave = make([]uint64, 0)
	for nodeId := range validNodes {
		if _, ok := hasInbound[nodeId]; !ok {
			toSave = append(toSave, nodeId)
		}
	}
	// ---------------------------
	return
}
