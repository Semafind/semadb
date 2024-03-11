package cache

// TODO: rename file to nodes.go

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/semafind/semadb/diskstore"
)

/* The reason we have a ShardPoint struct is because we need to store the node
 * id in the database. The node id is not part of the point struct. Having
 * uint64 ids helps us use more efficient data structures compared to raw UUIDs
 * when traversing the graph. */

// TODO: Rename shard point to node and NodeId to Id
// Represents a single point in the shard graph structure.
type ShardPoint struct {
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

// TODO: Remove n prefix in storage map
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

func getNode(graphBucket diskstore.ReadOnlyBucket, nodeId uint64) (ShardPoint, error) {
	// ---------------------------
	shardPoint := ShardPoint{NodeId: nodeId}
	// ---------------------------
	// Get vector
	vecVal := graphBucket.Get(NodeKey(nodeId, 'v'))
	if vecVal == nil {
		return shardPoint, fmt.Errorf("could not get vector %d", nodeId)
	}
	shardPoint.Vector = bytesToFloat32(vecVal)
	// ---------------------------
	// Get edges
	edgeVal := graphBucket.Get(NodeKey(nodeId, 'e'))
	if edgeVal == nil {
		return shardPoint, fmt.Errorf("could not get edges %d", nodeId)
	}
	shardPoint.edges = bytesToEdgeList(edgeVal)
	// ---------------------------
	return shardPoint, nil
}

func setPointEdges(graphBucket diskstore.Bucket, point ShardPoint) error {
	// ---------------------------
	// Set edges
	if err := graphBucket.Put(NodeKey(point.NodeId, 'e'), edgeListToBytes(point.edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	return nil
}

func setPoint(graphBucket diskstore.Bucket, point ShardPoint) error {
	// ---------------------------
	/* Sharing suffix keys with the same point id does not work because the
	 * underlying array the slice points to gets modified and badger does not
	 * like that. For example, suffixedKey[17] = 'e' and re-use, will not work. */
	// ---------------------------
	// Set vector
	if err := graphBucket.Put(NodeKey(point.NodeId, 'v'), float32ToBytes(point.Vector)); err != nil {
		return fmt.Errorf("could not set vector: %w", err)
	}
	// ---------------------------
	// Set edges
	if err := graphBucket.Put(NodeKey(point.NodeId, 'e'), edgeListToBytes(point.edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	return nil
}

func deletePoint(graphBucket diskstore.Bucket, point ShardPoint) error {
	// ---------------------------
	// Delete vector
	if err := graphBucket.Delete(NodeKey(point.NodeId, 'v')); err != nil {
		return fmt.Errorf("could not delete vector: %w", err)
	}
	// ---------------------------
	// Delete edges
	if err := graphBucket.Delete(NodeKey(point.NodeId, 'e')); err != nil {
		return fmt.Errorf("could not delete edges: %w", err)
	}
	// ---------------------------
	return nil
}

// This function is used to check if the edges of a point are valid. That is,
// are any of the nodes have edges to deletedSet.
func scanPointEdges(graphBucket diskstore.ReadOnlyBucket, deleteSet map[uint64]struct{}) (toPrune, toSave []uint64, err error) {
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
			edges := bytesToEdgeList(v)
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
