package vamana

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/vectorstore"
)

type graphNode struct {
	Id      uint64
	edges   []uint64
	isDirty bool
	edgesMu sync.RWMutex
	// ---------------------------
	// We keep a cache of the neighbours to avoid repeated lookups. This speeds
	// up performance significantly if items are cached and accessed multiple
	// times, e.g. during insert or repeated searches.
	neighbours    []vectorstore.VectorStorePoint
	neighLoadMu   sync.Mutex
	isNeighLoaded atomic.Bool
}

// ---------------------------
/* These functions assume a lock is held. We don't lock and unlock in each
 * function because the operations usually add multiple edges over iterations,
 * e.g. robust prune.
 *
 * If in doubt, run go test -race to check for race conditions assuming we have
 * concurrent tests that simulate parallel access.
 */

func (g *graphNode) LoadNeighbours(vstore vectorstore.VectorStore) error {
	if g.isNeighLoaded.Load() {
		return nil
	}
	g.neighLoadMu.Lock()
	defer g.neighLoadMu.Unlock()
	// We have this check again in case whilst waiting for a lock, another
	// goroutine has loaded the neighbours.
	if g.isNeighLoaded.Load() {
		return nil
	}
	ns, err := vstore.GetMany(g.edges...)
	if err != nil {
		return fmt.Errorf("could not load node neighbours: %w", err)
	}
	g.neighbours = ns
	g.isNeighLoaded.Store(true)
	return nil
}

func (g *graphNode) ClearNeighbours() {
	g.edges = g.edges[:0]
	g.neighbours = g.neighbours[:0]
	g.isDirty = true
	// When clearing, there won't be any neighbours to load, so they are deemed
	// loaded.
	g.isNeighLoaded.Store(true)
}

func (g *graphNode) AddNeighbour(neighbour vectorstore.VectorStorePoint) int {
	g.edges = append(g.edges, neighbour.Id())
	g.neighbours = append(g.neighbours, neighbour)
	g.isDirty = true
	return len(g.edges)
}

func (g *graphNode) AddNeighbourIfNotExists(neighbour vectorstore.VectorStorePoint) int {
	for _, n := range g.edges {
		if n == neighbour.Id() {
			return len(g.edges)
		}
	}
	return g.AddNeighbour(neighbour)
}

// ---------------------------

// ---------------------------
/* A node is graph node that is
 * indexed in a graph structure. A node has a unique node id and edges to other
 * nodes.

 * Storage map:
 * bucket:
 * - n<node_id>e: edges
 *
 */
// ---------------------------

func (g *graphNode) IdFromKey(key []byte) (uint64, bool) {
	return conversion.NodeIdFromKey(key, 'e')
}

func (g *graphNode) SizeInMemory() int64 {
	return int64(len(g.edges)*8) + 16
}

func (g *graphNode) CheckAndClearDirty() bool {
	if g.isDirty {
		g.isDirty = false
		return true
	}
	return false
}

func (g *graphNode) ReadFrom(id uint64, bucket diskstore.Bucket) (node *graphNode, err error) {
	node = &graphNode{Id: id}
	edgeBytes := bucket.Get(conversion.NodeKey(id, 'e'))
	if edgeBytes != nil {
		node.edges = conversion.BytesToEdgeList(edgeBytes)
	} else {
		err = cache.ErrNotFound
	}
	return
}
func (g *graphNode) WriteTo(id uint64, bucket diskstore.Bucket) error {
	edgeBytes := conversion.EdgeListToBytes(g.edges)
	if err := bucket.Put(conversion.NodeKey(id, 'e'), edgeBytes); err != nil {
		return fmt.Errorf("could not write edges: %w", err)
	}
	return nil
}

func (g *graphNode) DeleteFrom(id uint64, bucket diskstore.Bucket) error {
	if err := bucket.Delete(conversion.NodeKey(id, 'e')); err != nil {
		return fmt.Errorf("could not delete vector: %w", err)
	}
	return nil
}

// ---------------------------

// This function is used to check if the edges of a point are valid. That is,
// are any of the nodes have edges to deletedSet.
// NOTE: This loads the entire graph into the cache.
func (v *IndexVamana) EdgeScan(deleteSet map[uint64]struct{}) (toPrune, toSave []uint64, err error) {
	// ---------------------------
	/* toPrune is a list of nodes that have edges to nodes in the delete set.
	 * toSave are nodes that have no inbound edges left.
	 * For example, A -> B -> C, if B is in the delete set, A is in toPrune and C
	 * is in toSave.
	 *
	 * This is probably one of the most inefficient components of the index but
	 * it's correct. One can ignore this edge scanning business but may obtain
	 * disconnected graphs.*/
	// ---------------------------
	// We set capacity to the length of the delete set because we guess there is
	// at least one node pointing to each deleted node.
	toPrune = make([]uint64, 0, len(deleteSet))
	validNodes := make(map[uint64]struct{})
	hasInbound := make(map[uint64]struct{})
	// ---------------------------
	/* We first collect all the point ids in this bucket. Recall that some may be
	 * in cache and some may be flushed out. So we first scan the cache then go
	 * on to scan the bucket. */
	// ---------------------------
	err = v.nodeStore.ForEach(func(id uint64, node *graphNode) error {
		if _, ok := deleteSet[id]; ok {
			return nil
		}
		// ---------------------------
		validNodes[id] = struct{}{}
		/* We now check if the neighbours of this point are in the delete set. If
		 * they are, we add this point to the toPrune list whilst also
		 * maintaining which nodes have inbound edges. */
		node.edgesMu.RLock()
		addedToPrune := false
		for _, edgeId := range node.edges {
			hasInbound[edgeId] = struct{}{}
			if !addedToPrune {
				if _, inDeleteSet := deleteSet[edgeId]; inDeleteSet {
					toPrune = append(toPrune, id)
					addedToPrune = true
				}
			}
		}
		node.edgesMu.RUnlock()
		// ---------------------------
		return nil
	})
	if err != nil {
		return
	}
	// ---------------------------
	toSave = make([]uint64, 0)
	for nodeId := range validNodes {
		if _, ok := hasInbound[nodeId]; !ok && nodeId != STARTID {
			toSave = append(toSave, nodeId)
		}
	}
	// ---------------------------
	return
}
