package vamana

import (
	"fmt"
	"time"

	"github.com/semafind/semadb/shard/vectorstore"
)

// This point has a neighbour that is being deleted. We need to pool together
// the deleted neighbour neighbours and prune the point.
func (iv *IndexVamana) pruneDeleteNeighbour(pointA vectorstore.VectorStorePoint, nodeA *graphNode, deleteSet map[uint64]struct{}) error {
	// ---------------------------
	/* We are going to build a new candidate list of neighbours and then robust
	 * prune it. What is happening here is A -> B -> C, and B is being deleted.
	 * So we add existing neighbours of B to the candidate list. If C is also
	 * deleted, we refrain from recursing because we don't know how deep we will
	 * go. We instead use a saving mechanism to reconnect any potential left
	 * behind nodes in removeInBoundEdges. */
	// ---------------------------
	nodeA.edgesMu.Lock()
	defer nodeA.edgesMu.Unlock()
	validCandidateIds := make([]uint64, 0, len(nodeA.edges))
	toExpandIds := make([]uint64, 0, len(nodeA.edges)/2)
	for _, nB := range nodeA.edges {
		// Is B deleted?
		if _, ok := deleteSet[nB]; ok {
			// Yes, we need to expand its neighbours
			toExpandIds = append(toExpandIds, nB)
		} else {
			// Nope it is a valid candidate
			validCandidateIds = append(validCandidateIds, nB)
		}
	}
	// ---------------------------
	if len(toExpandIds) == 0 {
		// No neighbours are to be deleted, something's up
		return fmt.Errorf("no neighbours to be deleted for point: %d", pointA.Id())
	}
	// ---------------------------
	expandedNodes, err := iv.nodeStore.GetMany(toExpandIds...)
	if err != nil {
		return fmt.Errorf("could not get neighbour points for prune deletion: %w", err)
	}
	// Now pull the neighbours of B and add them to the candidate set
	for _, nodeB := range expandedNodes {
		nodeB.edgesMu.RLock()
		for _, nC := range nodeB.edges {
			// Is C a valid candidate or is it deleted too?
			if _, ok := deleteSet[nC]; !ok {
				validCandidateIds = append(validCandidateIds, nC)
			}
		}
		nodeB.edgesMu.RUnlock()
	}
	// ---------------------------
	// Potential new candidates for A
	candidateSet := NewDistSet(len(nodeA.edges)*2, 0, iv.vecStore.DistanceFromPoint(pointA))
	vecs, err := iv.vecStore.Get(validCandidateIds...)
	if err != nil {
		return fmt.Errorf("could not get valid candidate points for prune deletion: %w", err)
	}
	candidateSet.Add(vecs...)
	candidateSet.Sort()
	// ---------------------------
	if candidateSet.Len() > iv.parameters.DegreeBound {
		// We need to prune the neighbour as well to keep the degree bound
		iv.robustPrune(nodeA, candidateSet)
	} else {
		// There is enough space for the candidate neighbours
		nodeA.ClearNeighbours()
		for _, dse := range candidateSet.items {
			if dse.Point.Id() == nodeA.Id {
				// We don't want to add the point to itself, it may be here
				// if a deleted node was pointing back to us. Recall that we
				// add bi-directional edges.
				continue
			}
			nodeA.AddNeighbour(dse.Point)
		}
	}
	// ---------------------------
	return nil
}

// Attempts to remove the edges of the deleted points. This is done by scanning
// all the edges and removing the ones that point to a deleted point.
func (v *IndexVamana) removeInboundEdges(deleteSet map[uint64]struct{}) error {
	// The scanning may not be efficient but it is correct. We can optimise this
	// in the future.
	// ---------------------------
	startTime := time.Now()
	toPrune, toSave, err := v.EdgeScan(deleteSet)
	if err != nil {
		return fmt.Errorf("could not scan edges: %w", err)
	}
	v.logger.Debug().Int("deleteSetSize", len(deleteSet)).Int("toPruneSize", len(toPrune)).Int("toSaveSize", len(toSave)).Str("duration", time.Since(startTime).String()).Msg("EdgeScan")
	// ---------------------------
	startTime = time.Now()
	toPrunePoints, err := v.vecStore.Get(toPrune...)
	if err != nil {
		return fmt.Errorf("could not get points to prune: %w", err)
	}
	toPruneNodes, err := v.nodeStore.GetMany(toPrune...)
	if err != nil {
		return fmt.Errorf("could not get nodes to prune: %w", err)
	}
	for i, point := range toPrunePoints {
		if err := v.pruneDeleteNeighbour(point, toPruneNodes[i], deleteSet); err != nil {
			return fmt.Errorf("could not prune delete neighbour: %w", err)
		}
	}
	v.logger.Debug().Int("toPruneSize", len(toPrune)).Str("duration", time.Since(startTime).String()).Msg("PruneDeleteNeighbour")
	// ---------------------------
	/* This saving business happens because when deleting points, we can
	 * potentially create disconnected nodes. That is, all the incoming edges of
	 * a node might be deleted. This becomes rare as the graph grows but it can
	 * happen and means that some points might become unsearchable. For
	 * example this occurs 1 in 100 test case runs:
	 * 1,2
	 * 2,1,3
	 * 3,2,4
	 * 4,3
	 * where the first column is the node Id, the rest are edges. We delete 2
	 * and 3. This leaves 4 disconnected from the graph because
	 * pruneDeleteNeighbour only expands one level deep. Why one level deep?
	 * Because we don't want to expand the entire graph, we only want to expand
	 * the neighbours of the deleted neighbours. If we recursively expand, which
	 * we tried, then you have a chance of expanding many more nodes creating a
	 * huge computation. Instead we are taking the simple option of putting
	 * these few stragglers back to the start node. */
	if len(toSave) > 0 {
		startNode, err := v.nodeStore.Get(STARTID)
		if err != nil {
			return fmt.Errorf("could not get start node for saving: %w", err)
		}
		toSavePoints, err := v.vecStore.Get(toSave...)
		if err != nil {
			return fmt.Errorf("could not get points to save: %w", err)
		}
		for _, point := range toSavePoints {
			if point.Id() == STARTID {
				// We don't want to add the start node to itself, start node
				// never needs saving but may be in the list if no other node is
				// pointing to it.
				continue
			}
			// You have been saved
			startNode.AddNeighbourIfNotExists(point)
		}
	}
	// ---------------------------
	return nil
}
