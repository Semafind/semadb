package vamana

import (
	"fmt"
	"time"

	"github.com/semafind/semadb/shard/cache"
)

// This point has a neighbour that is being deleted. We need to pool together
// the deleted neighbour neighbours and prune the point.
func (v *IndexVamana) pruneDeleteNeighbour(pc cache.SharedPointCache, id uint64, deleteSet map[uint64]struct{}) error {
	// ---------------------------
	pointA, err := pc.GetPoint(id)
	if err != nil {
		return fmt.Errorf("could not get point: %w", err)
	}
	// ---------------------------
	/* We are going to build a new candidate list of neighbours and then robust
	 * prune it. What is happening here is A -> B -> C, and B is being deleted.
	 * So we add existing neighbours of B to the candidate list. If C is also
	 * deleted, we refrain from recursing because we don't know how deep we will
	 * go. We instead use a saving mechanism to reconnect any potential left
	 * behind nodes in removeInBoundEdges. */
	deletedNothing := true
	// ---------------------------
	err = pc.WithPointNeighbours(pointA, false, func(nnA []*cache.CachePoint) error {
		// These are the neighbours of A
		candidateSet := NewDistSet(pointA.Vector, len(nnA)*2, 0, v.distFn)
		for _, pointB := range nnA {
			// Is B deleted?
			if _, ok := deleteSet[pointB.NodeId]; ok {
				// Pull the neighbours of the deleted neighbour (B), and add them to the candidate set
				err := pc.WithPointNeighbours(pointB, true, func(nnB []*cache.CachePoint) error {
					// Check if those neighbours are in the delete set, if not add them
					// to the candidate set. We do this check in case our neighbour has
					// neighbours that are being deleted too.
					for _, pointC := range nnB {
						// Is C a valid candidate or is it deleted too?
						if _, ok := deleteSet[pointC.NodeId]; !ok {
							candidateSet.AddPoint(pointC)
						}
					}
					deletedNothing = false
					return nil
				})
				if err != nil {
					return fmt.Errorf("could not get deleted neighbour point neighbours: %w", err)
				}
			} else {
				// Nope, B is not deleted so we just add it to the candidate set
				candidateSet.AddPoint(pointB)
			}
		}
		candidateSet.Sort()
		// ---------------------------
		if candidateSet.Len() > v.parameters.DegreeBound {
			// We need to prune the neighbour as well to keep the degree bound
			robustPrune(pointA, candidateSet, v.parameters.Alpha, v.parameters.DegreeBound, v.distFn)
		} else {
			// There is enough space for the candidate neighbours
			pointA.ClearNeighbours()
			for _, dse := range candidateSet.items {
				if dse.point.NodeId == pointA.NodeId {
					// We don't want to add the point to itself, it may be here
					// if a deleted node was pointing back to us. Recall that we
					// add bi-directional edges.
					continue
				}
				pointA.AddNeighbour(dse.point)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not get point neighbours: %w", err)
	}
	if deletedNothing {
		// No neighbours are to be deleted, something's up
		return fmt.Errorf("no neighbours to be deleted for point: %d", pointA.NodeId)
	}
	// ---------------------------
	return nil
}

// Attempts to remove the edges of the deleted points. This is done by scanning
// all the edges and removing the ones that point to a deleted point.
func (v *IndexVamana) removeInboundEdges(pc cache.SharedPointCache, deleteSet map[uint64]struct{}) error {
	// The scanning may not be efficient but it is correct. We can optimise this
	// in the future.
	// ---------------------------
	startTime := time.Now()
	toPrune, toSave, err := pc.EdgeScan(deleteSet)
	if err != nil {
		return fmt.Errorf("could not scan edges: %w", err)
	}
	v.logger.Debug().Int("deleteSetSize", len(deleteSet)).Int("toPruneSize", len(toPrune)).Int("toSaveSize", len(toSave)).Str("duration", time.Since(startTime).String()).Msg("EdgeScan")
	// ---------------------------
	startTime = time.Now()
	for _, pointId := range toPrune {
		if err := v.pruneDeleteNeighbour(pc, pointId, deleteSet); err != nil {
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
		startNode, err := pc.GetPoint(STARTID)
		if err != nil {
			return fmt.Errorf("could not get start node for saving: %w", err)
		}
		for _, pointId := range toSave {
			point, err := pc.GetPoint(pointId)
			if err != nil {
				return fmt.Errorf("could not get point for saving: %w", err)
			}
			// You have been saved
			startNode.AddNeighbourIfNotExists(point)
		}
	}
	// ---------------------------
	return nil
}
