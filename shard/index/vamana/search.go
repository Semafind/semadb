package vamana

import (
	"fmt"
)

func (v *IndexVamana) greedySearch(query []float32, k int, searchSize int) (DistSet, DistSet, error) {
	// ---------------------------
	distFn := v.vecStore.DistanceFromFloat(query)
	// Initialise distance set
	searchSet := NewDistSet(searchSize, v.maxNodeId, distFn)
	// The faster visited set is only used for the search and we release it
	// after we're done. This does not affect the items stored in the search
	// set.
	defer searchSet.Release()
	visitedSet := NewDistSet(searchSize*2, 0, distFn)
	// Check that the search size is greater than k
	if searchSize < k {
		return searchSet, visitedSet, fmt.Errorf("searchSize (%d) must be greater than k (%d)", searchSize, k)
	}
	// ---------------------------
	// Start the search with the start point neighbours, recall that the start
	// point is not part of the database but an entry point to the graph.
	// Upstream search function filters it out but we return it here so the
	// graph can be constructed correctly.
	sn, err := v.nodeStore.Get(STARTID)
	if err != nil {
		return searchSet, visitedSet, fmt.Errorf("failed to get start point: %w", err)
	}
	searchSet.AddWithLimit(sn.Id)
	// ---------------------------
	/* This loop looks to curate the closest nodes to the query vector along the
	 * way. The loop terminates when we visited all the nodes in our search list. */
	for i := 0; i < min(len(searchSet.items), searchSize); {
		distElem := searchSet.items[i]
		if distElem.visited {
			i++
			continue
		}
		visitedSet.AddAlreadyUnique(distElem)
		searchSet.items[i].visited = true
		// ---------------------------
		// We have to lock the point here because while we are calculating the
		// distance of its neighbours (edges in the graph) we can't have another
		// goroutine changing them. The case we aren't covering is after we have
		// calculated, they may change the search we are doing is not
		// deterministic. With approximate search this is not a major problem.
		node, err := v.nodeStore.Get(distElem.Id)
		if err != nil {
			return searchSet, visitedSet, fmt.Errorf("failed to get node for neighbours: %w", err)
		}
		node.edgesMu.RLock()
		searchSet.AddWithLimit(node.edges...)
		node.edgesMu.RUnlock()
		// ---------------------------
		i = 0
	}
	// ---------------------------
	visitedSet.Sort()
	return searchSet, visitedSet, nil
}

// Update the edges of the node optimistically based on the candidateSet.
// NOTE: requires node edges to be locked.
func (iv *IndexVamana) robustPrune(node *graphNode, candidateSet DistSet) {
	// ---------------------------
	node.ClearNeighbours() // Reset edges / neighbours
	// ---------------------------
	for i := 0; i < len(candidateSet.items); i++ {
		// ---------------------------
		// Get the closest point
		closestElem := candidateSet.items[i]
		// Exclude the point itself, this might happen in case we are updating.
		// It is worth checking if this is the case.
		if closestElem.pruneRemoved || closestElem.Id == node.Id {
			continue
		}
		edgeCount := node.AddNeighbour(closestElem.Id)
		if edgeCount >= iv.parameters.DegreeBound {
			break
		}
		// ---------------------------
		// Prune optimistically
		distFn := iv.vecStore.DistanceFromPoint(closestElem.Id)
		for j := i + 1; j < len(candidateSet.items); j++ {
			nextElem := candidateSet.items[j]
			if nextElem.pruneRemoved {
				continue
			}
			// ---------------------------
			if iv.parameters.Alpha*distFn(nextElem.Id) < nextElem.distance {
				candidateSet.items[j].pruneRemoved = true
			}
		}
	}
	// ---------------------------
}
