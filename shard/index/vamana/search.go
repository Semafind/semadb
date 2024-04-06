package vamana

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func (v *IndexVamana) greedySearch(query []float32, k int, searchSize int, filter *roaring64.Bitmap) (DistSet, DistSet, error) {
	// ---------------------------
	distFn := v.vecStore.DistanceFromFloat(query)
	// Initialise distance set
	searchSet := NewDistSet(searchSize, v.maxNodeId.Load(), distFn)
	/* The faster visited set based on bitmaps is only used for the search and we
	 * release it after we're done. This does not affect the items stored in the
	 * search set. For the visitedSet, it actually just an array sorted by
	 * distance and not the actual check whether we have visited them or not
	 * happens inside searchSet, as in we search nodes we haven't yet visited.
	 */
	defer searchSet.Release()
	visitedSet := NewDistSet(searchSize*2, 0, distFn)
	// Check that the search size is greater than k
	if searchSize < k {
		return searchSet, visitedSet, fmt.Errorf("searchSize (%d) must be greater than k (%d)", searchSize, k)
	}
	resultSet := &searchSet
	/* This filtering business is an optimistic one. We perform a regular search
	 * starting from filtered points and only add them to the result set if they
	 * are in the filter. This is based on the navigable property of the graph. A
	 * larger search size would always yield better results but it is important
	 * to remember this is greedy search, it will most likely miss out on certain
	 * points who are in the filter but not near enough to what we are exploring. */
	if filter != nil {
		ds := NewDistSet(k, v.maxNodeId.Load(), distFn)
		resultSet = &ds
		/* Seed the search with the filtered points. We know these points are
		 * valid and in the extreme case where the filter is too selective, i.e.
		 * |filter|<searchSize we just order the filtered points based on their
		 * vector and return. */
		filterK := make([]uint64, 0, searchSize)
		iter := filter.Iterator()
		for i := 0; i < searchSize && iter.HasNext(); i++ {
			filterK = append(filterK, iter.Next())
		}
		filterPoints, err := v.vecStore.GetMany(filterK...)
		if err != nil {
			return searchSet, visitedSet, fmt.Errorf("failed to get filter points: %w", err)
		}
		searchSet.Add(filterPoints...)
		resultSet.AddWithLimit(filterPoints...)
	}
	// ---------------------------
	/* Start the search with the start point neighbours, recall that the start
	 * point is not part of the database but an entry point to the graph.
	 * Upstream search function filters it out but we return it here so the graph
	 * can be constructed correctly. */
	sn, err := v.vecStore.Get(STARTID)
	if err != nil {
		return searchSet, visitedSet, fmt.Errorf("failed to get start point: %w", err)
	}
	searchSet.AddWithLimit(sn)
	// ---------------------------
	/* This loop looks to curate the closest nodes to the query vector along the
	 * way. The loop terminates when we visited all the nodes in our search list. */
	for i := 0; i < min(len(searchSet.items), searchSize); {
		distElem := searchSet.items[i]
		if distElem.visited {
			i++
			continue
		}
		/* We know this is the first and only time we are visiting this node so
		 * we bypass duplicate check and add it straight to the visited set. */
		visitedSet.AddAlreadyUnique(distElem)
		searchSet.items[i].visited = true
		// ---------------------------
		// Get the node and its neighbours
		node, err := v.nodeStore.Get(distElem.Point.Id())
		if err != nil {
			return searchSet, visitedSet, fmt.Errorf("failed to get node for neighbours: %w", err)
		}
		if err := node.LoadNeighbours(v.vecStore); err != nil {
			return searchSet, visitedSet, fmt.Errorf("failed to load node neighbours: %w", err)
		}
		/* We have to lock the point here because while we are calculating the
		 * distance of its neighbours (edges in the graph) we can't have another
		 * goroutine changing them. The case we aren't covering is after we have
		 * calculated, they may change so the search we are doing is not
		 * deterministic. With approximate search this is not a major problem. */
		node.edgesMu.RLock()
		searchSet.AddWithLimit(node.neighbours...)
		node.edgesMu.RUnlock()
		// ---------------------------
		if filter != nil && filter.Contains(node.Id) {
			resultSet.AddWithLimit(distElem.Point)
		}
		// ---------------------------
		i = 0
	}
	// ---------------------------
	visitedSet.Sort()
	return *resultSet, visitedSet, nil
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
		if closestElem.pruneRemoved || closestElem.Point.Id() == node.Id {
			continue
		}
		edgeCount := node.AddNeighbour(closestElem.Point)
		if edgeCount >= iv.parameters.DegreeBound {
			break
		}
		// ---------------------------
		// Prune optimistically
		distFn := iv.vecStore.DistanceFromPoint(closestElem.Point)
		for j := i + 1; j < len(candidateSet.items); j++ {
			nextElem := candidateSet.items[j]
			if nextElem.pruneRemoved {
				continue
			}
			// ---------------------------
			if iv.parameters.Alpha*distFn(nextElem.Point) < nextElem.Distance {
				candidateSet.items[j].pruneRemoved = true
			}
		}
	}
	// ---------------------------
}
