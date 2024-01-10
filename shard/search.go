package shard

import (
	"fmt"

	"github.com/semafind/semadb/shard/cache"
)

func (s *Shard) greedySearch(pc *cache.PointCache, startPointId uint64, query []float32, k int, searchSize int) (DistSet, DistSet, error) {
	// ---------------------------
	// Initialise distance set
	searchSet := NewDistSet(query, searchSize, uint(s.maxNodeId.Load()), s.distFn)
	// The faster visited set is only used for the search and we release it
	// after we're done. This does not affect the items stored in the search
	// set.
	defer searchSet.Release()
	visitedSet := NewDistSet(query, searchSize*2, 0, s.distFn)
	// Check that the search size is greater than k
	if searchSize < k {
		return searchSet, visitedSet, fmt.Errorf("searchSize (%d) must be greater than k (%d)", searchSize, k)
	}
	// ---------------------------
	// Start the search with the start point neighbours, recall that the start
	// point is not part of the database but an entry point to the graph.
	// Upstream search function filters it out but we return it here so the
	// graph can be constructed correctly.
	sp, err := pc.GetPoint(startPointId)
	if err != nil {
		return searchSet, visitedSet, fmt.Errorf("failed to get start point: %w", err)
	}
	searchSet.AddPointWithLimit(sp)
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
		err := pc.WithPointNeighbours(distElem.point, true, func(neighbours []*cache.CachePoint) error {
			searchSet.AddPointWithLimit(neighbours...)
			return nil
		})
		if err != nil {
			return searchSet, visitedSet, fmt.Errorf("failed to get neighbours during search: %w", err)
		}
		// ---------------------------
		i = 0
	}
	// ---------------------------
	visitedSet.Sort()
	return searchSet, visitedSet, nil
}

func (s *Shard) robustPrune(point *cache.CachePoint, candidateSet DistSet, alpha float32, degreeBound int) {
	// ---------------------------
	point.ClearNeighbours() // Reset edges / neighbours
	// ---------------------------
	for i := 0; i < len(candidateSet.items); i++ {
		// ---------------------------
		// Get the closest point
		closestElem := candidateSet.items[i]
		// Exclude the point itself, this might happen in case we are updating.
		// It is worth checking if this is the case.
		if closestElem.pruneRemoved || closestElem.point.Id == point.Id {
			continue
		}
		edgeCount := point.AddNeighbour(closestElem.point)
		if edgeCount >= degreeBound {
			break
		}
		// ---------------------------
		// Prune optimistically
		for j := i + 1; j < len(candidateSet.items); j++ {
			nextElem := candidateSet.items[j]
			if nextElem.pruneRemoved {
				continue
			}
			// ---------------------------
			if alpha*s.distFn(closestElem.point.Vector, nextElem.point.Vector) < nextElem.distance {
				candidateSet.items[j].pruneRemoved = true
			}
		}
	}
	// ---------------------------
}
