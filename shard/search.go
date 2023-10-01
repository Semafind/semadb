package shard

import (
	"fmt"

	"github.com/google/uuid"
)

func (s *Shard) greedySearch(pc *PointCache, startPointId uuid.UUID, query []float32, k int, searchSize int) (DistSet, DistSet, error) {
	// ---------------------------
	// Initialise distance set
	searchSet := NewDistSet(query, searchSize*2, s.distFn)
	visitedSet := NewDistSet(query, searchSize*2, s.distFn)
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
	searchSet.AddPoint(sp)
	// ---------------------------
	/* This loop looks to curate the closest nodes to the query vector along the
	 * way. The loop terminates when we visited all the nodes in our search list. */
	for i := 0; i < searchSet.Len(); {
		distElem := searchSet.items[i]
		if visitedSet.Contains(distElem.point.Id) {
			i++
			continue
		}
		visitedSet.Add(distElem)
		neighbours, err := pc.GetPointNeighbours(distElem.point)
		if err != nil {
			return searchSet, visitedSet, fmt.Errorf("failed to get neighbours: %w", err)
		}
		searchSet.AddPoint(neighbours...)
		searchSet.Sort()
		if searchSet.Len() > searchSize {
			searchSet.KeepFirstK(searchSize)
		}
		i = 0
	}
	// ---------------------------
	visitedSet.Sort()
	return searchSet, visitedSet, nil
}

func (s *Shard) robustPrune(point *CachePoint, candidateSet DistSet, alpha float32, degreeBound int) {
	// ---------------------------
	// Exclude the point itself
	candidateSet.Remove(point.Id)
	// ---------------------------
	point.ClearNeighbours() // Reset edges / neighbours
	// ---------------------------
	for candidateSet.Len() > 0 {
		// ---------------------------
		// Get the closest point
		closestElem := candidateSet.Pop()
		edgeCount := point.AddNeighbour(closestElem.point)
		if edgeCount >= degreeBound {
			break
		}
		// ---------------------------
		// Prune optimistically
		for _, cand := range candidateSet.items {
			// We currently do this check because remove doesn't handle
			// re-ordering for performance purposes
			if !candidateSet.Contains(cand.point.Id) {
				continue
			}
			// ---------------------------
			if alpha*s.distFn(closestElem.point.Vector, cand.point.Vector) < cand.distance {
				candidateSet.Remove(cand.point.Id)
			}
		}
	}
	// ---------------------------
}
