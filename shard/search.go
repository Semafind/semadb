package shard

import (
	"fmt"
)

func eucDist(x, y []float32) float32 {
	var sum float32
	for i := range x {
		diff := x[i] - y[i]
		sum += diff * diff
	}
	return sum
}

func cosineDist(x, y []float32) float32 {
	var sum float32
	for i := range x {
		sum += x[i] * y[i]
	}
	return 1 - sum
}

func (s *Shard) dist(x, y []float32) float32 {
	if s.collection.DistMetric == "cosine" {
		return cosineDist(x, y)
	}
	return eucDist(x, y)
}

func (s *Shard) greedySearch(pc *PointCache, startPoint *CachePoint, query []float32, k int, searchSize int) (DistSet, DistSet, error) {
	// ---------------------------
	// Initialise distance set
	searchSet := NewDistSet(query, searchSize*2, s.dist)
	visitedSet := NewDistSet(query, searchSize*2, s.dist)
	// Check that the search size is greater than k
	if searchSize < k {
		return searchSet, visitedSet, fmt.Errorf("searchSize (%d) must be greater than k (%d)", searchSize, k)
	}
	// ---------------------------
	// Start the search with the start point
	searchSet.AddPoint(startPoint)
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
	point.Edges = point.Edges[:0] // Reset edges
	// ---------------------------
	for candidateSet.Len() > 0 {
		// ---------------------------
		// Get the closest point
		closestElem := candidateSet.Pop()
		point.Edges = append(point.Edges, closestElem.point.Id)
		if len(point.Edges) >= degreeBound {
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
			if alpha*s.dist(closestElem.point.Vector, cand.point.Vector) < cand.distance {
				candidateSet.Remove(cand.point.Id)
			}
		}
	}
	point.isEdgeDirty = true
	// ---------------------------
}
