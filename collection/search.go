package collection

import (
	"fmt"

	"gonum.org/v1/gonum/blas/blas32"
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
	return 1 - blas32.Dot(blas32.Vector{N: len(x), Inc: 1, Data: x}, blas32.Vector{N: len(y), Inc: 1, Data: y})
}

// func angularDist(x, y []float32) float32 {
// 	cosineSim := blas32.Dot(blas32.Vector{N: len(x), Inc: 1, Data: x}, blas32.Vector{N: len(y), Inc: 1, Data: y})
// 	angularDist := math.Acos(float64(cosineSim)) / math.Pi
// 	return float32(angularDist)
// }

func (c *Collection) dist(x, y []float32) float32 {
	if c.Config.DistMetric == "angular" {
		return cosineDist(x, y)
	}
	return eucDist(x, y)
}

func (c *Collection) greedySearch(startNode *CacheEntry, query []float32, k int, searchSize int, nodeCache *NodeCache) (*DistSet, *DistSet, error) {
	// ---------------------------
	// Check that the search size is greater than k
	if searchSize < k {
		return nil, nil, fmt.Errorf("searchSize (%d) must be greater than k (%d)", searchSize, k)
	}
	// ---------------------------
	// Initialise distance set
	searchSet := NewDistSet(query, searchSize*2, c.dist)
	visitedSet := NewDistSet(query, searchSize*2, c.dist)
	// ---------------------------
	// Get the start node
	searchSet.AddEntry(startNode)
	// ---------------------------
	/* This loop looks to curate the closest nodes to the query vector along the
	 * way. It is usually implemented with two sets, we try to merged them into
	 * one array with set semantics. The loop terminates when we visited all the
	 * nodes in our search list. */
	for i := 0; i < searchSet.Len(); {
		node := searchSet.items[i]
		if visitedSet.Contains(node.id) {
			i++
			continue
		}
		visitedSet.Add(node)
		neighbours, err := nodeCache.getNodeNeighbours(node.cacheEntry)
		if err != nil {
			return nil, nil, fmt.Errorf("could not get node (%v) neighbours: %v", node.id, err)
		}
		searchSet.AddEntry(neighbours...)
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

func (c *Collection) robustPrune(node Entry, candidateSet *DistSet, alpha float32, degreeBound int) ([]*CacheEntry, error) {
	// ---------------------------
	// Get the node neighbours
	// nodeNeighbours, err := nodeCache.getNodeNeighbours(node.Id)
	// if err != nil {
	// 	return nil, nil, fmt.Errorf("could not get node (%v) neighbours for pruning: %v", node.Id, err)
	// }
	// ---------------------------
	// Merge neighbours into candidate set
	// candidateSet.AddEntry(nodeNeighbours...)
	// candidateSet.Sort()
	candidateSet.Remove(node.Id) // Exclude the node itself
	// ---------------------------
	// We will overwrite existing neighbours
	newNeighours := make([]*CacheEntry, 0, degreeBound)
	// ---------------------------
	for candidateSet.Len() > 0 {
		// ---------------------------
		// Get the closest node
		closestElem := candidateSet.Pop()
		newNeighours = append(newNeighours, closestElem.cacheEntry)
		if len(newNeighours) >= degreeBound {
			break
		}
		// ---------------------------
		// Prune optimisitically
		for _, cand := range candidateSet.items {
			// We currently do this check because remove doesn't handle re-ordering for performance purposes
			if !candidateSet.Contains(cand.id) {
				continue
			}
			// ---------------------------
			if alpha*c.dist(closestElem.embedding, cand.embedding) <= cand.distance {
				candidateSet.Remove(cand.id)
			}
		}
	}
	// ---------------------------
	return newNeighours, nil
}
