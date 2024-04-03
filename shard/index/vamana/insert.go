package vamana

import (
	"context"
	"fmt"

	"github.com/semafind/semadb/utils"
)

func (v *IndexVamana) insertWorker(ctx context.Context, jobQueue <-chan IndexVectorChange) <-chan error {
	return utils.SinkWithContext(ctx, jobQueue, func(change IndexVectorChange) error {
		return v.insertSinglePoint(change)
	})
}

func (v *IndexVamana) insertSinglePoint(change IndexVectorChange) error {
	vecA, err := v.vecStore.Set(change.Id, change.Vector)
	if err != nil {
		return fmt.Errorf("could not set point: %w", err)
	}
	// ---------------------------
	_, visitedSet, err := v.greedySearch(change.Vector, 1, v.parameters.SearchSize)
	if err != nil {
		return fmt.Errorf("could not greedy search: %w", err)
	}
	// ---------------------------
	// We don't need to lock the point here because it does not yet have inbound
	// edges that other goroutines might use to visit this node.
	nodeA := &graphNode{Id: change.Id}
	v.robustPrune(nodeA, visitedSet)
	if err := v.nodeStore.Put(change.Id, nodeA); err != nil {
		return fmt.Errorf("could not set node: %w", err)
	}
	// ---------------------------
	// Add the bi-directional edges, suppose A is being added and has A -> B and
	// A -> C. Then we attempt to add edges from B and C back to A.
	nodeA.edgesMu.RLock()
	defer nodeA.edgesMu.RUnlock()
	for _, nB := range nodeA.neighbours {
		// So here n = B or C as the example goes
		nodeBs, err := v.nodeStore.Get(nB.Id())
		if err != nil {
			return fmt.Errorf("could not get neighbour point: %w", err)
		}
		nodeB := nodeBs[0]
		// ---------------------------
		// While we are adding the bi-directional edges, we need exclusive
		// access to ensure other goroutines don't modify the edges while we
		// are dealing with them. That is what the locks are for.
		nodeB.edgesMu.Lock()
		if len(nodeB.edges)+1 > v.parameters.DegreeBound {
			// We need to prune the neighbour as well to keep the degree bound
			distFn := v.vecStore.DistanceFromPoint(nB)
			candidateSet := NewDistSet(len(nodeB.edges)+1, 0, distFn)
			if err := nodeB.LoadNeighbours(v.vecStore); err != nil {
				nodeB.edgesMu.Unlock()
				return fmt.Errorf("could not load nodeB neighbours adding bi-directional edges: %w", err)
			}
			candidateSet.Add(nodeB.neighbours...)
			candidateSet.Add(vecA) // Here we are asking B or C to add A
			candidateSet.Sort()
			v.robustPrune(nodeB, candidateSet)
		} else {
			// ---------------------------
			// Add the edge
			nodeB.AddNeighbour(vecA)
		}
		nodeB.edgesMu.Unlock()
	}
	return nil
}
