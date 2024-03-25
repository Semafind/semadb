package vamana

import (
	"context"
	"fmt"

	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/utils"
)

func (v *IndexVamana) insertWorker(ctx context.Context, pc cache.SharedPointCache, jobQueue <-chan cache.GraphNode) <-chan error {
	return utils.SinkWithContext(ctx, jobQueue, func(point cache.GraphNode) error {
		// TODO: add test case for this
		if point.NodeId == STARTID {
			return fmt.Errorf("cannot insert start node with id: %d", STARTID)
		}
		// Check if the point exists
		_, err := pc.GetPoint(point.NodeId)
		if err == nil {
			return fmt.Errorf("point with node id %d already exists", point.NodeId)
		}
		// Insert the point
		if err := v.insertSinglePoint(pc, point); err != nil {
			return err
		}
		return nil
	})
}

func (v *IndexVamana) insertSinglePoint(pc cache.SharedPointCache, sp cache.GraphNode) error {
	point, err := pc.SetPoint(sp)
	if err != nil {
		return fmt.Errorf("could not set point: %w", err)
	}
	// ---------------------------
	_, visitedSet, err := greedySearch(pc, point.Vector, 1, v.parameters.SearchSize, v.distFn, v.maxNodeId)
	if err != nil {
		return fmt.Errorf("could not greedy search: %w", err)
	}
	// ---------------------------
	// We don't need to lock the point here because it does not yet have inbound
	// edges that other goroutines might use to visit this node.
	robustPrune(point, visitedSet, v.parameters.Alpha, v.parameters.DegreeBound, v.distFn)
	// ---------------------------
	// Add the bi-directional edges, suppose A is being added and has A -> B and
	// A -> C. Then we attempt to add edges from B and C back to A. point.Edges
	// is A -> B and A -> C.
	err = pc.WithPointNeighbours(point, true, func(nnA []*cache.CachePoint) error {
		for _, n := range nnA {
			// So here n = B or C as the example goes
			// While we are adding the bi-directional edges, we need exclusive
			// access to ensure other goroutines don't modify the edges while we
			// are dealing with them. That is what WithPointNeighbours is for.
			err = pc.WithPointNeighbours(n, false, func(nnB []*cache.CachePoint) error {
				if len(nnB)+1 > v.parameters.DegreeBound {
					// We need to prune the neighbour as well to keep the degree bound
					candidateSet := NewDistSet(n.Vector, len(nnB)+1, 0, v.distFn)
					candidateSet.AddPoint(nnB...)
					candidateSet.AddPoint(point) // Here we are asking B or C to add A
					candidateSet.Sort()
					robustPrune(n, candidateSet, v.parameters.Alpha, v.parameters.DegreeBound, v.distFn)
				} else {
					// ---------------------------
					// Add the edge
					n.AddNeighbour(point)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("could not get neighbour point neighbours for bi-directional edges: %w", err)
			}
		}
		return nil
	})
	return nil
}
