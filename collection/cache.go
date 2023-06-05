package collection

import (
	"fmt"
	"sync"
)

type NodeCache struct {
	mutex     sync.RWMutex
	cache     map[string]*Entry
	dirty     map[string]bool
	edgeDirty map[string]bool
	addCount  int
}

func NewNodeCache() *NodeCache {
	return &NodeCache{
		cache:     make(map[string]*Entry),
		dirty:     make(map[string]bool),
		edgeDirty: make(map[string]bool),
	}
}

func (nc *NodeCache) getNode(nodeId string, c *Collection) (*Entry, error) {
	// Optimistic check
	nc.mutex.RLock()
	entry, ok := nc.cache[nodeId]
	nc.mutex.RUnlock()
	if ok {
		return entry, nil
	}
	// Fetch from database
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	entry, ok = nc.cache[nodeId] // Second check in case another goroutine fetched it
	if ok {
		return entry, nil
	}
	embedding, err := c.getNodeEmbedding(nodeId)
	if err != nil {
		return nil, err
	}
	newEntry := &Entry{Id: nodeId, Embedding: embedding}
	nc.cache[nodeId] = newEntry
	return newEntry, nil
}

func (nc *NodeCache) getNodeNeighbours(nodeId string, c *Collection) ([]*Entry, error) {
	// Fetch start node
	entry, err := nc.getNode(nodeId, c)
	if err != nil {
		return nil, fmt.Errorf("could not get node for neighbours: %v", err)
	}
	// Check for edges
	if entry.Edges == nil {
		nc.mutex.Lock()
		// Secondary check
		if entry.Edges == nil {
			edges, err := c.getNodeEdges(nodeId)
			if err != nil {
				nc.mutex.Unlock()
				return nil, fmt.Errorf("could not get node edges for neighbours: %v", err)
			}
			entry.Edges = edges
		}
		nc.mutex.Unlock()
	}
	// Fetch the neighbouring entries
	neighbours := make([]*Entry, len(entry.Edges))
	for i, nId := range entry.Edges {
		neighbour, err := nc.getNode(nId, c)
		if err != nil {
			return nil, fmt.Errorf("could not get node neighbour (%v): %v", nId, err)
		}
		neighbours[i] = neighbour
	}
	return neighbours, nil
}

func (nc *NodeCache) setNode(entry *Entry) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	nc.cache[entry.Id] = entry
	nc.dirty[entry.Id] = true
	nc.addCount++
}

func (nc *NodeCache) setNodeEdges(nodeId string, edges []string) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	nc.cache[nodeId].Edges = edges
	if !nc.dirty[nodeId] {
		nc.edgeDirty[nodeId] = true
	}
}
