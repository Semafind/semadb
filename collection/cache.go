package collection

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type NodeCache struct {
	db        *badger.DB
	mutex     sync.RWMutex
	cache     map[string]*Entry
	dirty     map[string]bool
	edgeDirty map[string]bool
	addCount  int
}

func NewNodeCache(db *badger.DB) *NodeCache {
	return &NodeCache{
		db:        db,
		cache:     make(map[string]*Entry),
		dirty:     make(map[string]bool),
		edgeDirty: make(map[string]bool),
	}
}

func (nc *NodeCache) getNode(nodeId string) (*Entry, error) {
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
	embedding, err := nc.getNodeEmbedding(nodeId)
	if err != nil {
		return nil, err
	}
	newEntry := &Entry{Id: nodeId, Embedding: embedding}
	nc.cache[nodeId] = newEntry
	return newEntry, nil
}

func (nc *NodeCache) getNodeNeighbours(nodeId string) ([]*Entry, error) {
	// Fetch start node
	entry, err := nc.getNode(nodeId)
	if err != nil {
		return nil, fmt.Errorf("could not get node for neighbours: %v", err)
	}
	var edgeList []string
	// Check for edges
	nc.mutex.RLock()
	if entry.Edges == nil {
		nc.mutex.RUnlock()
		nc.mutex.Lock()
		// Secondary check
		if entry.Edges == nil {
			edges, err := nc.getNodeEdges(nodeId)
			if err != nil {
				nc.mutex.Unlock()
				return nil, fmt.Errorf("could not get node edges for neighbours: %v", err)
			}
			entry.Edges = edges
			edgeList = edges
		} else {
			edgeList = entry.Edges
		}
		nc.mutex.Unlock()
	} else {
		edgeList = entry.Edges
		nc.mutex.RUnlock()
	}
	// Fetch the neighbouring entries
	neighbours := make([]*Entry, len(edgeList))
	for i, nId := range edgeList {
		neighbour, err := nc.getNode(nId)
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

func (nc *NodeCache) flushBadgerEntries(batch []*badger.Entry) error {
	err := nc.db.Update(func(txn *badger.Txn) error {
		for _, entry := range batch {
			err := txn.SetEntry(entry)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not update database for flush: %v", err)
	}
	return nil
}

func (nc *NodeCache) flush() error {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	if nc.addCount == 0 {
		return nil
	}
	batchSize := 256
	batch := make([]*badger.Entry, 0, batchSize+16) // +16 to avoid reallocation
	// ---------------------------
	// Flush dirty entries
	for nodeId := range nc.dirty {
		entry, ok := nc.cache[nodeId]
		if !ok {
			return fmt.Errorf("could not find dirty node (%v) in cache", nodeId)
		}
		embeddingBytes, err := float32ToBytes(entry.Embedding)
		if err != nil {
			return fmt.Errorf("could not convert embedding to bytes: %v", err)
		}
		batch = append(batch, &badger.Entry{
			Key:   nodeEmbedKey(nodeId),
			Value: embeddingBytes,
		})
		edgeListBytes, err := edgeListToBytes(entry.Edges)
		if err != nil {
			return fmt.Errorf("could not convert edge list to bytes: %v", err)
		}
		batch = append(batch, &badger.Entry{
			Key:   nodeEdgeKey(nodeId),
			Value: edgeListBytes,
		})
		delete(nc.edgeDirty, nodeId)
		delete(nc.dirty, nodeId)
		if len(batch) >= batchSize {
			err := nc.flushBadgerEntries(batch)
			if err != nil {
				return fmt.Errorf("could not update database for flush: %v", err)
			}
			batch = batch[:0]
		}
	}
	// ---------------------------
	// Flush dirty edges
	for nodeId := range nc.edgeDirty {
		entry, ok := nc.cache[nodeId]
		if !ok {
			return fmt.Errorf("could not find dirty edge node (%v) in cache", nodeId)
		}
		edgeListBytes, err := edgeListToBytes(entry.Edges)
		if err != nil {
			return fmt.Errorf("could not convert edge list to bytes: %v", err)
		}
		batch = append(batch, &badger.Entry{
			Key:   nodeEdgeKey(nodeId),
			Value: edgeListBytes,
		})
		delete(nc.edgeDirty, nodeId)
		if len(batch) >= batchSize {
			err := nc.flushBadgerEntries(batch)
			if err != nil {
				return fmt.Errorf("could not update database for flush: %v", err)
			}
			batch = batch[:0]
		}
	}
	// ---------------------------
	// Flush batch
	if len(batch) > 0 {
		err := nc.flushBadgerEntries(batch)
		if err != nil {
			return fmt.Errorf("could not update database for flush: %v", err)
		}
	}
	nc.addCount = 0
	if len(nc.dirty) > 0 || len(nc.edgeDirty) > 0 {
		return fmt.Errorf("flush did not clear all dirty entries")
	}
	return nil
}
