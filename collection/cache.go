package collection

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type CacheEntry struct {
	Entry
	dirty      bool
	edgeDirty  bool
	mutex      sync.RWMutex
	neighbours []*CacheEntry
}

func (ce *CacheEntry) setEmbeddingNoLock(embedding []float32) {
	ce.Embedding = embedding
	ce.dirty = true
}

func (ce *CacheEntry) setNeighbours(neighbours []*CacheEntry) {
	ce.mutex.Lock()
	ce.neighbours = neighbours
	ce.edgeDirty = true
	edgeList := make([]uint64, len(neighbours))
	for i, n := range neighbours {
		edgeList[i] = n.Id
	}
	ce.Edges = edgeList
	ce.mutex.Unlock()
}

func (ce *CacheEntry) appendNeighbour(neighbour *CacheEntry) {
	ce.mutex.Lock()
	ce.neighbours = append(ce.neighbours, neighbour)
	ce.edgeDirty = true
	ce.Edges = append(ce.Edges, neighbour.Id)
	ce.mutex.Unlock()
}

// func (ce *CacheEntry) setEdgesNoLock(edges []string) {
// 	ce.Edges = edges
// 	ce.edgeDirty = true
// }

type NodeCache struct {
	db       *badger.DB
	mutex    sync.RWMutex
	cache    map[uint64]*CacheEntry
	addCount int
}

func NewNodeCache(db *badger.DB) *NodeCache {
	return &NodeCache{
		db:    db,
		cache: make(map[uint64]*CacheEntry),
	}
}

func (nc *NodeCache) getNode(nodeId uint64) (*CacheEntry, error) {
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
	edges, err := nc.getNodeEdges(nodeId)
	if err != nil {
		return nil, err
	}
	if embedding == nil {
		nc.addCount++
	}
	newEntry := &CacheEntry{Entry: Entry{Id: nodeId, Embedding: embedding, Edges: edges}}
	nc.cache[nodeId] = newEntry
	return newEntry, nil
}

func (nc *NodeCache) getNodeNeighbours(ce *CacheEntry) ([]*CacheEntry, error) {
	ce.mutex.RLock()
	neighbours := ce.neighbours
	if neighbours != nil {
		ce.mutex.RUnlock()
		return neighbours, nil
	}
	ce.mutex.RUnlock()
	ce.mutex.Lock()
	defer ce.mutex.Unlock()
	edgeList := ce.Edges
	// Fetch the neighbouring entries
	neighbours = make([]*CacheEntry, len(edgeList))
	for i, nId := range edgeList {
		neighbour, err := nc.getNode(nId)
		if err != nil {
			return nil, fmt.Errorf("could not get node neighbour (%v): %v", nId, err)
		}
		neighbours[i] = neighbour
	}
	ce.neighbours = neighbours
	return neighbours, nil
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
	batchSize := 256
	batch := make([]*badger.Entry, 0, batchSize+16) // +16 to avoid reallocation
	// ---------------------------
	// Flush dirty entries
	for _, ce := range nc.cache {
		if ce.dirty {
			embeddingBytes, err := float32ToBytes(ce.Embedding)
			if err != nil {
				return fmt.Errorf("could not convert embedding to bytes: %v", err)
			}
			batch = append(batch, &badger.Entry{
				Key:   nodeEmbedKey(ce.Id),
				Value: embeddingBytes,
			})
			ce.dirty = false
		}
		if ce.edgeDirty {
			edgeListBytes, err := edgeListToBytes(ce.Edges)
			if err != nil {
				return fmt.Errorf("could not convert edge list to bytes: %v", err)
			}
			batch = append(batch, &badger.Entry{
				Key:   nodeEdgeKey(ce.Id),
				Value: edgeListBytes,
			})
			ce.edgeDirty = false
		}
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
	return nil
}
