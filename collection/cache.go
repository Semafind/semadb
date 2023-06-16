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
	deleted    bool
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

func (ce *CacheEntry) setDeleted() {
	ce.mutex.Lock()
	ce.deleted = true
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
	db        *badger.DB
	mutex     sync.RWMutex
	cache     map[uint64]*CacheEntry
	startNode *CacheEntry
}

func NewNodeCache(db *badger.DB) (*NodeCache, error) {
	// ---------------------------
	nc := &NodeCache{
		db:    db,
		cache: make(map[uint64]*CacheEntry),
	}
	// ---------------------------
	// Read start node from database
	var startId uint64 = 0
	canLoadStartNode := false
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(STARTIDKEY))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return fmt.Errorf("could not get start node id: %v", err)
		}
		return item.Value(func(val []byte) error {
			startId = bytesToUint64(val)
			canLoadStartNode = true
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("could not get start node id: %v", err)
	}
	if canLoadStartNode {
		startNode, err := nc.getNode(startId)
		if err != nil {
			return nil, fmt.Errorf("could not get start node: %v", err)
		}
		nc.startNode = startNode
	}
	// ---------------------------
	return nc, nil
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

func (nc *NodeCache) getOrSetStartNode(entry *Entry) (*CacheEntry, error) {
	nc.mutex.RLock()
	if nc.startNode != nil {
		defer nc.mutex.RUnlock()
		return nc.startNode, nil
	}
	nc.mutex.RUnlock()
	// ---------------------------
	if entry == nil {
		return nil, fmt.Errorf("could not get start node: no start node set and no entry provided")
	}
	// ---------------------------
	newStartNode, err := nc.getNode(entry.Id)
	if err != nil {
		return nil, fmt.Errorf("could not get new start node: %v", err)
	}
	newStartNode.setEmbeddingNoLock(entry.Embedding)
	nc.mutex.Lock()
	nc.startNode = newStartNode
	nc.mutex.Unlock()
	return newStartNode, nil
}

func (nc *NodeCache) flush() error {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	txn := nc.db.NewTransaction(true)
	// ---------------------------
	// Flush dirty entries
	for _, ce := range nc.cache {
		if ce.deleted {
			// Check if the node is the start node
			if nc.startNode != nil && nc.startNode.Id == ce.Id {
				// We need to find a new candidate if any
				neighbours, err := nc.getNodeNeighbours(ce)
				if err != nil {
					return fmt.Errorf("could not get node neighbours: %v", err)
				}
				nc.startNode = nil
				for _, n := range neighbours {
					if !n.deleted {
						nc.startNode = n
						break
					}
				}
			}
			// ---------------------------
			err := txn.Delete(nodeEmbedKey(ce.Id))
			if err == badger.ErrTxnTooBig {
				err = txn.Commit()
				if err != nil {
					return fmt.Errorf("could not commit txn: %v", err)
				}
				txn = nc.db.NewTransaction(true)
				err = txn.Delete(nodeEmbedKey(ce.Id))
			}
			if err != nil {
				return fmt.Errorf("could not delete node embedding: %v", err)
			}
			err = txn.Delete(nodeEdgeKey(ce.Id))
			if err == badger.ErrTxnTooBig {
				err = txn.Commit()
				if err != nil {
					return fmt.Errorf("could not commit txn: %v", err)
				}
				txn = nc.db.NewTransaction(true)
				err = txn.Delete(nodeEdgeKey(ce.Id))
			}
			if err != nil {
				return fmt.Errorf("could not delete node edges: %v", err)
			}
			delete(nc.cache, ce.Id)
			continue
		}
		// ---------------------------
		if ce.dirty {
			embeddingBytes, err := float32ToBytes(ce.Embedding)
			if err != nil {
				return fmt.Errorf("could not convert embedding to bytes: %v", err)
			}
			err = txn.Set(nodeEmbedKey(ce.Id), embeddingBytes)
			if err == badger.ErrTxnTooBig {
				err = txn.Commit()
				if err != nil {
					return fmt.Errorf("could not commit txn: %v", err)
				}
				txn = nc.db.NewTransaction(true)
				err = txn.Set(nodeEmbedKey(ce.Id), embeddingBytes)
			}
			if err != nil {
				return fmt.Errorf("could not set node embedding: %v", err)
			}
			ce.dirty = false
		}
		// ---------------------------
		if ce.edgeDirty {
			edgeListBytes, err := edgeListToBytes(ce.Edges)
			if err != nil {
				return fmt.Errorf("could not convert edge list to bytes: %v", err)
			}
			err = txn.Set(nodeEdgeKey(ce.Id), edgeListBytes)
			if err == badger.ErrTxnTooBig {
				err = txn.Commit()
				if err != nil {
					return fmt.Errorf("could not commit txn: %v", err)
				}
				txn = nc.db.NewTransaction(true)
				err = txn.Set(nodeEdgeKey(ce.Id), edgeListBytes)
			}
			if err != nil {
				return fmt.Errorf("could not set node edge list: %v", err)
			}
			ce.edgeDirty = false
		}
	}
	// ---------------------------
	// Flush start node
	if nc.startNode != nil {
		err := txn.Set([]byte(STARTIDKEY), uint64ToBytes(nc.startNode.Id))
		if err == badger.ErrTxnTooBig {
			err = txn.Commit()
			if err != nil {
				return fmt.Errorf("could not commit txn: %v", err)
			}
			txn = nc.db.NewTransaction(true)
			err = txn.Set([]byte(STARTIDKEY), uint64ToBytes(nc.startNode.Id))
		}
		if err != nil {
			return fmt.Errorf("could not set start node id: %v", err)
		}
	} else {
		err := txn.Delete([]byte(STARTIDKEY))
		if err == badger.ErrTxnTooBig {
			err = txn.Commit()
			if err != nil {
				return fmt.Errorf("could not commit txn: %v", err)
			}
			txn = nc.db.NewTransaction(true)
			err = txn.Delete([]byte(STARTIDKEY))
		}
		if err != nil {
			return fmt.Errorf("could not delete start node id: %v", err)
		}
	}
	// ---------------------------
	// Flush batch
	err := txn.Commit()
	if err != nil {
		return fmt.Errorf("could not commit final txn: %v", err)
	}
	return nil
}
