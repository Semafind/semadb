package collection

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Entry is a node in the graph
// Some of the fields may be empty to optimise performance
type Entry struct {
	Id        string    `json:"id" binding:"required"`
	Embedding []float32 `json:"embedding" binding:"required"`
	Edges     []string
}

func nodeEmbedKey(nodeId string) []byte {
	nodeKey := []byte(nodeId)
	return append(nodeKey, []byte(EMBEDSUFFIX)...)
}

func nodeEdgeKey(nodeId string) []byte {
	nodeKey := []byte(nodeId)
	return append(nodeKey, []byte(EDGESUFFIX)...)
}

func (nc *NodeCache) getNodeEmbedding(nodeId string) ([]float32, error) {
	var embedding []float32
	err := nc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeEmbedKey(nodeId))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return fmt.Errorf("could not get node embedding: %v", err)
		}
		buffer, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("could not copy node embedding: %v", err)
		}
		embed, err := bytesToFloat32(buffer)
		if err != nil {
			return fmt.Errorf("could not convert embedding bytes to float32: %v", err)
		}
		embedding = embed
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("node embedding read errored: %v", err)
	}
	return embedding, nil
}

func (nc *NodeCache) getNodeEdges(nodeId string) ([]string, error) {
	var edges []string
	err := nc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeEdgeKey(nodeId))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return fmt.Errorf("could not get node edges: %v", err)
		}
		edgeList, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("could not copy node edges: %v", err)
		}
		edges, err = bytesToEdgeList(edgeList)
		if err != nil {
			return fmt.Errorf("could not convert edges bytes to edge list: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("node edges read errored: %v", err)
	}
	return edges, nil
}

func (c *Collection) getOrSetStartId(entry *Entry, override bool) (string, error) {
	// ---------------------------
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// ---------------------------
	// Read-only case
	if entry == nil && !override {
		if c.startNodeId != "" {
			return c.startNodeId, nil
		}
		err := c.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(STARTIDKEY))
			if err != nil {
				return fmt.Errorf("could not get start id: %v", err)
			}
			return item.Value(func(val []byte) error {
				c.startNodeId = string(val)
				return nil
			})
		})
		return c.startNodeId, err
	}
	// ---------------------------
	// Write case
	if entry != nil {
		if !override && c.startNodeId != "" {
			return c.startNodeId, nil
		}
		err := c.db.Update(func(txn *badger.Txn) error {
			txn.Set([]byte(STARTIDKEY), []byte(entry.Id))
			embedding, err := float32ToBytes(entry.Embedding)
			if err != nil {
				return fmt.Errorf("could not convert embedding to bytes: %v", err)
			}
			txn.Set(nodeEmbedKey(entry.Id), embedding)
			return c.increaseNodeCount(txn, 1)
		})
		if err != nil {
			return "", fmt.Errorf("could not set start id: %v", err)
		}
		c.startNodeId = entry.Id
		return c.startNodeId, nil
	}
	// ---------------------------
	return "", fmt.Errorf("could not set start id: invalid arguments (entry: %v, override: %v)", entry, override)
}
