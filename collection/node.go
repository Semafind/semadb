package collection

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Entry is a node in the graph
// Some of the fields may be empty to optimise performance
type Entry struct {
	Id        string
	Embedding []float32
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

func (c *Collection) getNodeEmbeddingTxn(txn *badger.Txn, nodeId string) ([]float32, error) {
	item, err := txn.Get(nodeEmbedKey(nodeId))
	if err != nil {
		return nil, fmt.Errorf("could not get node embedding: %v", err)
	}
	buffer, err := item.ValueCopy(nil)
	if err != nil {
		return nil, fmt.Errorf("could not copy node embedding: %v", err)
	}
	embedding, err := bytesToFloat32(buffer)
	if err != nil {
		return nil, fmt.Errorf("could not convert embedding bytes to float32: %v", err)
	}
	return embedding, nil
}

func (c *Collection) getNodeEmbedding(nodeId string) ([]float32, error) {
	var embedding []float32
	err := c.db.View(func(txn *badger.Txn) error {
		embed, err := c.getNodeEmbeddingTxn(txn, nodeId)
		if err != nil {
			return fmt.Errorf("could not get node embedding: %v", err)
		}
		embedding = embed
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("node embedding read errored: %v", err)
	}
	return embedding, nil
}

func (c *Collection) getNodeNeighbours(nodeId string) ([]Entry, error) {
	neighbours := make([]Entry, 0, 10)
	// Start a new read only transaction
	err := c.db.View(func(txn *badger.Txn) error {
		// Get the node edges, i.e. the neighbours
		item, err := txn.Get(nodeEdgeKey(nodeId))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return fmt.Errorf("could not get node neighbours: %v", err)
		}
		// Iterate over the neighbours and get their embeddings
		err = item.Value(func(val []byte) error {
			edgeList, err := bytesToEdgeList(val)
			if err != nil {
				return fmt.Errorf("could not convert neighbours bytes to edge list: %v", err)
			}
			for _, neighbourId := range edgeList {
				embedding, err := c.getNodeEmbeddingTxn(txn, neighbourId)
				if err != nil {
					return fmt.Errorf("could not get neighbour embedding: %v", err)
				}
				neighbours = append(neighbours, Entry{Id: neighbourId, Embedding: embedding})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("could not get node neighbours: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("node neighbours read errored: %v", err)
	}
	return neighbours, nil
}

func (c *Collection) setNodeAsEntry(entry Entry, increaseCount uint64) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		// Set the node embedding
		embeddingBytes, err := float32ToBytes(entry.Embedding)
		if err != nil {
			return fmt.Errorf("could not convert embedding to bytes: %v", err)
		}
		err = txn.Set(nodeEmbedKey(entry.Id), embeddingBytes)
		if err != nil {
			return fmt.Errorf("could not set node embedding: %v", err)
		}
		edgeListBytes, err := edgeListToBytes(entry.Edges)
		if err != nil {
			return fmt.Errorf("could not convert edge list to bytes: %v", err)
		}
		err = txn.Set(nodeEdgeKey(entry.Id), edgeListBytes)
		if err != nil {
			return fmt.Errorf("could not set node edges: %v", err)
		}
		if increaseCount != 0 {
			// Update the count
			return c.increaseNodeCount(txn, increaseCount)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("node set as entry errored: %v", err)
	}
	return nil
}

func (c *Collection) setNodeNeighbours(nodeId string, neighbours []string) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		edgeListBytes, err := edgeListToBytes(neighbours)
		if err != nil {
			return fmt.Errorf("could not convert edge list to bytes: %v", err)
		}
		err = txn.Set(nodeEdgeKey(nodeId), edgeListBytes)
		if err != nil {
			return fmt.Errorf("could not set node edges: %v", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("node set neighbours errored: %v", err)
	}
	return nil
}
