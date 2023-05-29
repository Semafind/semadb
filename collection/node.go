package collection

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

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
