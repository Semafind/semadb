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

func (nc *NodeCache) getNodeEmbedding(nodeId string) ([]float32, error) {
	var embedding []float32
	err := nc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeEmbedKey(nodeId))
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
