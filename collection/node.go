package collection

import (
	"context"
	"fmt"
	"net/rpc"

	"github.com/dgraph-io/badger/v4"
	_ "github.com/go-sql-driver/mysql"
	"github.com/semafind/semadb/scribble/remote"
)

// Entry is a node in the graph
// Some of the fields may be empty to optimise performance
type Entry struct {
	Id        uint64    `json:"id" binding:"required"`
	Embedding []float32 `json:"embedding" binding:"required"`
	Edges     []uint64
}

func nodeEmbedKey(nodeId uint64) []byte {
	nodeKey := uint64ToBytes(nodeId)
	finalKey := append([]byte("N_"), nodeKey...)
	return append(finalKey, []byte(EMBEDSUFFIX)...)
}

func nodeEdgeKey(nodeId uint64) []byte {
	nodeKey := uint64ToBytes(nodeId)
	finalKey := append([]byte("N_"), nodeKey...)
	return append(finalKey, []byte(EDGESUFFIX)...)
}

func (nc *NodeCache) getNodeFromSQL(nodeId uint64) (*Entry, error) {
	var node Entry
	var embedding []byte
	var edges []byte
	nc.sqlDB.QueryRow("SELECT embedding, edges FROM nodes WHERE id = ?", nodeId).Scan(&embedding, &edges)
	node.Id = nodeId
	node.Embedding, _ = bytesToFloat32(embedding)
	node.Edges, _ = bytesToEdgeList(edges)
	return &node, nil
}

var rcpClient *rpc.Client

func init() {
	// var err error
	// rcpClient, err = rpc.DialHTTP("tcp", "localhost:9090")
	// if err != nil {
	// 	panic(err)
	// }
}

func (nc *NodeCache) getNodeFromRPC(nodeId uint64) (*Entry, error) {
	nodeRequest := remote.NodeRequest{Id: nodeId}
	var resp remote.NodeResponse
	err := rcpClient.Call("NodeRepo.GetNode", nodeRequest, &resp)
	if err != nil {
		return nil, fmt.Errorf("could not get node from remote: %v", err)
	}
	return &Entry{
		Id:        resp.Id,
		Embedding: resp.Embedding,
		Edges:     resp.Edges,
	}, nil
}

func (nc *NodeCache) getNodeFromEmpty(nodeId uint64) (*Entry, error) {
	return &Entry{
		Id: nodeId,
	}, nil
}

func (nc *NodeCache) getNodeFromTiKV(nodeId uint64) (*Entry, error) {
	entry := new(Entry)
	entry.Id = nodeId
	ctx := context.Background()
	// Get embedding
	v, err := nc.tikvDB.Get(ctx, nodeEmbedKey(nodeId))
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, fmt.Errorf("node embedding not found")
	} else {
		embed, err := bytesToFloat32(v)
		if err != nil {
			return nil, err
		}
		entry.Embedding = embed
	}
	// Get edges
	v, err = nc.tikvDB.Get(ctx, nodeEdgeKey(nodeId))
	if err != nil {
		return nil, err
	}
	if v == nil {
		return entry, nil
	} else {
		edges, err := bytesToEdgeList(v)
		if err != nil {
			return nil, err
		}
		entry.Edges = edges
	}
	return entry, nil
}

func (nc *NodeCache) getNodeFromCassandra(nodeId uint64) (*Entry, error) {
	entry := new(Entry)
	entry.Id = nodeId
	// Get embedding
	err := nc.cassandraSession.Query("SELECT embedding, edges FROM nodes WHERE id = ?", nodeId).Scan(&entry.Embedding, &entry.Edges)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (nc *NodeCache) getNodeFromDB(nodeId uint64) (*Entry, error) {
	return nc.getNodeFromSQL(nodeId)
}

func (nc *NodeCache) getNodeEmbedding(nodeId uint64) ([]float32, error) {
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

func (nc *NodeCache) getNodeEdges(nodeId uint64) ([]uint64, error) {
	var edges []uint64
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
