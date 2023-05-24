package collection

import (
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v4"
	"github.com/semafind/semadb/numerical"
	"github.com/vmihailenco/msgpack/v5"
)

type Entry struct {
	Id        string
	Embedding []float32
}

// ---------------------------

const (
	STARTIDKEY  = "_STARTID"
	EMBEDSUFFIX = "_EMBED"
	EDGESUFFIX  = "_EDGE"
)

// ---------------------------
/* This mess of converting things into bytes happens because the key value store
 * only understands bytes. */

func nodeEmbedKey(nodeId string) []byte {
	nodeKey := []byte(nodeId)
	return append(nodeKey, []byte(EMBEDSUFFIX)...)
}

func nodeEdgeKey(nodeId string) []byte {
	nodeKey := []byte(nodeId)
	return append(nodeKey, []byte(EDGESUFFIX)...)
}

func float32ToBytes(f []float32) ([]byte, error) {
	return msgpack.Marshal(f)
}

func bytesToFloat32(b []byte) ([]float32, error) {
	var f []float32
	err := msgpack.Unmarshal(b, &f)
	return f, err
}

func edgeListToBytes(edges []string) ([]byte, error) {
	return msgpack.Marshal(edges)
}

func bytesToEdgeList(b []byte) ([]string, error) {
	var edges []string
	err := msgpack.Unmarshal(b, &edges)
	return edges, err
}

// ---------------------------

type Collection struct {
	Id string
	db *badger.DB
}

func NewCollection(id string, db *badger.DB) *Collection {
	return &Collection{Id: id, db: db}
}

func putEntry(entry Entry, txn *badger.Txn) error {
	return nil
}

func (c *Collection) getOrSetStartId(entry *Entry) (string, error) {
	startId := ""
	err := c.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(STARTIDKEY))
		if err == badger.ErrKeyNotFound {
			// Initialise the database with the first node
			txn.Set([]byte(STARTIDKEY), []byte(entry.Id))
			embedding, err := float32ToBytes(entry.Embedding)
			if err != nil {
				return fmt.Errorf("could not convert embedding to bytes: %v", err)
			}
			txn.Set(nodeEmbedKey(entry.Id), embedding)
			txn.Set(nodeEdgeKey(entry.Id), []byte{})
			startId = entry.Id
			return c.increaseNodeCount(txn, 1)
		} else if err != nil {
			return fmt.Errorf("could not get start id: %v", err)
		}
		return item.Value(func(val []byte) error {
			startId = string(val)
			return nil
		})
	})
	return startId, err
}

func (c *Collection) Put(entries []Entry) error {
	// Sanity checks
	if len(entries) == 0 {
		return nil
	}
	// Check if the database has been initialised with at least one node
	startId, err := c.getOrSetStartId(&entries[0])
	if err != nil {
		return fmt.Errorf("could not get start id: %v", err)
	}
	fmt.Println("HERE--")
	fmt.Println("startId:", startId)
	err = c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(NODECOUNTKEY))
		if err != nil {
			return fmt.Errorf("HERE--: %v", err)
		}
		return item.Value(func(val []byte) error {
			fmt.Println("nodeCount:", val)
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("could not get node count: %v", err)
	}
	fmt.Println("DONE--")
	return nil
}

func (c *Collection) Search(values numerical.Matrix, k int) ([]string, error) {
	log.Fatal("not implemented")
	return nil, nil
}
