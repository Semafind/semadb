package collection

import (
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v4"
	"github.com/semafind/semadb/numerical"
)

type Entry struct {
	Id        string
	Embedding []float32
}

// ---------------------------

type Collection struct {
	Id string
	db *badger.DB
}

func NewCollection(id string, db *badger.DB) *Collection {
	return &Collection{Id: id, db: db}
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
			// Empty edge list
			// txn.Set(nodeEdgeKey(entry.Id), []byte{})
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

func (c *Collection) putEntry(startNodeId string, entry Entry) error {
	searchSet, visitedSet, err := c.greedySearch(startNodeId, entry.Embedding, 1, 128)
	if err != nil {
		return fmt.Errorf("could not perform greedy search: %v", err)
	}
	fmt.Println("searchSet:", searchSet)
	fmt.Println("visitedSet:", visitedSet)
	prunedNeighbours, err := c.robustPrune(entry, visitedSet, 1.2, 64)
	if err != nil {
		return fmt.Errorf("could not perform robust prune: %v", err)
	}
	fmt.Println("prunedNeighbours:", prunedNeighbours)
	log.Fatal("Not Implemented")
	return nil
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
	// ---------------------------
	for i, entry := range entries {
		if entry.Id == startId {
			continue
		}
		fmt.Println("putting entry:", i)
		if i > 1 {
			break
		}
		if err := c.putEntry(startId, entry); err != nil {
			log.Println("could not put entry:", err)
			continue
		}
	}
	// ---------------------------
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
	// ---------------------------
	fmt.Println("DONE--")
	return nil
}

func (c *Collection) Search(values numerical.Matrix, k int) ([]string, error) {
	log.Fatal("not implemented")
	return nil, nil
}
