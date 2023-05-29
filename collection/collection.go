package collection

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/semafind/semadb/numerical"
)

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
	// ---------------------------
	searchSize := 75
	degreeBound := 64
	alpha := float32(1.2)
	// ---------------------------
	_, visitedSet, err := c.greedySearch(startNodeId, entry.Embedding, 1, searchSize)
	if err != nil {
		return fmt.Errorf("could not perform greedy search: %v", err)
	}
	// ---------------------------
	prunedNeighbours, err := c.robustPrune(entry, visitedSet, alpha, degreeBound)
	if err != nil {
		return fmt.Errorf("could not perform robust prune: %v", err)
	}
	entry.Edges = prunedNeighbours
	err = c.setNodeAsEntry(entry, 1)
	if err != nil {
		return fmt.Errorf("could not set node as entry: %v", err)
	}
	// ---------------------------
	// Add the bidirectional edges
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, neighbourId := range prunedNeighbours {
		neighbourEdgeEntries, err := c.getNodeNeighbours(neighbourId)
		if err != nil {
			return fmt.Errorf("could not get node neighbours for bidirectional edges: %v", err)
		}
		if len(neighbourEdgeEntries)+1 > degreeBound+rng.Intn(degreeBound) {
			// Prune the neighbour
			// log.Println("pruning neighbour:", neighbourId)
			neighbourEmbedding, err := c.getNodeEmbedding(neighbourId)
			if err != nil {
				return fmt.Errorf("could not get node embedding for bidirectional edges: %v", err)
			}
			candidateSet := NewDistSet(len(neighbourEdgeEntries) + 1)
			// Add current entry
			candidateSet.Add(&DistSetElem{distance: eucDist(entry.Embedding, neighbourEmbedding), id: entry.Id, embedding: entry.Embedding})
			// Add neighbour edge candidates to prune
			for _, edgeEntry := range neighbourEdgeEntries {
				candidateSet.Add(&DistSetElem{distance: eucDist(neighbourEmbedding, edgeEntry.Embedding), id: edgeEntry.Id, embedding: edgeEntry.Embedding})
			}
			candidateSet.Sort()
			// Prune the neighbour
			neighbourPrunedEdges, err := c.robustPrune(Entry{Id: neighbourId, Embedding: neighbourEmbedding}, candidateSet, alpha, degreeBound)
			if err != nil {
				return fmt.Errorf("could not perform robust prune for bidirectional edges: %v", err)
			}
			err = c.setNodeNeighbours(neighbourId, neighbourPrunedEdges)
			if err != nil {
				return fmt.Errorf("could not set node neighbours for bidirectional edges: %v", err)
			}
		} else {
			// Append the current entry to the edge list of the neighbour
			edgeList := make([]string, len(neighbourEdgeEntries)+1)
			for i, edge := range neighbourEdgeEntries {
				edgeList[i] = edge.Id
			}
			edgeList[len(neighbourEdgeEntries)] = entry.Id
			err = c.setNodeNeighbours(neighbourId, edgeList)
			if err != nil {
				return fmt.Errorf("could not set node neighbours for bidirectional edges: %v", err)
			}
		}
	}
	// ---------------------------
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
	// var wg sync.WaitGroup
	profileFile, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(profileFile)
	defer pprof.StopCPUProfile()
	for i, entry := range entries {
		if entry.Id == startId {
			continue
		}
		if i > 200 {
			break
		}
		// wg.Add(1)
		// go func(entry Entry) {
		// 	fmt.Println("putting entry:", entry.Id)
		// 	if err := c.putEntry(startId, entry); err != nil {
		// 		log.Println("could not put entry:", err)
		// 	}
		// 	wg.Done()
		// }(entry)
		fmt.Println("putting entry:", i)
		if err := c.putEntry(startId, entry); err != nil {
			log.Println("could not put entry:", err)
			continue
		}
	}
	// ---------------------------
	nodeCount, _ := c.getNodeCount()
	// ---------------------------
	fmt.Println("Final node count:", nodeCount)
	return nil
}

func (c *Collection) Search(values numerical.Matrix, k int) ([]string, error) {
	log.Fatal("not implemented")
	return nil, nil
}
