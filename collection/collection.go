package collection

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/schollz/progressbar/v3"
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

func (c *Collection) putEntry(startNodeId string, entry Entry, nodeCache *NodeCache) error {
	// ---------------------------
	searchSize := 75
	degreeBound := 64
	alpha := float32(1.2)
	// ---------------------------
	_, visitedSet, err := c.greedySearch(startNodeId, entry.Embedding, 1, searchSize, nodeCache)
	if err != nil {
		return fmt.Errorf("could not perform greedy search: %v", err)
	}
	nodeCache.setNode(&entry)
	// ---------------------------
	prunedNeighbours, err := c.robustPrune(entry, visitedSet, alpha, degreeBound, nodeCache)
	if err != nil {
		return fmt.Errorf("could not perform robust prune: %v", err)
	}
	nodeCache.setNodeEdges(entry.Id, prunedNeighbours)
	if err != nil {
		return fmt.Errorf("could not set node as entry: %v", err)
	}
	// ---------------------------
	// Add the bidirectional edges
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, neighbourId := range prunedNeighbours {
		neighbourEdgeEntries, err := nodeCache.getNodeNeighbours(neighbourId, c)
		if err != nil {
			return fmt.Errorf("could not get node neighbours for bidirectional edges: %v", err)
		}
		// Check if the neighbour already has the entry as a neighbour
		alreadyNeighbour := false
		for _, edgeEntry := range neighbourEdgeEntries {
			if edgeEntry.Id == entry.Id {
				alreadyNeighbour = true
				break
			}
		}
		if alreadyNeighbour {
			continue
		}
		if len(neighbourEdgeEntries)+1 > degreeBound+rng.Intn(degreeBound) {
			neighbourEntry, err := nodeCache.getNode(neighbourId, c)
			if err != nil {
				return fmt.Errorf("could not get node embedding for bidirectional edges: %v", err)
			}
			neighbourEdgeEntries = append(neighbourEdgeEntries, neighbourEntry)
			candidateSet := NewDistSet(neighbourEntry.Embedding, len(neighbourEdgeEntries))
			candidateSet.AddEntry(neighbourEdgeEntries...)
			candidateSet.Sort()
			// Prune the neighbour
			neighbourPrunedEdges, err := c.robustPrune(*neighbourEntry, candidateSet, alpha, degreeBound, nodeCache)
			if err != nil {
				return fmt.Errorf("could not perform robust prune for bidirectional edges: %v", err)
			}
			nodeCache.setNodeEdges(neighbourId, neighbourPrunedEdges)
			if err != nil {
				return fmt.Errorf("could not set node edges for bidirectional edges: %v", err)
			}
		} else {
			// Append the current entry to the edge list of the neighbour
			edgeList := make([]string, len(neighbourEdgeEntries)+1)
			for i, edge := range neighbourEdgeEntries {
				edgeList[i] = edge.Id
			}
			edgeList[len(neighbourEdgeEntries)] = entry.Id
			nodeCache.setNodeEdges(neighbourId, edgeList)
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
	// ---------------------------
	// var wg sync.WaitGroup
	profileFile, _ := os.Create("dump/cpu.prof")
	bar := progressbar.Default(int64(len(entries)) - 1)
	pprof.StartCPUProfile(profileFile)
	defer pprof.StopCPUProfile()
	nodeCache := NewNodeCache()
	for _, entry := range entries {
		if entry.Id == startId {
			continue
		}
		bar.Add(1)
		// wg.Add(1)
		// go func(entry Entry) {
		// 	fmt.Println("putting entry:", entry.Id)
		// 	if err := c.putEntry(startId, entry); err != nil {
		// 		log.Println("could not put entry:", err)
		// 	}
		// 	wg.Done()
		// }(entry)
		if err := c.putEntry(startId, entry, nodeCache); err != nil {
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
