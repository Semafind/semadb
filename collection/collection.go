package collection

import (
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/schollz/progressbar/v3"
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
	newNode, err := nodeCache.getNode(entry.Id)
	if err != nil {
		return fmt.Errorf("could not get node from cache: %v", err)
	}
	newNode.setEmbeddingNoLock(entry.Embedding)
	// ---------------------------
	prunedNeighbours, err := c.robustPrune(entry, visitedSet, alpha, degreeBound, nodeCache)
	if err != nil {
		return fmt.Errorf("could not perform robust prune: %v", err)
	}
	newNode.setEdgesNoLock(prunedNeighbours)
	// ---------------------------
	// Add the bidirectional edges
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, neighbourId := range prunedNeighbours {
		neighbour, err := nodeCache.getNode(neighbourId)
		if err != nil {
			return fmt.Errorf("could not get node from cache for bidirectional edges: %v", err)
		}
		neighbour.mutex.Lock()
		neighbourNeighbours, err := nodeCache.getNodeNeighbours(neighbourId)
		if err != nil {
			return fmt.Errorf("could not get node neighbours for bidirectional edges: %v", err)
		}
		// ---------------------------
		if len(neighbourNeighbours)+1 > degreeBound+rng.Intn(degreeBound) {
			candidateSet := NewDistSet(neighbour.Embedding, len(neighbourNeighbours)+1)
			candidateSet.AddEntry(neighbourNeighbours...)
			candidateSet.AddEntry(newNode)
			candidateSet.Sort()
			// Prune the neighbour
			neighbourPrunedEdges, err := c.robustPrune(neighbour.Entry, candidateSet, alpha, degreeBound, nodeCache)
			if err != nil {
				return fmt.Errorf("could not perform robust prune for bidirectional edges: %v", err)
			}
			neighbour.setEdgesNoLock(neighbourPrunedEdges)
		} else {
			// Append the current entry to the edge list of the neighbour
			neighbour.setEdgesNoLock(append(neighbour.Edges, newNode.Id))
		}
		neighbour.mutex.Unlock()
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
	var wg sync.WaitGroup
	profileFile, _ := os.Create("dump/cpu.prof")
	bar := progressbar.Default(int64(len(entries)) - 1)
	pprof.StartCPUProfile(profileFile)
	defer pprof.StopCPUProfile()
	nodeCache := NewNodeCache(c.db)
	putQueue := make(chan Entry, len(entries))
	// Start the workers
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		go func() {
			for entry := range putQueue {
				if err := c.putEntry(startId, entry, nodeCache); err != nil {
					log.Println("could not put entry:", err)
				}
				bar.Add(1)
				wg.Done()
			}
		}()
	}
	// Submit the entries to the workers
	for _, entry := range entries {
		if entry.Id == startId {
			continue
		}
		wg.Add(1)
		putQueue <- entry
	}
	close(putQueue)
	wg.Wait()
	if err := nodeCache.flush(); err != nil {
		return fmt.Errorf("could not flush node cache: %v", err)
	}
	// ---------------------------
	nodeCount, _ := c.getNodeCount()
	// ---------------------------
	// c.DumpCacheToCSVGraph("dump/graph.csv", nodeCache)
	// ---------------------------
	fmt.Println("Final node count:", nodeCount)
	return nil
}

func (c *Collection) Search(vector []float32, k int) ([]string, error) {
	// ---------------------------
	searchSize := 75
	nodeCache := NewNodeCache(c.db)
	// ---------------------------
	searchSet, _, err := c.greedySearch("0", vector, k, searchSize, nodeCache)
	if err != nil {
		return nil, fmt.Errorf("could not perform greedy search: %v", err)
	}
	searchSet.KeepFirstK(k)
	nearestNodeIds := make([]string, len(searchSet.items))
	for i, item := range searchSet.items {
		nearestNodeIds[i] = item.id
	}
	return nearestNodeIds, nil
}

func (c *Collection) DumpCacheToCSVGraph(fname string, nc *NodeCache) error {
	// Check if fname ends with csv
	if !strings.HasSuffix(fname, ".csv") {
		fname += ".csv"
	}
	// ---------------------------
	// Open the file
	f, err := os.Create(fname)
	if err != nil {
		return fmt.Errorf("could not create file: %v", err)
	}
	defer f.Close()
	// ---------------------------
	csvWriter := csv.NewWriter(f)
	// ---------------------------
	for _, ce := range nc.cache {
		edgeListRow := make([]string, len(ce.Edges)+1)
		edgeListRow[0] = ce.Id
		copy(edgeListRow[1:], ce.Edges)
		if err := csvWriter.Write(edgeListRow); err != nil {
			return fmt.Errorf("could not write to csv: %v", err)
		}
	}
	return nil
}
