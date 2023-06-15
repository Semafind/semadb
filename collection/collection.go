package collection

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/schollz/progressbar/v3"
	"github.com/vmihailenco/msgpack/v5"
	"gonum.org/v1/gonum/blas/blas32"
)

// ---------------------------

type CollectionConfig struct {
	SearchSize  int     `json:"searchSize" default:"75"`
	DegreeBound int     `json:"degreeBound" default:"64"`
	Alpha       float32 `json:"alpha" default:"1.2"`
	EmbedDim    uint    `json:"embedDim" binding:"required"`
	DistMetric  string  `json:"distMetric" default:"euclidean"`
	Description string  `json:"description"`
}

func DefaultCollectionConfig(embedDim uint) CollectionConfig {
	return CollectionConfig{
		SearchSize:  75,
		DegreeBound: 64,
		Alpha:       1.2,
		EmbedDim:    embedDim,
		DistMetric:  "euclidean",
	}
}

// ---------------------------

type Collection struct {
	Id          string
	Config      CollectionConfig
	db          *badger.DB
	cache       *NodeCache
	startNodeId string
	mutex       sync.RWMutex
}

func NewCollection(config CollectionConfig) (*Collection, error) {
	newId, err := uuid.NewUUID()
	if err != nil {
		return nil, fmt.Errorf("could not generate new collection id: %v", err)
	}
	colId := newId.String()
	// ---------------------------
	dbDir, ok := os.LookupEnv("DBDIR")
	if !ok {
		dbDir = "dump"
	}
	collectionDir := filepath.Join(dbDir, colId)
	// ---------------------------
	// Check if collection directory already exists
	// This check almost certainly won't fail since UUIDs have low chance of collision
	if _, err := os.Stat(collectionDir); !os.IsNotExist(err) {
		return nil, fmt.Errorf("collection already exists: %v", collectionDir)
	}
	// ---------------------------
	// Create collection directory
	db, err := badger.Open(badger.DefaultOptions(collectionDir))
	if err != nil {
		return nil, fmt.Errorf("could not open database for collection (%v): %v", colId, err)
	}
	// ---------------------------
	newCollection := &Collection{Id: colId, Config: config, db: db, cache: NewNodeCache(db)}
	newCollection.writeConfig()
	// ---------------------------
	return newCollection, nil
}

func OpenCollection(colId string) (*Collection, error) {
	// ---------------------------
	dbDir, ok := os.LookupEnv("DBDIR")
	if !ok {
		dbDir = "dump"
	}
	collectionDir := filepath.Join(dbDir, colId)
	// ---------------------------
	// Check if collection directory already exists
	if _, err := os.Stat(collectionDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("collection does not exist: %v", collectionDir)
	}
	// ---------------------------
	// Open collection directory
	db, err := badger.Open(badger.DefaultOptions(collectionDir))
	if err != nil {
		return nil, fmt.Errorf("could not open database for collection (%v): %v", colId, err)
	}
	// ---------------------------
	collection := &Collection{Id: colId, db: db, cache: NewNodeCache(db)}
	collection.readConfig()
	// ---------------------------
	_, err = collection.getOrSetStartId(nil, false)
	if err != nil {
		return nil, fmt.Errorf("could not get or set start node id: %v", err)
	}
	// ---------------------------
	return collection, nil
}

func (c *Collection) Close() error {
	if err := c.cache.flush(); err != nil {
		return fmt.Errorf("could not flush cache on close: %v", err)
	}
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("could not close database: %v", err)
	}
	return nil
}

func (c *Collection) writeConfig() error {
	err := c.db.Update(func(txn *badger.Txn) error {
		configBytes, err := msgpack.Marshal(c.Config)
		if err != nil {
			return fmt.Errorf("could not marshal config: %v", err)
		}
		err = txn.Set([]byte(CONFIGKEY), configBytes)
		if err != nil {
			return fmt.Errorf("could not write config: %v", err)
		}
		return nil
	})
	return err
}

func (c *Collection) readConfig() error {
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(CONFIGKEY))
		if err != nil {
			return fmt.Errorf("could not get config: %v", err)
		}
		return item.Value(func(val []byte) error {
			err = msgpack.Unmarshal(val, &c.Config)
			if err != nil {
				return fmt.Errorf("could not unmarshal config: %v", err)
			}
			return nil
		})
	})
	return err
}

func (c *Collection) putEntry(startNodeId string, entry Entry, nodeCache *NodeCache) error {
	// ---------------------------
	searchSize := c.Config.SearchSize
	degreeBound := c.Config.DegreeBound
	alpha := c.Config.Alpha
	// ---------------------------
	startNode, err := c.cache.getNode(startNodeId)
	if err != nil {
		return fmt.Errorf("could not get start node from cache: %v", err)
	}
	// ---------------------------
	_, visitedSet, err := c.greedySearch(startNode, entry.Embedding, 1, searchSize, nodeCache)
	if err != nil {
		return fmt.Errorf("could not perform greedy search: %v", err)
	}
	newNode, err := nodeCache.getNode(entry.Id)
	if err != nil {
		return fmt.Errorf("could not get node from cache: %v", err)
	}
	newNode.setEmbeddingNoLock(entry.Embedding)
	// ---------------------------
	prunedNeighbours, err := c.robustPrune(entry, visitedSet, alpha, degreeBound)
	if err != nil {
		return fmt.Errorf("could not perform robust prune: %v", err)
	}
	// newNode.setEdgesNoLock(prunedNeighbours)
	newNode.setNeighbours(prunedNeighbours)
	// ---------------------------
	// Add the bidirectional edges
	for _, neighbour := range prunedNeighbours {
		neighbourNeighbours, err := nodeCache.getNodeNeighbours(neighbour)
		if err != nil {
			return fmt.Errorf("could not get node neighbours for bidirectional edges: %v", err)
		}
		// ---------------------------
		if len(neighbourNeighbours)+1 > degreeBound+rand.Intn(degreeBound) {
			candidateSet := NewDistSet(neighbour.Embedding, len(neighbourNeighbours)+1, c.dist)
			candidateSet.AddEntry(neighbourNeighbours...)
			candidateSet.AddEntry(newNode)
			candidateSet.Sort()
			// Prune the neighbour
			neighbourPrunedEdges, err := c.robustPrune(neighbour.Entry, candidateSet, alpha, degreeBound)
			if err != nil {
				return fmt.Errorf("could not perform robust prune for bidirectional edges: %v", err)
			}
			// neighbour.mutex.Lock()
			neighbour.setNeighbours(neighbourPrunedEdges)
		} else {
			// Append the current entry to the edge list of the neighbour
			// neighbour.mutex.Lock()
			neighbour.appendNeighbour(newNode)
		}
		// neighbour.mutex.Unlock()
	}
	// ---------------------------
	// Find max and minimum distances
	// if len(prunedDists) < (degreeBound * 3 / 4) {
	// 	return nil
	// }
	// var minDist float32 = math.MaxFloat32
	// var maxDist float32 = 0.0
	// var meanDist float32 = 0.0
	// for _, dist := range prunedDists {
	// 	if dist < minDist {
	// 		minDist = dist
	// 	}
	// 	if dist > maxDist {
	// 		maxDist = dist
	// 	}
	// 	meanDist += dist
	// }
	// meanDist /= float32(len(prunedDists))
	// normMeanDist := (meanDist - minDist) / (maxDist - minDist + 1e-6)
	// if len(prunedDists) >= len(startNode.Edges) && normMeanDist < 0.5 {
	// 	log.Printf("Replacing start node %v -> %v\n", startNode.Id, newNode.Id)
	// 	_, err := c.getOrSetStartId(&entry, true)
	// 	if err != nil {
	// 		return fmt.Errorf("could not get or set start node id: %v", err)
	// 	}
	// }
	// ---------------------------
	return nil
}

func (c *Collection) Put(entries []Entry) error {
	// Sanity checks
	if len(entries) == 0 {
		return nil
	}
	// ---------------------------
	centroid := make([]float32, c.Config.EmbedDim)
	cv := blas32.Vector{
		N:    len(centroid),
		Inc:  1,
		Data: centroid,
	}
	for _, entry := range entries {
		blas32.Axpy(1.0, blas32.Vector{
			N:    len(entry.Embedding),
			Inc:  1,
			Data: entry.Embedding,
		}, cv)
	}
	blas32.Scal(1.0/float32(len(entries)), cv)
	// Find closest entry to the centroid
	var closestEntry Entry
	var closestDist float32 = math.MaxFloat32
	for _, entry := range entries {
		dist := c.dist(entry.Embedding, centroid)
		if dist < closestDist {
			closestDist = dist
			closestEntry = entry
		}
	}
	// ---------------------------
	// Check if the database has been initialised with at least one node
	startId, err := c.getOrSetStartId(&closestEntry, false)
	if err != nil {
		return fmt.Errorf("could not get or set start node id: %v", err)
	}
	c.putEntry(startId, entries[0], c.cache)
	// ---------------------------
	var wg sync.WaitGroup
	bar := progressbar.Default(int64(len(entries)) - 1)
	putQueue := make(chan Entry, len(entries))
	// Start the workers
	numWorkers := runtime.NumCPU() * 2
	for i := 0; i < numWorkers; i++ {
		go func() {
			for entry := range putQueue {
				if err := c.putEntry(startId, entry, c.cache); err != nil {
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
	if err := c.cache.flush(); err != nil {
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
	startId, err := c.getOrSetStartId(nil, false)
	if err != nil {
		return nil, fmt.Errorf("could not get start id: %v", err)
	}
	// ---------------------------
	startNode, err := c.cache.getNode(startId)
	if err != nil {
		return nil, fmt.Errorf("could not get start node: %v", err)
	}
	// ---------------------------
	searchSet, _, err := c.greedySearch(startNode, vector, k, c.Config.SearchSize, c.cache)
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
