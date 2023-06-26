package collection

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocql/gocql"
	"github.com/schollz/progressbar/v3"
	"github.com/tikv/client-go/v2/rawkv"
)

type CacheEntry struct {
	Entry
	dirty      bool
	edgeDirty  bool
	deleted    bool
	mutex      sync.RWMutex
	neighbours []*CacheEntry
}

func (ce *CacheEntry) setEmbeddingNoLock(embedding []float32) {
	ce.Embedding = embedding
	ce.dirty = true
}

func (ce *CacheEntry) setNeighbours(neighbours []*CacheEntry) {
	ce.mutex.Lock()
	ce.neighbours = neighbours
	ce.edgeDirty = true
	edgeList := make([]uint64, len(neighbours))
	for i, n := range neighbours {
		edgeList[i] = n.Id
	}
	ce.Edges = edgeList
	ce.mutex.Unlock()
}

func (ce *CacheEntry) setDeleted() {
	ce.mutex.Lock()
	ce.deleted = true
	ce.mutex.Unlock()
}

func (ce *CacheEntry) appendNeighbour(neighbour *CacheEntry) {
	ce.mutex.Lock()
	ce.neighbours = append(ce.neighbours, neighbour)
	ce.edgeDirty = true
	ce.Edges = append(ce.Edges, neighbour.Id)
	ce.mutex.Unlock()
}

// func (ce *CacheEntry) setEdgesNoLock(edges []string) {
// 	ce.Edges = edges
// 	ce.edgeDirty = true
// }

type NodeCache struct {
	db               *badger.DB
	mutex            sync.RWMutex
	cache            map[uint64]*CacheEntry
	startNode        *CacheEntry
	sqlDB            *sql.DB
	tikvDB           *rawkv.Client
	cassandraSession *gocql.Session
}

const (
	sqlServer = "127.0.0.1"
	// sqlPass   = "semadb-pass"
)

func NewNodeCache(db *badger.DB) (*NodeCache, error) {
	// ---------------------------
	nc := &NodeCache{
		db:    db,
		cache: make(map[uint64]*CacheEntry),
	}
	// ---------------------------
	if sqlDB, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:4000)/semadb", sqlServer)); err != nil {
		return nil, fmt.Errorf("could not open sql connection: %v", err)
	} else {
		nc.sqlDB = sqlDB
	}
	// ---------------------------
	// if tikvDB, err := rawkv.NewClientWithOpts(context.TODO(), []string{fmt.Sprintf("%s:2379", sqlServer)}); err != nil {
	// 	return nil, fmt.Errorf("could not open tikv connection: %v", err)
	// } else {
	// 	nc.tikvDB = tikvDB
	// }
	// cassandraConfig := gocql.NewCluster(fmt.Sprintf("%s:9042", sqlServer))
	// cassandraConfig.Keyspace = "semadb"
	// cassandraConfig.ProtoVersion = 4
	// cassandraConfig.Consistency = gocql.One
	// if cassandraSession, err := cassandraConfig.CreateSession(); err != nil {
	// 	return nil, fmt.Errorf("could not open cassandra connection: %v", err)
	// } else {
	// 	nc.cassandraSession = cassandraSession
	// }
	// ---------------------------
	// Read start node from database
	var startId uint64 = 0
	canLoadStartNode := false
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(STARTIDKEY))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return fmt.Errorf("could not get start node id: %v", err)
		}
		return item.Value(func(val []byte) error {
			startId = bytesToUint64(val)
			canLoadStartNode = true
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("could not get start node id: %v", err)
	}
	if canLoadStartNode {
		startNode, err := nc.getNode(startId)
		if err != nil {
			return nil, fmt.Errorf("could not get start node: %v", err)
		}
		nc.startNode = startNode
	}
	// ---------------------------
	return nc, nil
}

func (nc *NodeCache) getNode(nodeId uint64) (*CacheEntry, error) {
	nc.mutex.RLock()
	entry, ok := nc.cache[nodeId]
	nc.mutex.RUnlock()
	if ok {
		return entry, nil
	}
	// Fetch from database
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	entry, ok = nc.cache[nodeId] // Second check in case another goroutine fetched it
	if ok {
		return entry, nil
	}
	readEntry, err := nc.getNodeFromDB(nodeId)
	if err != nil {
		return nil, err
	}
	newEntry := &CacheEntry{Entry: *readEntry}
	nc.cache[nodeId] = newEntry
	return newEntry, nil
}

func (nc *NodeCache) getNodeNeighbours(ce *CacheEntry) ([]*CacheEntry, error) {
	ce.mutex.RLock()
	neighbours := ce.neighbours
	if neighbours != nil {
		ce.mutex.RUnlock()
		return neighbours, nil
	}
	ce.mutex.RUnlock()
	ce.mutex.Lock()
	defer ce.mutex.Unlock()
	edgeList := ce.Edges
	// Fetch the neighbouring entries
	neighbours = make([]*CacheEntry, len(edgeList))
	for i, nId := range edgeList {
		neighbour, err := nc.getNode(nId)
		if err != nil {
			return nil, fmt.Errorf("could not get node neighbour (%v): %v", nId, err)
		}
		neighbours[i] = neighbour
	}
	ce.neighbours = neighbours
	return neighbours, nil
}

func (nc *NodeCache) getOrSetStartNode(entry *Entry) (*CacheEntry, error) {
	nc.mutex.RLock()
	if nc.startNode != nil {
		defer nc.mutex.RUnlock()
		return nc.startNode, nil
	}
	nc.mutex.RUnlock()
	// ---------------------------
	if entry == nil {
		return nil, fmt.Errorf("could not get start node: no start node set and no entry provided")
	}
	// ---------------------------
	newStartNode, err := nc.getNode(entry.Id)
	if err != nil {
		return nil, fmt.Errorf("could not get new start node: %v", err)
	}
	newStartNode.setEmbeddingNoLock(entry.Embedding)
	nc.mutex.Lock()
	nc.startNode = newStartNode
	nc.mutex.Unlock()
	return newStartNode, nil
}

func (nc *NodeCache) flushToSQL() error {
	valueStrings := make([]string, 0, len(nc.cache))
	valueArgs := make([]interface{}, 0, len(nc.cache)*3)
	// ---------------------------
	// Flush dirty entries
	for _, ce := range nc.cache {
		// ---------------------------
		if ce.dirty {
			embeddingBytes, err := float32ToBytes(ce.Embedding)
			if err != nil {
				return fmt.Errorf("could not convert embedding to bytes: %v", err)
			}
			edgeListBytes, err := edgeListToBytes(ce.Edges)
			if err != nil {
				return fmt.Errorf("could not convert edge list to bytes: %v", err)
			}
			// _, err = nc.sqlDB.Exec("INSERT INTO nodes (id, embedding, edges) VALUES (?, ?, ?)", ce.Id, embeddingBytes, edgeListBytes)
			// "INSERT INTO player (id, coins, goods) VALUES (?, ?, ?)" + strings.Repeat(",(?,?,?)", amount-1)
			valueStrings = append(valueStrings, "(?, ?, ?)")
			valueArgs = append(valueArgs, ce.Id, embeddingBytes, edgeListBytes)
			if len(valueStrings) >= 10000 {
				_, err := nc.sqlDB.Exec("INSERT INTO nodes (id, embedding, edges) VALUES "+strings.Join(valueStrings, ","), valueArgs...)
				if err != nil {
					return fmt.Errorf("could not insert nodes: %v", err)
				}
				valueStrings = valueStrings[:0]
				valueArgs = valueArgs[:0]
			}
			// if _, err := stmt.Exec(ce.Id, embeddingBytes, edgeListBytes); err != nil {
			// 	return fmt.Errorf("could not insert node: %v", err)
			// }
			ce.dirty = false
			ce.edgeDirty = false
		}
	}
	if len(valueStrings) > 0 {
		_, err := nc.sqlDB.Exec("INSERT INTO nodes (id, embedding, edges) VALUES "+strings.Join(valueStrings, ","), valueArgs...)
		if err != nil {
			return fmt.Errorf("could not insert nodes: %v", err)
		}
	}
	return nil
}

func (nc *NodeCache) flushToTiKV() error {
	// ---------------------------
	ctx := context.TODO()
	// ---------------------------
	// Flush dirty entries
	bar := progressbar.Default(int64(len(nc.cache)))
	batchSize := 20000
	embedKeys := make([][]byte, 0, batchSize+1)
	edgeKeys := make([][]byte, 0, batchSize+1)
	embedValues := make([][]byte, 0, batchSize+1)
	edgeValues := make([][]byte, 0, batchSize+1)
	for _, ce := range nc.cache {
		// ---------------------------
		if ce.dirty {
			embeddingBytes, err := float32ToBytes(ce.Embedding)
			if err != nil {
				return fmt.Errorf("could not convert embedding to bytes: %v", err)
			}
			edgeListBytes, err := edgeListToBytes(ce.Edges)
			if err != nil {
				return fmt.Errorf("could not convert edge list to bytes: %v", err)
			}
			embedKeys = append(embedKeys, nodeEmbedKey(ce.Id))
			edgeKeys = append(edgeKeys, nodeEdgeKey(ce.Id))
			embedValues = append(embedValues, embeddingBytes)
			edgeValues = append(edgeValues, edgeListBytes)
			ce.dirty = false
			ce.edgeDirty = false
			bar.Add(1)
			if len(embedKeys) >= batchSize {
				err = nc.tikvDB.BatchPut(ctx, embedKeys, embedValues)
				if err != nil {
					return fmt.Errorf("could not put node embedding: %v", err)
				}
				err = nc.tikvDB.BatchPut(ctx, edgeKeys, edgeValues)
				if err != nil {
					return fmt.Errorf("could not put node edge list: %v", err)
				}
				embedKeys = embedKeys[:0]
				edgeKeys = edgeKeys[:0]
				embedValues = embedValues[:0]
				edgeValues = edgeValues[:0]
			}
		}
	}
	if len(embedKeys) >= 0 {
		err := nc.tikvDB.BatchPut(ctx, embedKeys, embedValues)
		if err != nil {
			return fmt.Errorf("could not put node embedding: %v", err)
		}
		err = nc.tikvDB.BatchPut(ctx, edgeKeys, edgeValues)
		if err != nil {
			return fmt.Errorf("could not put node edge list: %v", err)
		}
	}
	return nil
}

func (nc *NodeCache) flushToCassandra() error {
	// ---------------------------
	// Flush dirty entries
	bar := progressbar.Default(int64(len(nc.cache)))
	for _, ce := range nc.cache {
		// ---------------------------
		if ce.dirty {
			// ---------------------------
			// Insert into cassandra
			err := nc.cassandraSession.Query("INSERT INTO nodes (id, embedding, edges) VALUES (?, ?, ?)", ce.Id, ce.Embedding, ce.Edges).Consistency(gocql.Any).Exec()
			if err != nil {
				return fmt.Errorf("could not insert node: %v", err)
			}
			ce.dirty = false
			ce.edgeDirty = false
			bar.Add(1)
		}
	}
	return nil
}

func (nc *NodeCache) flush() error {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()
	txn := nc.db.NewTransaction(true)
	// ---------------------------
	currentTime := time.Now()
	// ---------------------------
	err := nc.flushToSQL()
	if err != nil {
		return fmt.Errorf("could not flush to sql: %v", err)
	}
	// err := nc.flushToTiKV()
	// if err != nil {
	// 	return fmt.Errorf("could not flush to tikv: %v", err)
	// }
	// err := nc.flushToCassandra()
	// if err != nil {
	// 	return fmt.Errorf("could not flush to cassandra: %v", err)
	// }
	// ---------------------------
	// Flush start node
	if nc.startNode != nil {
		err := txn.Set([]byte(STARTIDKEY), uint64ToBytes(nc.startNode.Id))
		if err == badger.ErrTxnTooBig {
			err = txn.Commit()
			if err != nil {
				return fmt.Errorf("could not commit txn: %v", err)
			}
			txn = nc.db.NewTransaction(true)
			err = txn.Set([]byte(STARTIDKEY), uint64ToBytes(nc.startNode.Id))
		}
		if err != nil {
			return fmt.Errorf("could not set start node id: %v", err)
		}
	} else {
		err := txn.Delete([]byte(STARTIDKEY))
		if err == badger.ErrTxnTooBig {
			err = txn.Commit()
			if err != nil {
				return fmt.Errorf("could not commit txn: %v", err)
			}
			txn = nc.db.NewTransaction(true)
			err = txn.Delete([]byte(STARTIDKEY))
		}
		if err != nil {
			return fmt.Errorf("could not delete start node id: %v", err)
		}
	}
	// ---------------------------
	// Print time took
	fmt.Printf("Flushed cache in %v\n", time.Since(currentTime))
	// ---------------------------
	// Flush batch
	err = txn.Commit()
	if err != nil {
		return fmt.Errorf("could not commit final txn: %v", err)
	}
	return nil
}
