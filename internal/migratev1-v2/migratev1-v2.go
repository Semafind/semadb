/*
This package attempts to migrate the old database format to the new one. It is
not guaranteed to work and should be used with caution. The package is not
maintained and is only provided as a reference.

We should at some point delete this migration package as it is not useful
anymore.
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

type v1Collection struct {
	UserId     string
	Id         string
	VectorSize uint
	DistMetric string
	Replicas   uint
	Algorithm  string
	Timestamp  int64
	CreatedAt  int64
	ShardIds   []string
	// Active user plan
	UserPlan models.UserPlan
}

/*
nodedb.bbolt

internal: delete, nothing useful here to transfer

userCollections:
  userId/collectionId -> OldCollection

*/

func migrateNodeDB(oldDb, newDb *bbolt.DB) error {
	newUserCols := make(map[string][]byte)
	// Create the bucket if it does not exist
	err := oldDb.View(func(tx *bbolt.Tx) error {
		// ---------------------------
		ucb := tx.Bucket([]byte("userCollections"))
		if ucb == nil {
			return fmt.Errorf("bucket userCollections not found")
		}
		// ---------------------------
		// For each cannot modify the bucket
		err := ucb.ForEach(func(k, v []byte) error {
			fmt.Println("Migrating collection", string(k))
			// ---------------------------
			var oldCollection v1Collection
			if err := msgpack.Unmarshal(v, &oldCollection); err != nil {
				return err
			}
			// ---------------------------
			newCollection := models.Collection{
				UserId:    oldCollection.UserId,
				Id:        oldCollection.Id,
				Replicas:  oldCollection.Replicas,
				Timestamp: time.Now().Unix(),
				CreatedAt: oldCollection.CreatedAt,
				ShardIds:  oldCollection.ShardIds,
				IndexSchema: models.IndexSchema{
					"vector": models.IndexSchemaValue{
						Type: models.IndexTypeVectorVamana,
						VectorVamana: &models.IndexVectorVamanaParameters{
							VectorSize:     oldCollection.VectorSize,
							DistanceMetric: oldCollection.DistMetric,
							SearchSize:     75,
							DegreeBound:    64,
							Alpha:          1.2,
						},
					},
				},
			}
			// ---------------------------
			ncBytes, err := msgpack.Marshal(newCollection)
			if err != nil {
				return fmt.Errorf("failed to marshal new collection: %w", err)
			}
			// ---------------------------
			newUserCols[string(k)] = ncBytes
			// ---------------------------
			return nil
		})
		// ---------------------------
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to update old database: %w", err)
	}
	err = newDb.Update(func(tx *bbolt.Tx) error {
		ucb, err := tx.CreateBucketIfNotExists([]byte("userCollections"))
		if err != nil {
			return err
		}
		// ---------------------------
		for k, v := range newUserCols {
			if err := ucb.Put([]byte(k), v); err != nil {
				return fmt.Errorf("failed to put new collection %s: %w", k, err)
			}
		}
		// ---------------------------
		return nil
	})
	return err
}

/*
sharddb.bbolt

internal: fullCopy

OldShard:
	graphIndex:
	n<nodeId>e -> NodeEdges
	n<nodeId>v -> NodeVector

	points:
	n<nodeId>i -> pointUUID
	n<pointId>m -> pointMetadata
	p<pointId>i -> nodeId

NewShard:
	index/vectorVamana/vector:
	n<nodeId>v -> NodeVector
	n<nodeId>e -> NodeEdges

	points:
	n<nodeId>i -> pointUUID
	n<pointId>d -> pointData
	p<pointId>i -> nodeId


*/

func migrateShardDb(oldDb, newDb *bbolt.DB) error {
	internalKVs := make(map[string][]byte)
	vamanaIndexKVs := make(map[string][]byte)
	pointsKVs := make(map[string][]byte)
	// ---------------------------
	err := oldDb.View(func(tx *bbolt.Tx) error {
		// ---------------------------
		internal := tx.Bucket([]byte("internal"))
		if internal == nil {
			return fmt.Errorf("bucket internal not found")
		}
		err := internal.ForEach(func(k, v []byte) error {
			fmt.Println("Migrating internal key", string(k))
			internalKVs[string(k)] = v
			return nil
		})
		if err != nil {
			return err
		}
		// ---------------------------
		graphIndex := tx.Bucket([]byte("graphIndex"))
		if graphIndex == nil {
			return fmt.Errorf("bucket graphIndex not found")
		}
		err = graphIndex.ForEach(func(k, v []byte) error {
			fmt.Println("Migrating graphIndex key", string(k))
			vamanaIndexKVs[string(k)] = v
			return nil
		})
		if err != nil {
			return err
		}
		// ---------------------------
		points := tx.Bucket([]byte("points"))
		if points == nil {
			return fmt.Errorf("bucket points not found")
		}
		err = points.ForEach(func(k, v []byte) error {
			fmt.Println("Migrating points key", string(k))
			switch {
			case k[0] == 'n' && k[9] == 'i':
				pointsKVs[string(k)] = v
			case k[0] == 'p' && k[17] == 'i':
				pointsKVs[string(k)] = v
			case k[0] == 'n' && k[9] == 'm':
				// Metadata becomes {'metadata': pointMetadata} object
				var metadata any
				if err := msgpack.Unmarshal(v, &metadata); err != nil {
					return fmt.Errorf("failed to unmarshal metadata: %w", err)
				}
				newData := map[string]any{"metadata": metadata}
				newDataBytes, err := msgpack.Marshal(newData)
				if err != nil {
					return fmt.Errorf("failed to marshal new metadata: %w", err)
				}
				newKey := [10]byte{}
				copy(newKey[:], k[:10])
				newKey[9] = 'd'
				pointsKVs[string(newKey[:])] = newDataBytes
			}
			return nil
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to read old database: %w", err)
	}
	// ---------------------------
	log.Println("Migrating internal keys", len(internalKVs))
	err = newDb.Update(func(tx *bbolt.Tx) error {
		// ---------------------------
		internal, err := tx.CreateBucketIfNotExists([]byte("internal"))
		if err != nil {
			return err
		}
		for k, v := range internalKVs {
			if err := internal.Put([]byte(k), v); err != nil {
				return fmt.Errorf("failed to put internal key %s: %w", k, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to add new internal: %w", err)
	}
	log.Println("Migrating graphIndex keys", len(vamanaIndexKVs))
	err = newDb.Update(func(tx *bbolt.Tx) error {
		// ---------------------------
		graphIndex, err := tx.CreateBucketIfNotExists([]byte("index/vectorVamana/vector"))
		if err != nil {
			return err
		}
		for k, v := range vamanaIndexKVs {
			if err := graphIndex.Put([]byte(k), v); err != nil {
				return fmt.Errorf("failed to put graphIndex key %s: %w", k, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to add new vamanaIndex: %w", err)
	}
	log.Println("Migrating points keys", len(pointsKVs))
	err = newDb.Update(func(tx *bbolt.Tx) error {
		// ---------------------------
		points, err := tx.CreateBucketIfNotExists([]byte("points"))
		if err != nil {
			return err
		}
		for k, v := range pointsKVs {
			if err := points.Put([]byte(k), v); err != nil {
				return fmt.Errorf("failed to put points key %s: %w", k, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to add new points: %w", err)
	}
	return err
}

func main() {
	// Database file flag
	dbFile := flag.String("db", "graph.db", "Path to the BoltDB file")
	// Parse the flags
	flag.Parse()
	// ---------------------------
	// Check if the old database file exists
	oldDbFile := *dbFile + ".v1"
	if _, err := os.Stat(oldDbFile); os.IsNotExist(err) {
		log.Println("Old database file not found, creating", oldDbFile)
		if err := os.Rename(*dbFile, oldDbFile); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("Old database file found", oldDbFile)
	}
	// Open the database file
	oldDb, err := bbolt.Open(oldDbFile, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	// Close the database
	defer oldDb.Close()
	// ---------------------------
	// Open a new database file
	newDb, err := bbolt.Open(*dbFile, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer newDb.Close()
	// ---------------------------
	// Check if the dbfile ends with nodedb.bbolt
	if strings.HasSuffix(*dbFile, "nodedb.bbolt") {
		if err := migrateNodeDB(oldDb, newDb); err != nil {
			log.Fatal(err)
		}
	} else if strings.HasSuffix(*dbFile, "sharddb.bbolt") {
		if err := migrateShardDb(oldDb, newDb); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("Unknown database file")
	}
	// ---------------------------
}
