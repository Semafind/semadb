package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/semafind/semadb/collection"
	"github.com/semafind/semadb/numerical"
)

// ---------------------------

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%v took %v", name, elapsed)
}

// ---------------------------

// var globalNodeCache *nodecache.NodeCache = nodecache.NewNodeCache(100, 10*time.Minute)

func main() {
	fmt.Println("Generating random key values.")
	defer timeTrack(time.Now(), "main")
	// ---------------------------
	// randCollectionId := fmt.Sprintf("%x", time.Now().Unix())
	randCollectionId := "test"
	fmt.Println("randCollectionId", randCollectionId)
	// ---------------------------
	fmt.Println("Opening badger db 1.")
	db, err := badger.Open(badger.DefaultOptions("dump/" + randCollectionId))
	if err != nil {
		log.Fatal(err)
	}
	// ---------------------------
	const N = 1_000
	const dims = 128
	// Generate random key strings
	docIds := make([]string, N)
	for i := range docIds {
		docIds[i] = fmt.Sprintf("ID%x", (i+1)*rand.Intn(1000))
	}
	embeddings := numerical.RandMatrix(N, dims)
	fmt.Printf("Generated random %v key values.\n", N)
	fmt.Printf("Example vector slice: %v\n", embeddings.Data[:2])
	// ---------------------------
	entries := make([]collection.Entry, N)
	for i := range entries {
		entries[i] = collection.Entry{Id: docIds[i], Embedding: embeddings.Data[i*dims : (i+1)*dims]}
	}
	collection := collection.NewCollection(randCollectionId, db)
	err = collection.Put(entries)
	if err != nil {
		fmt.Println(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}
