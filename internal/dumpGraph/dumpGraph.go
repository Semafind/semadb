package main

import (
	"encoding/binary"
	"flag"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/shard/cache"
)

func bytesToEdgeList(b []byte) []uint64 {
	edges := make([]uint64, len(b)/8)
	for i := range edges {
		edges[i] = binary.LittleEndian.Uint64(b[i*8:])
	}
	return edges
}

// Run using
// go run ./internal/dumpGraph/dumpGraph.go -path /path/to/db

func main() {
	// ---------------------------
	// Get dbPath from flag
	var dbPath string
	flag.StringVar(&dbPath, "path", "", "Path to the database")
	flag.Parse()
	log.Info().Str("path", dbPath).Msg("starting dumpGraph")
	// ---------------------------
	db, err := diskstore.Open(dbPath)
	if err != nil {
		log.Fatal().Err(err).Msg("could not open database")
	}
	defer db.Close()
	// ---------------------------
	err = db.Read("graphIndex", func(graphIndex diskstore.ReadOnlyBucket) error {
		return graphIndex.ForEach(func(k, v []byte) error {
			if k[0] != 'n' || k[len(k)-1] != 'e' {
				return nil
			}
			nodeIdBytes := k[1 : len(k)-1]
			nodeId := cache.BytesToUint64(nodeIdBytes)
			edges := bytesToEdgeList(v)
			// Print as nodeid, edge1, edge2, ...
			fmt.Printf("%d", nodeId)
			for _, edge := range edges {
				fmt.Printf(",%d", edge)
			}
			fmt.Printf("\n")
			return nil
		})
	})
	if err != nil {
		log.Fatal().Err(err).Msg("could not read graph index")
	}
	// ---------------------------
}
