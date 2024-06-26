package main

import (
	"flag"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
)

// Run using
// go run ./internal/dumpGraph/dumpGraph.go -path /path/to/db

func main() {
	// ---------------------------
	// Get dbPath from flag
	var dbPath string
	flag.StringVar(&dbPath, "path", "", "Path to the database")
	var buckeName string
	flag.StringVar(&buckeName, "bucket", "index/vectorVamana/vector", "Name of the bucket to dump")
	flag.Parse()
	log.Info().Str("path", dbPath).Msg("starting dumpGraph")
	// ---------------------------
	db, err := diskstore.Open(dbPath)
	if err != nil {
		log.Fatal().Err(err).Msg("could not open database")
	}
	defer db.Close()
	// ---------------------------
	err = db.Read(func(bm diskstore.BucketManager) error {
		b, err := bm.Get(buckeName)
		if err != nil {
			return err
		}
		return b.ForEach(func(k, v []byte) error {
			nodeId, ok := conversion.NodeIdFromKey(k, 'e')
			if !ok {
				return nil
			}
			edges := conversion.BytesToEdgeList(v)
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
