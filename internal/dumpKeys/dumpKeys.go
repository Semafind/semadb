package main

import (
	"flag"
	"fmt"

	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"
)

const DELIMITER = "|"

func dumpBucket(tx *bbolt.Tx, name []byte, prefix string) {
	// Get the bucket
	b := tx.Bucket(name)
	if b == nil {
		return
	}
	// Dump all keys in the bucket
	err := b.ForEach(func(k, v []byte) error {
		fmt.Printf("%s%s%s%s%s\n", prefix, DELIMITER, "_b_"+string(name), DELIMITER, string(k))
		return nil
	})
	if err != nil {
		log.Error().Err(err).Str("bucket", string(name)).Msg("could not dump bucket")
	}
	// Recurse into all sub-buckets
	err = b.ForEachBucket(func(name []byte) error {
		dumpBucket(tx, name, prefix+DELIMITER+"_b_"+string(name))
		return nil
	})
	if err != nil {
		log.Error().Err(err).Str("bucket", string(name)).Msg("could not recurse into bucket")
	}

}

func main() {
	// ---------------------------
	var dbPath string
	flag.StringVar(&dbPath, "path", "", "Path to the database")
	flag.Parse()
	log.Info().Str("path", dbPath).Msg("starting dumpKeys")
	// ---------------------------
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("could not open database")
	}
	defer db.Close()
	// ---------------------------
	err = db.View(func(tx *bbolt.Tx) error {
		// Seed the initial bucket names
		err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			dumpBucket(tx, name, "")
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatal().Err(err).Msg("could not read database")
	}
	// ---------------------------
}
