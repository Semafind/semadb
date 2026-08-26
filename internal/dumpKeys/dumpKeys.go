package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

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
		slog.Error("could not dump bucket", "bucket", string(name), "error", err)
	}
	// Recurse into all sub-buckets
	err = b.ForEachBucket(func(name []byte) error {
		dumpBucket(tx, name, prefix+DELIMITER+"_b_"+string(name))
		return nil
	})
	if err != nil {
		slog.Error("could not recurse into bucket", "bucket", string(name), "error", err)
	}

}

func main() {
	// ---------------------------
	var dbPath string
	flag.StringVar(&dbPath, "path", "", "Path to the database")
	flag.Parse()
	slog.Info("starting dumpKeys", "path", dbPath)
	// ---------------------------
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		slog.Error("could not open database", "error", err)
		os.Exit(1)
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
		slog.Error("could not read database", "error", err)
		os.Exit(1)
	}
	// ---------------------------
}
