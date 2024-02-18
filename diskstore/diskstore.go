package diskstore

import (
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

type ReadOnlyBucket interface {
	Get([]byte) []byte
	ForEach(func(k, v []byte) error) error
	PrefixScan(prefix []byte, f func(k, v []byte) error) error
}

// A bucket is like a kvstore, it can be used to store key-value pairs. We call
// it a bucket because usually a single kvstore layer is used to store multiple
// buckets.
type Bucket interface {
	ReadOnlyBucket
	Put([]byte, []byte) error
	Delete([]byte) error
}

// A disk storage layer abstracts multiple buckets.
type DiskStore interface {
	// The path to where the store is located. It may be a file or a directory.
	Path() string
	CreateBucketsIfNotExists(bucketNames []string) error
	Read(bucketName string, f func(ReadOnlyBucket) error) error
	Write(bucketName string, f func(Bucket) error) error
	BackupToFile(path string) error
	Close() error
}

// A disk storage layer that can be used to store things in memory. Leave path
// empty to use memory.
func Open(path string) (DiskStore, error) {
	if path == "" {
		return NewMemDiskStore(), nil
	}
	// ---------------------------
	bboltDB, err := bbolt.Open(path, 0644, &bbolt.Options{Timeout: 1 * time.Minute})
	if err != nil {
		return nil, fmt.Errorf("could not open db %s: %w", path, err)
	}
	return BBoltDiskStore{bboltDB: bboltDB}, nil
}
