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

type emptyReadOnlyBucket struct{}

func (emptyReadOnlyBucket) Get(k []byte) []byte {
	return nil
}

func (emptyReadOnlyBucket) ForEach(f func(k, v []byte) error) error {
	return nil
}

func (emptyReadOnlyBucket) PrefixScan(prefix []byte, f func(k, v []byte) error) error {
	return nil
}

// ---------------------------

// A bucket is like a kvstore, it can be used to store key-value pairs. We call
// it a bucket because usually a single kvstore layer is used to store multiple
// buckets.
type Bucket interface {
	ReadOnlyBucket
	Put([]byte, []byte) error
	Delete([]byte) error
}

type ReadOnlyBucketManager interface {
	ReadBucket(bucketName string) (ReadOnlyBucket, error)
}

type BucketManager interface {
	ReadOnlyBucketManager
	WriteBucket(bucketName string) (Bucket, error)
}

// A disk storage layer abstracts multiple buckets.
type DiskStore interface {
	// The path to where the store is located. It may be a file or a directory.
	Path() string
	Read(f func(ReadOnlyBucketManager) error) error
	Write(f func(BucketManager) error) error
	BackupToFile(path string) error
	SizeInBytes() (int64, error)
	Close() error
}

// A disk storage layer that can be used to store things in memory. Leave path
// empty to use memory.
func Open(path string) (DiskStore, error) {
	if path == "" {
		return newMemDiskStore(), nil
	}
	// ---------------------------
	bboltDB, err := bbolt.Open(path, 0644, &bbolt.Options{Timeout: 1 * time.Minute})
	if err != nil {
		return nil, fmt.Errorf("could not open db %s: %w", path, err)
	}
	return bboltDiskStore{bboltDB: bboltDB}, nil
}
