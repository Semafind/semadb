package diskstore

import (
	"errors"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

type emptyReadOnlyBucket struct{}

func (emptyReadOnlyBucket) IsReadOnly() bool {
	return true
}

func (emptyReadOnlyBucket) Get(k []byte) []byte {
	return nil
}

func (emptyReadOnlyBucket) ForEach(f func(k, v []byte) error) error {
	return nil
}

func (emptyReadOnlyBucket) PrefixScan(prefix []byte, f func(k, v []byte) error) error {
	return nil
}

func (emptyReadOnlyBucket) RangeScan(start, end []byte, inclusive bool, f func(k, v []byte) error) error {
	return nil
}

func (emptyReadOnlyBucket) Put(k, v []byte) error {
	return errors.New("cannot put into empty read-only bucket")
}

func (emptyReadOnlyBucket) Delete(k []byte) error {
	return errors.New("cannot delete from empty read-only bucket")
}

// ---------------------------

// A convenience type for read-only buckets. Can be used as an argument to a function
// to restrict the actions it can perform.
type ReadOnlyBucket interface {
	Get([]byte) []byte
	// Potentially unordered iteration over all key-value pairs.
	ForEach(func(k, v []byte) error) error
	// Scan all key-value pairs with a given prefix. The function f will be called
	// with all key-value pairs that have the given prefix.
	PrefixScan(prefix []byte, f func(k, v []byte) error) error
	// Scan over range of key-value pairs. Giving nil as start or end will start
	// or end at the beginning or end of the bucket.
	RangeScan(start, end []byte, inclusive bool, f func(k, v []byte) error) error
}

// A bucket is like a kvstore, it can be used to store key-value pairs. We call
// it a bucket because usually a single kvstore layer is used to store multiple
// buckets.
type Bucket interface {
	ReadOnlyBucket
	IsReadOnly() bool
	Put([]byte, []byte) error
	Delete([]byte) error
}

type BucketManager interface {
	Get(bucketName string) (Bucket, error)
	Delete(bucketName string) error
}

// A disk storage layer abstracts multiple buckets.
type DiskStore interface {
	// The path to where the store is located. It may be a file or a directory.
	Path() string
	Read(f func(BucketManager) error) error
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
