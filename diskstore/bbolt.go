package diskstore

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt"
)

type bboltBucket struct {
	bb *bbolt.Bucket
}

func (b bboltBucket) Get(k []byte) []byte {
	// Not huge fan of this b.bb business but it's explicit.
	return b.bb.Get(k)
}

func (b bboltBucket) Put(k, v []byte) error {
	return b.bb.Put(k, v)
}

func (b bboltBucket) Delete(k []byte) error {
	return b.bb.Delete(k)
}

func (b bboltBucket) ForEach(f func(k, v []byte) error) error {
	return b.bb.ForEach(func(k, v []byte) error {
		return f(k, v)
	})
}

func (b bboltBucket) PrefixScan(prefix []byte, f func(k, v []byte) error) error {
	c := b.bb.Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if err := f(k, v); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------

type bboltBucketManager struct {
	tx *bbolt.Tx
}

func (bm bboltBucketManager) ReadBucket(bucketName string) (ReadOnlyBucket, error) {
	bucket := bm.tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return nil, fmt.Errorf("bucket %s does not exist", bucketName)
	}
	return bboltBucket{bb: bucket}, nil
}

func (bm bboltBucketManager) WriteBucket(bucketName string) (Bucket, error) {
	bucket, err := bm.tx.CreateBucketIfNotExists([]byte(bucketName))
	if err != nil {
		return nil, fmt.Errorf("could not create bucket %s: %w", bucketName, err)
	}
	return bboltBucket{bb: bucket}, nil
}

// ---------------------------

type bboltDiskStore struct {
	bboltDB *bbolt.DB
}

func (ds bboltDiskStore) Path() string {
	return ds.bboltDB.Path()
}

func (ds bboltDiskStore) Read(f func(ReadOnlyBucketManager) error) error {
	return ds.bboltDB.View(func(tx *bbolt.Tx) error {
		bm := bboltBucketManager{tx: tx}
		return f(bm)
	})
}

func (ds bboltDiskStore) Write(f func(BucketManager) error) error {
	return ds.bboltDB.Update(func(tx *bbolt.Tx) error {
		bm := bboltBucketManager{tx: tx}
		return f(bm)
	})
}

func (ds bboltDiskStore) BackupToFile(path string) error {
	return ds.bboltDB.View(func(tx *bbolt.Tx) error {
		return tx.CopyFile(path, 0644)
	})
}

func (ds bboltDiskStore) SizeInBytes() (int64, error) {
	var size int64
	err := ds.bboltDB.View(func(tx *bbolt.Tx) error {
		size = tx.Size()
		return nil
	})
	return size, err
}

func (ds bboltDiskStore) Close() error {
	return ds.bboltDB.Close()
}
