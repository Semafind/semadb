package diskstore

import (
	"bytes"
	"fmt"
	"sync"

	"go.etcd.io/bbolt"
)

type bboltBucket struct {
	bb *bbolt.Bucket
}

func (b bboltBucket) IsReadOnly() bool {
	return !b.bb.Writable()
}

func (b bboltBucket) Get(k []byte) []byte {
	// Not huge fan of this b.bb business but it's explicit.
	return b.bb.Get(k)
}

func (b bboltBucket) Put(k, v []byte) error {
	// We don't check for read-only here because bbolt will return an error if
	// the bucket is not writable already.
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

func (b bboltBucket) RangeScan(start, end []byte, inclusive bool, f func(k, v []byte) error) error {
	c := b.bb.Cursor()
	// ---------------------------
	var k, v []byte
	if start == nil {
		k, v = c.First()
	} else {
		k, v = c.Seek(start)
		if !inclusive && bytes.Equal(k, start) {
			k, v = c.Next()
		}
	}
	// ---------------------------
	for ; k != nil; k, v = c.Next() {
		// ---------------------------
		if end != nil {
			if inclusive {
				if bytes.Compare(k, end) > 0 {
					break
				}
			} else {
				if bytes.Compare(k, end) >= 0 {
					break
				}
			}
		}
		// ---------------------------
		if err := f(k, v); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------

type bboltBucketManager struct {
	tx         *bbolt.Tx
	isReadOnly bool
	// bbolt objects within a transaction are not thread safe but we want
	// multiple go routines to potentially create buckets
	mu sync.Mutex
}

func (bm *bboltBucketManager) Get(bucketName string) (Bucket, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if bm.isReadOnly {
		bucket := bm.tx.Bucket([]byte(bucketName))
		if bucket == nil {
			// We changed to returning an empty bucket to mirror the write case.
			// That is, when writing we automatically create a bucket, when reading
			// we automatically return an empty bucket.
			return emptyReadOnlyBucket{}, nil
			// return nil, fmt.Errorf("bucket %s does not exist", bucketName)
		}
		return bboltBucket{bb: bucket}, nil
	}
	// This potentially modifies the b+ tree, so the lock is necessary avoid
	// race condition on the tx which is not thread safe.
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

func (ds bboltDiskStore) Read(f func(BucketManager) error) error {
	return ds.bboltDB.View(func(tx *bbolt.Tx) error {
		bm := &bboltBucketManager{tx: tx, isReadOnly: true}
		return f(bm)
	})
}

func (ds bboltDiskStore) Write(f func(BucketManager) error) error {
	return ds.bboltDB.Update(func(tx *bbolt.Tx) error {
		bm := &bboltBucketManager{tx: tx}
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
