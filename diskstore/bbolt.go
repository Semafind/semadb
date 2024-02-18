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

type BBoltDiskStore struct {
	bboltDB *bbolt.DB
}

func (ds BBoltDiskStore) Path() string {
	return ds.bboltDB.Path()
}

func (ds BBoltDiskStore) CreateBucketsIfNotExists(bucketName []string) error {
	return ds.bboltDB.Update(func(tx *bbolt.Tx) error {
		for _, name := range bucketName {
			_, err := tx.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (ds BBoltDiskStore) Read(bucketName string, f func(ReadOnlyBucket) error) error {
	return ds.bboltDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %s does not exist", bucketName)
		}
		return f(bboltBucket{bb: b})
	})
}

func (ds BBoltDiskStore) Write(bucketName string, f func(Bucket) error) error {
	return ds.bboltDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %s does not exist", bucketName)
		}
		return f(bboltBucket{bb: b})
	})
}

func (ds BBoltDiskStore) BackupToFile(path string) error {
	return ds.bboltDB.View(func(tx *bbolt.Tx) error {
		return tx.CopyFile(path, 0644)
	})
}

func (ds BBoltDiskStore) Close() error {
	return ds.bboltDB.Close()
}
