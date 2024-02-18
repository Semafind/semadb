package diskstore

import (
	"fmt"

	"go.etcd.io/bbolt"
)

type BBoltDiskStore struct {
	bboltDB *bbolt.DB
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
		return f(b)
	})
}

func (ds BBoltDiskStore) Write(bucketName string, f func(Bucket) error) error {
	return ds.bboltDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %s does not exist", bucketName)
		}
		return f(b)
	})
}

func (ds BBoltDiskStore) Close() error {
	return ds.bboltDB.Close()
}
