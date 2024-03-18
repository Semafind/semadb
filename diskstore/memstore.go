package diskstore

import (
	"bytes"
	"fmt"
	"sync"
)

type memBucket struct {
	data       map[string][]byte
	isReadOnly bool
}

func NewMemBucket(isReadOnly bool) Bucket {
	return &memBucket{
		data:       make(map[string][]byte),
		isReadOnly: isReadOnly,
	}
}

func (b *memBucket) IsReadOnly() bool {
	return b.isReadOnly
}

func (b *memBucket) Get(k []byte) []byte {
	return b.data[string(k)]
}

func (b *memBucket) Put(k, v []byte) error {
	if b.isReadOnly {
		return fmt.Errorf("cannot put into read-only memory bucket")
	}
	b.data[string(k)] = v
	return nil
}

func (b *memBucket) ForEach(f func(k, v []byte) error) error {
	for k, v := range b.data {
		if err := f([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

func (b *memBucket) PrefixScan(prefix []byte, f func(k, v []byte) error) error {
	for k, v := range b.data {
		if len(k) < len(prefix) {
			continue
		}
		if k[:len(prefix)] == string(prefix) {
			if err := f([]byte(k), v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *memBucket) RangeScan(start, end []byte, f func(k, v []byte) error) error {
	for k, v := range b.data {
		if bytes.Compare([]byte(k), start) >= 0 && bytes.Compare([]byte(k), end) <= 0 {
			if err := f([]byte(k), v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *memBucket) Delete(k []byte) error {
	if b.isReadOnly {
		return fmt.Errorf("cannot delete in a read-only memory bucket")
	}
	delete(b.data, string(k))
	return nil
}

type memBucketManager struct {
	buckets    map[string]map[string][]byte
	isReadOnly bool
	mu         sync.Mutex
}

func (bm *memBucketManager) Get(bucketName string) (Bucket, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	b, ok := bm.buckets[bucketName]
	if !ok {
		b = make(map[string][]byte)
		bm.buckets[bucketName] = b
	}
	mb := &memBucket{
		data:       b,
		isReadOnly: bm.isReadOnly,
	}
	return mb, nil
}

type memDiskStore struct {
	buckets map[string]map[string][]byte
	// This lock is used to give a consistent view of the store such that Write
	// does not interleave with any Read.
	mu sync.RWMutex
}

func newMemDiskStore() *memDiskStore {
	return &memDiskStore{
		buckets: make(map[string]map[string][]byte),
	}
}

func (ds *memDiskStore) Path() string {
	return "memory"
}

func (ds *memDiskStore) Read(f func(BucketManager) error) error {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	bm := &memBucketManager{
		buckets:    ds.buckets,
		isReadOnly: true,
	}
	return f(bm)
}

func (ds *memDiskStore) Write(f func(BucketManager) error) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	bm := &memBucketManager{
		buckets:    ds.buckets,
		isReadOnly: false,
	}
	return f(bm)
}

func (ds *memDiskStore) BackupToFile(path string) error {
	return fmt.Errorf("not supported")
}

func (ds *memDiskStore) SizeInBytes() (int64, error) {
	return 0, nil
}

func (ds *memDiskStore) Close() error {
	clear(ds.buckets)
	return nil
}
