package diskstore

import "fmt"

type memBucket map[string][]byte

func (b memBucket) Get(k []byte) []byte {
	return b[string(k)]
}

func (b memBucket) Put(k, v []byte) error {
	b[string(k)] = v
	return nil
}

func (b memBucket) ForEach(f func(k, v []byte) error) error {
	for k, v := range b {
		if err := f([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

func (b memBucket) PrefixScan(prefix []byte, f func(k, v []byte) error) error {
	for k, v := range b {
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

func (b memBucket) Delete(k []byte) error {
	delete(b, string(k))
	return nil
}

type bucketMap map[string]memBucket

func (bm bucketMap) ReadBucket(bucketName string) (ReadOnlyBucket, error) {
	b, ok := bm[bucketName]
	if !ok {
		return nil, fmt.Errorf("bucket %s does not exist", bucketName)
	}
	return b, nil
}

func (bm bucketMap) WriteBucket(bucketName string) (Bucket, error) {
	b, ok := bm[bucketName]
	if !ok {
		b = make(memBucket)
		bm[bucketName] = b
	}
	return b, nil
}

type memDiskStore struct {
	buckets bucketMap
}

func newMemDiskStore() *memDiskStore {
	return &memDiskStore{
		buckets: make(bucketMap),
	}
}

func (ds *memDiskStore) Path() string {
	return "memory"
}

func (ds *memDiskStore) Read(f func(ReadOnlyBucketManager) error) error {
	return f(ds.buckets)
}

func (ds *memDiskStore) Write(f func(BucketManager) error) error {
	return f(ds.buckets)
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
