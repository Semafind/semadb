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

type memDiskStore struct {
	buckets map[string]memBucket
}

func newMemDiskStore() *memDiskStore {
	return &memDiskStore{
		buckets: make(map[string]memBucket),
	}
}

func (ds *memDiskStore) Path() string {
	return "memory"
}

func (ds *memDiskStore) CreateBucketsIfNotExists(bucketNames []string) error {
	for _, name := range bucketNames {
		if _, ok := ds.buckets[name]; ok {
			continue
		}
		ds.buckets[name] = make(memBucket)
	}
	return nil
}

func (ds *memDiskStore) Read(bucketName string, f func(ReadOnlyBucket) error) error {
	b, ok := ds.buckets[bucketName]
	if !ok {
		return fmt.Errorf("bucket %s does not exist", bucketName)
	}
	return f(b)
}

func (ds *memDiskStore) Write(bucketName string, f func(Bucket) error) error {
	b, ok := ds.buckets[bucketName]
	if !ok {
		return fmt.Errorf("bucket %s does not exist", bucketName)
	}
	return f(b)
}

func (ds *memDiskStore) BackupToFile(path string) error {
	return fmt.Errorf("not supported")
}

func (ds *memDiskStore) Close() error {
	clear(ds.buckets)
	return nil
}
