package diskstore

import "fmt"

type MemBucket map[string][]byte

func (b MemBucket) Get(k []byte) []byte {
	return b[string(k)]
}

func (b MemBucket) Put(k, v []byte) error {
	b[string(k)] = v
	return nil
}

func (b MemBucket) ForEach(f func(k, v []byte) error) error {
	for k, v := range b {
		if err := f([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

func (b MemBucket) Delete(k []byte) error {
	delete(b, string(k))
	return nil
}

type MemDiskStore struct {
	buckets map[string]MemBucket
}

func NewMemDiskStore() *MemDiskStore {
	return &MemDiskStore{
		buckets: make(map[string]MemBucket),
	}
}

func (ds *MemDiskStore) Path() string {
	return "memory"
}

func (ds *MemDiskStore) CreateBucketsIfNotExists(bucketNames []string) error {
	for _, name := range bucketNames {
		if _, ok := ds.buckets[name]; ok {
			continue
		}
		ds.buckets[name] = make(MemBucket)
	}
	return nil
}

func (ds *MemDiskStore) Read(bucketName string, f func(ReadOnlyBucket) error) error {
	b, ok := ds.buckets[bucketName]
	if !ok {
		return fmt.Errorf("bucket %s does not exist", bucketName)
	}
	return f(b)
}

func (ds *MemDiskStore) Write(bucketName string, f func(Bucket) error) error {
	b, ok := ds.buckets[bucketName]
	if !ok {
		return fmt.Errorf("bucket %s does not exist", bucketName)
	}
	return f(b)
}

func (ds *MemDiskStore) BackupToFile(path string) error {
	return fmt.Errorf("not supported")
}

func (ds *MemDiskStore) Close() error {
	clear(ds.buckets)
	return nil
}
