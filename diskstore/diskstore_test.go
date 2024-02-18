package diskstore_test

import (
	"path/filepath"
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/stretchr/testify/require"
)

func tempDiskStore(t *testing.T, path string, inMemory bool) diskstore.DiskStore {
	if inMemory {
		path = ""
	} else if path == "" {
		path = filepath.Join(t.TempDir(), "test.db")
	}
	ds, err := diskstore.Open(path)
	require.NoError(t, err)
	return ds
}

func Test_Path(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	ds := tempDiskStore(t, path, false)
	require.Equal(t, path, ds.Path())
	require.NoError(t, ds.Close())
}

func Test_NoBuckets(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run("inMemory", func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.Read("bucket", func(b diskstore.ReadOnlyBucket) error {
				require.FailNow(t, "should not be called")
				return nil
			})
			require.Error(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_BucketRecreation(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run("inMemory", func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.CreateBucketsIfNotExists([]string{"bucket"})
			require.NoError(t, err)
			err = ds.Write("bucket", func(b diskstore.Bucket) error {
				return b.Put([]byte("wizard"), []byte("gandalf"))
			})
			require.NoError(t, err)
			err = ds.CreateBucketsIfNotExists([]string{"bucket"})
			require.NoError(t, err)
			err = ds.Read("bucket", func(b diskstore.ReadOnlyBucket) error {
				require.Equal(t, []byte("gandalf"), b.Get([]byte("wizard")))
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_ReadWriteRead(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run("inMemory", func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.CreateBucketsIfNotExists([]string{"bucket"})
			require.NoError(t, err)
			err = ds.Write("bucket", func(b diskstore.Bucket) error {
				return b.Put([]byte("wizard"), []byte("gandalf"))
			})
			require.NoError(t, err)
			err = ds.Read("bucket", func(b diskstore.ReadOnlyBucket) error {
				require.Equal(t, []byte("gandalf"), b.Get([]byte("wizard")))
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_WriteReadMultiple(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run("inMemory", func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.CreateBucketsIfNotExists([]string{"bucket1", "bucket2"})
			require.NoError(t, err)
			err = ds.WriteMultiple([]string{"bucket1", "bucket2"}, func(buckets []diskstore.Bucket) error {
				require.Len(t, buckets, 2)
				require.NoError(t, buckets[0].Put([]byte("wizard"), []byte("gandalf")))
				require.NoError(t, buckets[1].Put([]byte("hobbit"), []byte("frodo")))
				return nil
			})
			require.NoError(t, err)
			err = ds.ReadMultiple([]string{"bucket1", "bucket2"}, func(buckets []diskstore.ReadOnlyBucket) error {
				require.Len(t, buckets, 2)
				require.Equal(t, []byte("gandalf"), buckets[0].Get([]byte("wizard")))
				require.Equal(t, []byte("frodo"), buckets[1].Get([]byte("hobbit")))
				require.Nil(t, buckets[0].Get([]byte("hobbit")))
				require.Nil(t, buckets[1].Get([]byte("wizard")))
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_Persistance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	ds := tempDiskStore(t, path, false)
	err := ds.CreateBucketsIfNotExists([]string{"bucket"})
	require.NoError(t, err)
	err = ds.Write("bucket", func(b diskstore.Bucket) error {
		return b.Put([]byte("wizard"), []byte("gandalf"))
	})
	require.NoError(t, err)
	require.NoError(t, ds.Close())

	ds, err = diskstore.Open(path)
	require.NoError(t, err)
	err = ds.Read("bucket", func(b diskstore.ReadOnlyBucket) error {
		require.Equal(t, []byte("gandalf"), b.Get([]byte("wizard")))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, ds.Close())
}

func Test_Backup(t *testing.T) {
	ds := tempDiskStore(t, "", false)
	err := ds.CreateBucketsIfNotExists([]string{"bucket"})
	require.NoError(t, err)
	err = ds.Write("bucket", func(b diskstore.Bucket) error {
		return b.Put([]byte("wizard"), []byte("gandalf"))
	})
	require.NoError(t, err)
	// ---------------------------
	backupFilePath := filepath.Join(t.TempDir(), "backup.db")
	err = ds.BackupToFile(backupFilePath)
	require.NoError(t, err)
	require.NoError(t, ds.Close())
	// ---------------------------
	ds, err = diskstore.Open(backupFilePath)
	require.NoError(t, err)
	err = ds.Read("bucket", func(b diskstore.ReadOnlyBucket) error {
		require.Equal(t, []byte("gandalf"), b.Get([]byte("wizard")))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, ds.Close())
}

func Test_ForEach(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run("inMemory", func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.CreateBucketsIfNotExists([]string{"bucket"})
			require.NoError(t, err)
			err = ds.Write("bucket", func(b diskstore.Bucket) error {
				err := b.Put([]byte("wizard"), []byte("gandalf"))
				require.NoError(t, err)
				err = b.Put([]byte("hobbit"), []byte("frodo"))
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
			err = ds.Read("bucket", func(b diskstore.ReadOnlyBucket) error {
				err := b.ForEach(func(k, v []byte) error {
					switch string(k) {
					case "wizard":
						require.Equal(t, []byte("gandalf"), v)
					case "hobbit":
						require.Equal(t, []byte("frodo"), v)
					default:
						require.FailNowf(t, "unexpected key", "key: %s", k)
					}
					return nil
				})
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_PrefixScan(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run("inMemory", func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.CreateBucketsIfNotExists([]string{"bucket"})
			require.NoError(t, err)
			err = ds.Write("bucket", func(b diskstore.Bucket) error {
				err := b.Put([]byte("wizard"), []byte("gandalf"))
				require.NoError(t, err)
				err = b.Put([]byte("hobbit"), []byte("frodo"))
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
			err = ds.Read("bucket", func(b diskstore.ReadOnlyBucket) error {
				err := b.PrefixScan([]byte("w"), func(k, v []byte) error {
					require.Equal(t, []byte("wizard"), k)
					require.Equal(t, []byte("gandalf"), v)
					return nil
				})
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}