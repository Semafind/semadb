package diskstore_test

import (
	"path/filepath"
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/stretchr/testify/require"
)

func tempDiskStore(t *testing.T, path string, inMemory bool) diskstore.DiskStore {
	if inMemory {
		return diskstore.NewMemDiskStore()
	}
	if path == "" {
		path = filepath.Join(t.TempDir(), "test.db")
	}
	ds, err := diskstore.Open(path)
	require.NoError(t, err)
	return ds
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
