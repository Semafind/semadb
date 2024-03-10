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
			err := ds.Read(func(bm diskstore.ReadOnlyBucketManager) error {
				_, err := bm.ReadBucket("bucket")
				return err
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
			bname := "bucket"
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.WriteBucket(bname)
				require.NoError(t, err)
				return b.Put([]byte("wizard"), []byte("gandalf"))
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.ReadOnlyBucketManager) error {
				b, err := bm.ReadBucket(bname)
				require.NoError(t, err)
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
			bname := "bucket"
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.WriteBucket(bname)
				require.NoError(t, err)
				return b.Put([]byte("wizard"), []byte("gandalf"))
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.ReadOnlyBucketManager) error {
				b, err := bm.ReadBucket(bname)
				require.NoError(t, err)
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
			err := ds.Write(func(bm diskstore.BucketManager) error {
				bucket1, err := bm.WriteBucket("bucket1")
				require.NoError(t, err)
				bucket2, err := bm.WriteBucket("bucket2")
				require.NoError(t, err)
				require.NoError(t, bucket1.Put([]byte("wizard"), []byte("gandalf")))
				require.NoError(t, bucket2.Put([]byte("hobbit"), []byte("frodo")))
				return nil
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.ReadOnlyBucketManager) error {
				bucket1, err := bm.ReadBucket("bucket1")
				require.NoError(t, err)
				bucket2, err := bm.ReadBucket("bucket2")
				require.NoError(t, err)
				require.Equal(t, []byte("gandalf"), bucket1.Get([]byte("wizard")))
				require.Equal(t, []byte("frodo"), bucket2.Get([]byte("hobbit")))
				require.Nil(t, bucket1.Get([]byte("hobbit")))
				require.Nil(t, bucket2.Get([]byte("wizard")))
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
	err := ds.Write(func(bm diskstore.BucketManager) error {
		b, err := bm.WriteBucket("bucket")
		require.NoError(t, err)
		return b.Put([]byte("wizard"), []byte("gandalf"))
	})
	require.NoError(t, err)
	require.NoError(t, ds.Close())

	ds, err = diskstore.Open(path)
	require.NoError(t, err)
	err = ds.Read(func(bm diskstore.ReadOnlyBucketManager) error {
		b, err := bm.ReadBucket("bucket")
		require.NoError(t, err)
		require.Equal(t, []byte("gandalf"), b.Get([]byte("wizard")))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, ds.Close())
}

func Test_Backup(t *testing.T) {
	ds := tempDiskStore(t, "", false)
	err := ds.Write(func(bm diskstore.BucketManager) error {
		b, err := bm.WriteBucket("bucket")
		require.NoError(t, err)
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
	err = ds.Read(func(bm diskstore.ReadOnlyBucketManager) error {
		b, err := bm.ReadBucket("bucket")
		require.NoError(t, err)
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
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.WriteBucket("bucket")
				require.NoError(t, err)
				err = b.Put([]byte("wizard"), []byte("gandalf"))
				require.NoError(t, err)
				err = b.Put([]byte("hobbit"), []byte("frodo"))
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.ReadOnlyBucketManager) error {
				b, err := bm.ReadBucket("bucket")
				require.NoError(t, err)
				err = b.ForEach(func(k, v []byte) error {
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
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.WriteBucket("bucket")
				require.NoError(t, err)
				err = b.Put([]byte("wizard"), []byte("gandalf"))
				require.NoError(t, err)
				err = b.Put([]byte("hobbit"), []byte("frodo"))
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.ReadOnlyBucketManager) error {
				b, err := bm.ReadBucket("bucket")
				require.NoError(t, err)
				err = b.PrefixScan([]byte("w"), func(k, v []byte) error {
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
