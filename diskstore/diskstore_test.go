package diskstore_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/stretchr/testify/assert"
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
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.Read(func(bm diskstore.BucketManager) error {
				b, err := bm.Get("bucket")
				require.NoError(t, err)
				require.Nil(t, b.Get([]byte("wizard")))
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_BucketRecreation(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			bname := "bucket"
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.Get(bname)
				require.NoError(t, err)
				return b.Put([]byte("wizard"), []byte("gandalf"))
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.BucketManager) error {
				b, err := bm.Get(bname)
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
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			bname := "bucket"
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.Get(bname)
				require.NoError(t, err)
				return b.Put([]byte("wizard"), []byte("gandalf"))
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.BucketManager) error {
				b, err := bm.Get(bname)
				require.NoError(t, err)
				require.Equal(t, []byte("gandalf"), b.Get([]byte("wizard")))
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_NoWriteOnReadBucket(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			bname := "bucket"
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.Get(bname)
				require.NoError(t, err)
				require.False(t, b.IsReadOnly())
				return b.Put([]byte("wizard"), []byte("gandalf"))
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.BucketManager) error {
				b, err := bm.Get(bname)
				require.NoError(t, err)
				require.True(t, b.IsReadOnly())
				err = b.Put([]byte("hobbit"), []byte("frodo"))
				require.Error(t, err)
				err = b.Delete([]byte("wizard"))
				require.Error(t, err)
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_ConcurrentReadWrite(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			bname := "bucket"
			for i := 0; i < 100; i++ {
				err := ds.Write(func(bm diskstore.BucketManager) error {
					b, err := bm.Get(bname)
					require.NoError(t, err)
					istring := fmt.Sprintf("%d", i)
					return b.Put([]byte(istring), []byte(istring))
				})
				require.NoError(t, err)
			}
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				// Readers
				wg.Add(2)
				go func() {
					defer wg.Done()
					err := ds.Read(func(bm diskstore.BucketManager) error {
						b, err := bm.Get(bname)
						assert.NoError(t, err)
						for i := 0; i < 100; i++ {
							istring := fmt.Sprintf("%d", i)
							assert.Equal(t, []byte(istring), b.Get([]byte(istring)))
						}
						return nil
					})
					assert.NoError(t, err)
				}()
				// More writers
				go func() {
					defer wg.Done()
					for i := 100; i < 200; i++ {
						err := ds.Write(func(bm diskstore.BucketManager) error {
							b, err := bm.Get(bname)
							assert.NoError(t, err)
							istring := fmt.Sprintf("%d", i)
							return b.Put([]byte(istring), []byte(istring))
						})
						assert.NoError(t, err)
					}
				}()
			}
			wg.Wait()
			require.NoError(t, ds.Close())
		})
	}
}

func Test_ConcurrentReadInBucket(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.Get("bucket")
				require.NoError(t, err)
				for i := 0; i < 100; i++ {
					istring := fmt.Sprintf("%d", i)
					require.NoError(t, b.Put([]byte(istring), []byte(istring)))
				}
				var wg sync.WaitGroup
				for i := 0; i < 4; i++ {
					// Readers
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < 100; i++ {
							istring := fmt.Sprintf("%d", i)
							assert.Equal(t, []byte(istring), b.Get([]byte(istring)))
						}
					}()
				}
				wg.Wait()
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_ConcurrentWriteAcrossBuckets(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.Write(func(bm diskstore.BucketManager) error {
				var wg sync.WaitGroup
				// Concurrent writes
				for i := 0; i < 4; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						b, err := bm.Get(fmt.Sprintf("bucket%d", i))
						assert.NoError(t, err)
						for i := 0; i < 100; i++ {
							istring := fmt.Sprintf("%d", i)
							require.NoError(t, b.Put([]byte(istring), []byte(istring)))
						}
					}()
				}
				wg.Wait()
				for i := 0; i < 4; i++ {
					b, err := bm.Get(fmt.Sprintf("bucket%d", i))
					require.NoError(t, err)
					for i := 0; i < 100; i++ {
						istring := fmt.Sprintf("%d", i)
						require.Equal(t, []byte(istring), b.Get([]byte(istring)))
					}
				}
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, ds.Close())
		})
	}
}

func Test_WriteReadMultiple(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.Write(func(bm diskstore.BucketManager) error {
				bucket1, err := bm.Get("bucket1")
				require.NoError(t, err)
				bucket2, err := bm.Get("bucket2")
				require.NoError(t, err)
				require.NoError(t, bucket1.Put([]byte("wizard"), []byte("gandalf")))
				require.NoError(t, bucket2.Put([]byte("hobbit"), []byte("frodo")))
				return nil
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.BucketManager) error {
				bucket1, err := bm.Get("bucket1")
				require.NoError(t, err)
				bucket2, err := bm.Get("bucket2")
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
		b, err := bm.Get("bucket")
		require.NoError(t, err)
		return b.Put([]byte("wizard"), []byte("gandalf"))
	})
	require.NoError(t, err)
	require.NoError(t, ds.Close())

	ds, err = diskstore.Open(path)
	require.NoError(t, err)
	err = ds.Read(func(bm diskstore.BucketManager) error {
		b, err := bm.Get("bucket")
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
		b, err := bm.Get("bucket")
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
	err = ds.Read(func(bm diskstore.BucketManager) error {
		b, err := bm.Get("bucket")
		require.NoError(t, err)
		require.Equal(t, []byte("gandalf"), b.Get([]byte("wizard")))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, ds.Close())
}

func Test_ForEach(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.Get("bucket")
				require.NoError(t, err)
				err = b.Put([]byte("wizard"), []byte("gandalf"))
				require.NoError(t, err)
				err = b.Put([]byte("hobbit"), []byte("frodo"))
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.BucketManager) error {
				b, err := bm.Get("bucket")
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
		t.Run(fmt.Sprintf("inMemory=%v", inMemory), func(t *testing.T) {
			ds := tempDiskStore(t, "", inMemory)
			err := ds.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.Get("bucket")
				require.NoError(t, err)
				err = b.Put([]byte("wizard"), []byte("gandalf"))
				require.NoError(t, err)
				err = b.Put([]byte("hobbit"), []byte("frodo"))
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
			err = ds.Read(func(bm diskstore.BucketManager) error {
				b, err := bm.Get("bucket")
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
