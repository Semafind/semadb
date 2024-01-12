package cache

import (
	"path/filepath"
	"testing"

	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func randCachePoints(size int) []*CachePoint {
	ps := make([]*CachePoint, size)
	for i := 0; i < size; i++ {
		ps[i] = &CachePoint{
			ShardPoint: ShardPoint{
				NodeId: uint64(i + 1),
				Point: models.Point{
					Vector:   []float32{1, 2, 3},
					Metadata: []byte("metadata"),
				},
			},
		}
	}
	return ps
}

func withTempBucket(t *testing.T, readOnly bool, f func(b *bbolt.Bucket)) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	defer db.Close()
	if readOnly {
		err = db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("test"))
			f(b)
			return nil
		})
	} else {
		err = db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucket([]byte("test"))
			if err != nil {
				return err
			}
			f(b)
			return nil
		})
	}
	require.NoError(t, err)
}

func TestPointCacheFlush(t *testing.T) {
	ps := randCachePoints(10)
	store := newSharedInMemStore()
	withTempBucket(t, false, func(b *bbolt.Bucket) {
		pc := newPointCache(b, store)
		for _, p := range ps {
			_, err := pc.SetPoint(p.ShardPoint)
			require.NoError(t, err)
		}
		require.Greater(t, store.estimatedSize.Load(), int32(0))
		require.NoError(t, pc.Flush())
	})
	require.Equal(t, 10, len(ps))
	for _, p := range ps {
		require.Equal(t, p.isDirty, false)
	}
}
