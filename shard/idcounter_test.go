package shard

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func tempDB(t *testing.T) *bbolt.DB {
	dbpath := filepath.Join(t.TempDir(), "sharddb.bbolt")
	db, err := bbolt.Open(dbpath, 0666, nil)
	require.NoError(t, err)
	return db
}

func TestCounterBlank(t *testing.T) {
	// ---------------------------
	db := tempDB(t)
	db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("testing"))
		require.NoError(t, err)
		counter, err := NewIdCounter(b)
		require.NoError(t, err)
		require.Equal(t, uint64(1), counter.NextId())
		require.Equal(t, uint64(2), counter.NextId())
		return nil
	})
}

func TestCounterPersistance(t *testing.T) {
	// ---------------------------
	db := tempDB(t)
	db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("testing"))
		require.NoError(t, err)
		counter, err := NewIdCounter(b)
		require.NoError(t, err)
		require.Equal(t, uint64(1), counter.NextId())
		require.Equal(t, uint64(2), counter.NextId())
		require.NoError(t, counter.Flush())
		return nil
	})
	// ---------------------------
	db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("testing"))
		counter, err := NewIdCounter(b)
		require.NoError(t, err)
		require.Equal(t, uint64(3), counter.NextId())
		require.Equal(t, uint64(4), counter.NextId())
		return nil
	})
}

func TestCounterFreeing(t *testing.T) {
	// ---------------------------
	db := tempDB(t)
	db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("testing"))
		require.NoError(t, err)
		counter, err := NewIdCounter(b)
		require.NoError(t, err)
		require.Equal(t, uint64(1), counter.NextId())
		require.Equal(t, uint64(2), counter.NextId())
		counter.FreeId(1)
		require.NoError(t, counter.Flush())
		return nil
	})
	// ---------------------------
	db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("testing"))
		counter, err := NewIdCounter(b)
		require.NoError(t, err)
		require.Equal(t, uint64(1), counter.NextId())
		require.Equal(t, uint64(3), counter.NextId())
		return nil
	})
}
