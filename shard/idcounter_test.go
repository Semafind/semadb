package shard_test

import (
	"path/filepath"
	"testing"

	"github.com/semafind/semadb/shard"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func tempDB(t *testing.T) *bbolt.DB {
	dbpath := filepath.Join(t.TempDir(), "sharddb.bbolt")
	db, err := bbolt.Open(dbpath, 0666, nil)
	require.NoError(t, err)
	return db
}

func withCounter(t *testing.T, db *bbolt.DB, f func(*shard.IdCounter)) {
	if db == nil {
		db = tempDB(t)
	}
	db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("testing"))
		require.NoError(t, err)
		counter, err := shard.NewIdCounter(b, []byte("freeIds"), []byte("nextFreeId"))
		require.NoError(t, err)
		f(counter)
		return nil
	})
}

func TestCounterBlank(t *testing.T) {
	// ---------------------------
	withCounter(t, nil, func(counter *shard.IdCounter) {
		require.Equal(t, uint64(1), counter.NextId())
		require.Equal(t, uint64(2), counter.NextId())
	})
}

func TestCounterPersistance(t *testing.T) {
	// ---------------------------
	db := tempDB(t)
	withCounter(t, db, func(counter *shard.IdCounter) {
		require.Equal(t, uint64(1), counter.NextId())
		require.Equal(t, uint64(2), counter.NextId())
		require.NoError(t, counter.Flush())
	})
	// ---------------------------
	withCounter(t, db, func(counter *shard.IdCounter) {
		require.Equal(t, uint64(3), counter.NextId())
		require.Equal(t, uint64(4), counter.NextId())
	})
}

func TestCounterFreeing(t *testing.T) {
	// ---------------------------
	db := tempDB(t)
	withCounter(t, db, func(counter *shard.IdCounter) {
		require.Equal(t, uint64(1), counter.NextId())
		require.Equal(t, uint64(2), counter.NextId())
		counter.FreeId(1)
		require.NoError(t, counter.Flush())
	})
	// ---------------------------
	withCounter(t, db, func(counter *shard.IdCounter) {
		require.Equal(t, uint64(1), counter.NextId())
		require.Equal(t, uint64(3), counter.NextId())
	})
}
