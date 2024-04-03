package cache_test

import (
	"slices"
	"testing"

	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/shard/cache"
	"github.com/stretchr/testify/require"
)

type dummyStorable struct {
	id    uint64
	value uint64
}

func (d dummyStorable) IdFromKey(key []byte) (uint64, bool) {
	return conversion.BytesToUint64(key), len(key) == 8
}
func (d dummyStorable) CheckAndClearDirty() bool {
	return false
}

func (d dummyStorable) SizeInMemory() int64 {
	return 16
}

func (d dummyStorable) ReadFrom(id uint64, bucket diskstore.Bucket) (dummy dummyStorable, err error) {
	valueBytes := bucket.Get(conversion.Uint64ToBytes(id))
	if valueBytes == nil {
		err = cache.ErrNotFound
		return
	}
	dummy.id = id
	dummy.value = conversion.BytesToUint64(valueBytes)
	return
}
func (d dummyStorable) WriteTo(bucket diskstore.Bucket) error {
	return bucket.Put(conversion.Uint64ToBytes(d.id), conversion.Uint64ToBytes(d.value))
}
func (d dummyStorable) DeleteFrom(bucket diskstore.Bucket) error {
	return bucket.Delete(conversion.Uint64ToBytes(d.id))
}

func seedBucketWithDummy(t *testing.T, bucket diskstore.Bucket, items ...dummyStorable) {
	t.Helper()
	c := cache.NewItemCache[dummyStorable](bucket)
	for _, item := range items {
		require.NoError(t, c.Put(item.id, item))
	}
	require.NoError(t, c.Flush())
}

func TestItemCache_Get(t *testing.T) {
	// Empty cache triggers a read from the disk
	bucket := diskstore.NewMemBucket(false)
	dummy := dummyStorable{42, 42}
	dummy.WriteTo(bucket)
	c := cache.NewItemCache[dummyStorable](bucket)
	d, err := c.Get(42)
	require.NoError(t, err)
	require.EqualValues(t, 42, d.id)
	require.EqualValues(t, 42, d.value)
	_, err = c.Get(43)
	require.ErrorIs(t, err, cache.ErrNotFound)
}

func TestItemCache_Put(t *testing.T) {
	c := cache.NewItemCache[dummyStorable](diskstore.NewMemBucket(false))
	d := dummyStorable{43, 43}
	err := c.Put(43, d)
	require.NoError(t, err)
	d2, err := c.Get(43)
	require.NoError(t, err)
	require.EqualValues(t, d, d2)
	require.Equal(t, 1, c.Count())
}

func TestItemCache_Delete(t *testing.T) {
	bucket := diskstore.NewMemBucket(false)
	seedBucketWithDummy(t, bucket, dummyStorable{42, 42})
	c := cache.NewItemCache[dummyStorable](bucket)
	// Delete existing item in cache
	d2 := dummyStorable{43, 43}
	require.NoError(t, c.Put(43, d2))
	require.Equal(t, 2, c.Count())
	require.NoError(t, c.Delete(43))
	require.Equal(t, 1, c.Count())
	_, err := c.Get(43)
	require.ErrorIs(t, err, cache.ErrNotFound)
	// Delete non-existing item in cache, but exists in bucket
	require.NoError(t, c.Delete(42))
	require.Equal(t, 0, c.Count())
	_, err = c.Get(42)
	require.ErrorIs(t, err, cache.ErrNotFound)
	// Delete non-existing item in cache and bucket
	require.NoError(t, c.Delete(44))
	require.Equal(t, 0, c.Count())
	_, err = c.Get(44)
	require.ErrorIs(t, err, cache.ErrNotFound)
}

func TestItemCache_Flush(t *testing.T) {
	bucket := diskstore.NewMemBucket(false)
	seedBucketWithDummy(t, bucket, dummyStorable{42, 42})
	c := cache.NewItemCache[dummyStorable](bucket)
	d := dummyStorable{43, 43}
	require.NoError(t, c.Put(43, d))
	require.NoError(t, c.Delete(42))
	require.NoError(t, c.Flush())
	require.Equal(t, 1, c.Count())
	err := bucket.ForEach(func(key, value []byte) error {
		require.EqualValues(t, 43, conversion.BytesToUint64(key))
		require.EqualValues(t, 43, conversion.BytesToUint64(value))
		return nil
	})
	require.NoError(t, err)
}

func TestItemCache_ForEach(t *testing.T) {
	bucket := diskstore.NewMemBucket(false)
	c := cache.NewItemCache[dummyStorable](bucket)
	// Add some items
	err := c.Put(43, dummyStorable{43, 43})
	require.NoError(t, err)
	// Add and delete, should not show up
	err = c.Put(44, dummyStorable{44, 44})
	require.NoError(t, err)
	require.NoError(t, c.Delete(44))
	require.NoError(t, c.Flush())
	// Extra item in bucket, should show up
	d1 := dummyStorable{42, 42}
	d1.WriteTo(bucket)
	ids := make([]uint64, 0)
	err = c.ForEach(func(id uint64, item dummyStorable) error {
		ids = append(ids, id)
		return nil
	})
	require.NoError(t, err)
	slices.Sort(ids)
	require.EqualValues(t, []uint64{42, 43}, ids)
}
