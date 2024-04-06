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
	dummy.value = conversion.BytesToUint64(valueBytes)
	return
}
func (d dummyStorable) WriteTo(id uint64, bucket diskstore.Bucket) error {
	return bucket.Put(conversion.Uint64ToBytes(id), conversion.Uint64ToBytes(d.value))
}
func (d dummyStorable) DeleteFrom(id uint64, bucket diskstore.Bucket) error {
	return bucket.Delete(conversion.Uint64ToBytes(id))
}

func seedBucketWithDummy(t *testing.T, bucket diskstore.Bucket, items ...dummyStorable) {
	t.Helper()
	c := cache.NewItemCache[uint64, dummyStorable](bucket)
	for i, item := range items {
		require.NoError(t, c.Put(uint64(i), item))
	}
	require.NoError(t, c.Flush())
}

func TestItemCache_Get(t *testing.T) {
	// Empty cache triggers a read from the disk
	bucket := diskstore.NewMemBucket(false)
	dummy := dummyStorable{42}
	dummy.WriteTo(42, bucket)
	c := cache.NewItemCache[uint64, dummyStorable](bucket)
	d, err := c.Get(42)
	require.NoError(t, err)
	require.EqualValues(t, 42, d.value)
	_, err = c.Get(43)
	require.ErrorIs(t, err, cache.ErrNotFound)
}

func TestItemCache_GetMany(t *testing.T) {
	// Empty cache triggers a read from the disk
	bucket := diskstore.NewMemBucket(false)
	dummy := dummyStorable{42}
	dummy.WriteTo(42, bucket)
	c := cache.NewItemCache[uint64, dummyStorable](bucket)
	c.Put(43, dummyStorable{43})
	c.Put(44, dummyStorable{44})
	c.Delete(44)
	ds, err := c.GetMany(42, 43, 44, 45)
	require.NoError(t, err)
	require.Len(t, ds, 2)
	require.EqualValues(t, 42, ds[0].value)
	require.EqualValues(t, 43, ds[1].value)
}

func TestItemCache_Put(t *testing.T) {
	c := cache.NewItemCache[uint64, dummyStorable](diskstore.NewMemBucket(false))
	d := dummyStorable{43}
	err := c.Put(43, d)
	require.NoError(t, err)
	d2, err := c.Get(43)
	require.NoError(t, err)
	require.EqualValues(t, d, d2)
	require.Equal(t, 1, c.Count())
}

func TestItemCache_Delete(t *testing.T) {
	bucket := diskstore.NewMemBucket(false)
	seedBucketWithDummy(t, bucket, dummyStorable{42})
	c := cache.NewItemCache[uint64, dummyStorable](bucket)
	// Delete existing item in cache
	d2 := dummyStorable{43}
	require.NoError(t, c.Put(43, d2))
	require.Equal(t, 2, c.Count())
	require.NoError(t, c.Delete(43))
	require.Equal(t, 1, c.Count())
	_, err := c.Get(43)
	require.ErrorIs(t, err, cache.ErrNotFound)
	// Delete non-existing item in cache, but exists in bucket
	require.NoError(t, c.Delete(0))
	require.Equal(t, 0, c.Count())
	_, err = c.Get(0)
	require.ErrorIs(t, err, cache.ErrNotFound)
	// Delete non-existing item in cache and bucket
	require.NoError(t, c.Delete(44))
	require.Equal(t, 0, c.Count())
	_, err = c.Get(44)
	require.ErrorIs(t, err, cache.ErrNotFound)
}

func TestItemCache_Flush(t *testing.T) {
	bucket := diskstore.NewMemBucket(false)
	seedBucketWithDummy(t, bucket, dummyStorable{42})
	c := cache.NewItemCache[uint64, dummyStorable](bucket)
	d := dummyStorable{43}
	require.NoError(t, c.Put(43, d))
	require.NoError(t, c.Delete(0))
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
	c := cache.NewItemCache[uint64, dummyStorable](bucket)
	// Add some items
	err := c.Put(43, dummyStorable{43})
	require.NoError(t, err)
	// Add and delete, should not show up
	err = c.Put(44, dummyStorable{44})
	require.NoError(t, err)
	require.NoError(t, c.Delete(44))
	require.NoError(t, c.Flush())
	// Extra item in bucket, should show up
	d1 := dummyStorable{42}
	d1.WriteTo(42, bucket)
	ids := make([]uint64, 0)
	err = c.ForEach(func(id uint64, item dummyStorable) error {
		ids = append(ids, id)
		return nil
	})
	require.NoError(t, err)
	slices.Sort(ids)
	require.EqualValues(t, []uint64{42, 43}, ids)
}
