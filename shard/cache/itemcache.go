package cache

import (
	"errors"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/diskstore"
)

var ErrNotFound = errors.New("not found")

/* Represents an item that store itself to a bucket. */
type Storable[K comparable, T any] interface {
	Cachable
	// Extract the id from the key, returns false if the key is not valid
	IdFromKey(key []byte) (K, bool)
	// Check if the item is dirty and clear the dirty flag, allows the Storable
	// item to determine when it's dirty
	CheckAndClearDirty() bool
	// Read the item from the bucket, the return value is inserted into the
	// cache, return a cache.ErrNotFound if the item is not found
	ReadFrom(id K, bucket diskstore.Bucket) (T, error)
	// Write the item to the bucket
	WriteTo(id K, bucket diskstore.Bucket) error
	// Delete the item from the bucket
	DeleteFrom(id K, bucket diskstore.Bucket) error
}

type itemCacheElem[K comparable, V Storable[K, V]] struct {
	value     V
	IsDirty   bool
	IsDeleted bool
}

/* The item cache is a simple in-memory cache that stores items of type T backed
 * by a disktore bucket. It is useful to avoid decoding and encoding items all
 * the time. For example, for vectors encoding and decoding can take time so
 * having a cache helps. */
type ItemCache[K comparable, V Storable[K, V]] struct {
	items        map[K]*itemCacheElem[K, V]
	itemsMu      sync.Mutex
	isAllInCache bool
	bucket       diskstore.Bucket
}

func NewItemCache[K comparable, V Storable[K, V]](bucket diskstore.Bucket) *ItemCache[K, V] {
	ic := &ItemCache[K, V]{
		items:  make(map[K]*itemCacheElem[K, V]),
		bucket: bucket,
	}
	return ic
}

// Approximate size in memory based on len(items) * size(item)
func (ic *ItemCache[K, T]) SizeInMemory() int64 {
	ic.itemsMu.Lock()
	defer ic.itemsMu.Unlock()
	for _, item := range ic.items {
		return int64(len(ic.items)) * item.value.SizeInMemory()
	}
	return 0
}

// Update the bucket of the cache, this is useful if this cache is shared across
// multiple transactions with different bucket handlers. For example, first
// transaction creates elements, second transaction uses the same bucket name
// but gets a different handler.
func (ic *ItemCache[K, T]) UpdateBucket(bucket diskstore.Bucket) {
	ic.bucket = bucket
}

func (ic *ItemCache[K, T]) read(id K) (T, error) {
	/* This dummy value is used to access the ReadFrom method. It allows a
	 * Storable to group all methods and the compiler to check that it follows
	 * the interface. Otherwise, it looks a big ugly, if there is a better to
	 * design the storable interface, we should explore it. */
	var dummyValue T
	value, err := dummyValue.ReadFrom(id, ic.bucket)
	if err != nil {
		return value, err
	}
	item := &itemCacheElem[K, T]{
		value: value,
	}
	ic.items[id] = item
	return item.value, nil
}

// Get an item from the cache, if it's not in the cache, it will be read from the
// bucket. Check for ErrNotFound to see if the item is not in the bucket.
func (ic *ItemCache[K, V]) Get(id K) (value V, err error) {
	ic.itemsMu.Lock()
	defer ic.itemsMu.Unlock()
	if item, ok := ic.items[id]; ok {
		if item.IsDeleted {
			err = ErrNotFound
			return
		}
		return item.value, nil
	}
	value, err = ic.read(id)
	return
}

// Attempts to get many items from the cache, if an item is not in the cache, it
// will be read from the bucket. Ignores NotFound errors, if an item is not found
// it will be skipped.
func (ic *ItemCache[K, V]) GetMany(ids ...K) ([]V, error) {
	ic.itemsMu.Lock()
	defer ic.itemsMu.Unlock()
	values := make([]V, 0, len(ids))
	for _, id := range ids {
		if item, ok := ic.items[id]; ok {
			if item.IsDeleted {
				continue
			}
			values = append(values, item.value)
			continue
		}
		if item, err := ic.read(id); err != nil && err != ErrNotFound {
			return nil, err
		} else if err == nil {
			values = append(values, item)
		}
	}
	return values, nil
}

func (ic *ItemCache[K, T]) Count() int {
	ic.itemsMu.Lock()
	defer ic.itemsMu.Unlock()
	bucketCount := 0
	err := ic.bucket.ForEach(func(key, value []byte) error {
		var dummyValue T
		id, ok := dummyValue.IdFromKey(key)
		if !ok {
			return nil
		}
		if _, ok := ic.items[id]; !ok {
			bucketCount++
		}
		return nil
	})
	if err != nil {
		log.Warn().Err(err).Msg("error counting item cache items in bucket")
		return 0
	}
	cacheCount := 0
	for _, item := range ic.items {
		if !item.IsDeleted {
			cacheCount++
		}
	}
	return cacheCount + bucketCount
}

// Put an item in the cache, it will be marked as dirty and written to the bucket
// on the next Flush.
func (ic *ItemCache[K, T]) Put(id K, item T) {
	ic.itemsMu.Lock()
	defer ic.itemsMu.Unlock()
	ic.items[id] = &itemCacheElem[K, T]{value: item, IsDirty: true}
}

// Delete an item from the cache, it will be marked as deleted and written to the
// bucket on the next Flush.
func (ic *ItemCache[K, T]) Delete(ids ...K) error {
	ic.itemsMu.Lock()
	defer ic.itemsMu.Unlock()
	for _, id := range ids {
		if elem, ok := ic.items[id]; ok {
			elem.IsDeleted = true
			continue
		}
		_, err := ic.read(id)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return err
		}
		ic.items[id].IsDeleted = true
	}
	return nil
}

// Iterate over all items in the cache, if the item is not in the cache, it will
// be read from the bucket. NOTE: Loads all items in memory.
func (ic *ItemCache[K, T]) ForEach(fn func(id K, item T) error) error {
	// ---------------------------
	// We have merge what's in the bucket with what's in the cache
	ic.itemsMu.Lock()
	defer ic.itemsMu.Unlock()
	// ---------------------------
	if !ic.isAllInCache {
		err := ic.bucket.ForEach(func(key, value []byte) error {
			var dummyValue T
			id, ok := dummyValue.IdFromKey(key)
			// Is this a valid key? It may be that a single item stores multiple key
			// values and wants to recover the original id from one key.
			if !ok {
				return nil
			}
			// Do we already have this item in cache?
			if _, ok := ic.items[id]; ok {
				return nil
			}
			// If not, let's read it from the bucket
			if _, err := ic.read(id); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		ic.isAllInCache = true
	}
	// ---------------------------
	for id, item := range ic.items {
		if item.IsDeleted {
			continue
		}
		if err := fn(id, item.value); err != nil {
			return err
		}
	}
	// ---------------------------
	return nil
}

// Flush all items in the cache to the bucket. If an item is marked as deleted,
// it will be deleted from the bucket. If an item is marked as dirty, it will be
// written to the bucket.
func (ic *ItemCache[K, T]) Flush() error {
	ic.itemsMu.Lock()
	defer ic.itemsMu.Unlock()
	for id, item := range ic.items {
		if item.IsDeleted {
			if err := item.value.DeleteFrom(id, ic.bucket); err != nil {
				return err
			}
			delete(ic.items, id)
			continue
		}
		if item.IsDirty || item.value.CheckAndClearDirty() {
			if err := item.value.WriteTo(id, ic.bucket); err != nil {
				return err
			}
			item.IsDirty = false
		}
	}
	return nil
}
