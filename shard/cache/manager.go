package cache

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"
)

// The cache manager allows us to reuse the same cache for multiple operations
// such as search and insert. It will try to keep the total cache sizes under
// the maxSize but during operation it may go over.
type Manager struct {
	// In bytes, set to -1 for unlimited, 0 for no shared caching
	maxSize int32
	stores  map[string]*sharedInMemStore
	mu      sync.Mutex
}

func NewManager(maxSize int32) *Manager {
	return &Manager{
		stores:  make(map[string]*sharedInMemStore),
		maxSize: maxSize,
	}
}

// Checks if the total size of the cache is over the limit and if so, it will
// scrap the least recently used cache.
func (m *Manager) checkAndPrune() {
	// Nothing to do if there is no limit
	if m.maxSize == -1 {
		return
	}
	// ---------------------------
	m.mu.Lock()
	defer m.mu.Unlock()
	// ---------------------------
	// Nothing can be stored in the cache if the limit is 0
	if m.maxSize == 0 {
		clear(m.stores)
		return
	}
	// ---------------------------
	type storeElem struct {
		name  string
		store *sharedInMemStore
		size  int32
	}
	stores := make([]storeElem, 0, len(m.stores))
	totalSize := int32(0)
	for n, s := range m.stores {
		ssize := s.estimatedSize.Load()
		stores = append(stores, storeElem{name: n, store: s, size: ssize})
		totalSize += ssize
	}
	// ---------------------------
	// Check if we need to prune stores
	if totalSize <= m.maxSize {
		return
	}
	// ---------------------------
	// Prune until we are under the limit
	slices.SortFunc(stores, func(a, b storeElem) int {
		return a.store.lastAccessed.Compare(b.store.lastAccessed)
	})
	for _, s := range stores {
		if totalSize <= m.maxSize {
			break
		}
		delete(m.stores, s.name)
		log.Debug().Str("name", s.name).Msg("Pruning store")
		totalSize -= s.size
	}
}

func (m *Manager) with(name string, bucket *bbolt.Bucket, readOnly bool, f func(c *PointCache) error) error {
	// ---------------------------
	if bucket.Writable() && readOnly {
		return fmt.Errorf("read only cache for %s cannot be used with a writable bucket", name)
	}
	// ---------------------------
	// We start with manager lock so others don't try to create the same store
	m.mu.Lock()
	if existingStore, ok := m.stores[name]; ok {
		existingStore.lastAccessed = time.Now()
		m.mu.Unlock()
		/* Bbolt allows multiple read transactions to be open at the same time
		 * but only a single write. For example, if there is an insert operation,
		 * we should still be able to search. Now, we need to make sure the cache
		 * business doesn't hinder this. If we naively just use RWMutex, and give
		 * insert a write lock and search a read lock, then the search has to
		 * wait for the insert to finish. Instead, if cannot get a lock, we can
		 * still proceed with a cold cache and serve the query. It just improves
		 * the cases where a cache exists and few concurrent requests coming in.
		 *
		 * Another concern is whether a store is scrapped or not. So suppose we
		 * acquire a read or a write lock, we need to check if the store is
		 * scrapped or not by the previous write owner.
		 */
		s := existingStore
		if readOnly {
			/* We are using TryRLock here because we can survive if we don't get
			 * the lock with a fresh cold cache. The idea is, if there is an
			 * available cache then use it, otherwise use a cold cache to keep
			 * running. */
			if existingStore.mu.TryRLock() {
				defer existingStore.mu.RUnlock()
			} else {
				// We couldn't get the lock, so we'll use a clean cold cache to
				// not block any ready only requests such as search and just
				// give a blank cache to operate on to keep the ball rolling. No
				// one else will benefit from this cache but it's better than
				// waiting.
				log.Debug().Str("name", name).Msg("Creating read only cold cache")
				s = newSharedInMemStore()
			}
		} else {
			/* We are going to write, so we'll wait for a write lock, as we do that
			 * the incoming search requests will start getting a cold cache because
			 * of the TryRLock above but still keep operating. This is better than
			 * blocking the search requests. If there is already a write operation
			 * like insert, update or delete, then we'll have to wait anyway because
			 * of bbolt (recall bbolt only allows one read-write transaction at a
			 * time) which is absolutely fine for a search heavy workload. */
			existingStore.mu.Lock()
			defer existingStore.mu.Unlock()
		}
		if s.scrapped {
			log.Debug().Str("name", name).Bool("readOnly", readOnly).Msg("Store is scrapped, using temporary new store")
			/* Cold temporary start, what has happened is although the store was
			 * in the manager, while we were waiting for the lock on it, it got
			 * scrapped and is not longer good to use. To keep things running,
			 * we opt to create a cold cache instead of waiting for another
			 * shared store. For example a search comes in while we are
			 * inserting points. */
			s = newSharedInMemStore()
			/* It is responsibility of the goroutine which scrapped the store to
			 * delete it from the manager. */
		}
		if s == existingStore {
			log.Debug().Str("name", name).Bool("readOnly", readOnly).Msg("Reusing store")
			defer m.checkAndPrune()
		}
		pc := newPointCache(bucket, s)
		if err := f(pc); err != nil {
			/* Something went wrong, we'll scrap the store and delete it from the
			 * manager. */
			s.scrapped = true
			m.mu.Lock()
			delete(m.stores, name)
			m.mu.Unlock()
			return fmt.Errorf("error while executing cache operation: %w", err)
		}
		return nil
	}
	log.Debug().Str("name", name).Bool("readOnly", readOnly).Msg("Creating new store")
	s := newSharedInMemStore()
	if m.maxSize != 0 {
		m.stores[name] = s
	}
	// We know the following locks will succeed because it is a new store.
	if readOnly {
		s.mu.RLock()
		defer s.mu.RUnlock()
	} else {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	// By unlocking after we have the store lock, we guarantee that the store
	// will not be scrapped by another goroutine.
	m.mu.Unlock()
	pc := newPointCache(bucket, s)
	if err := f(pc); err != nil {
		s.scrapped = true
		m.mu.Lock()
		delete(m.stores, name)
		m.mu.Unlock()
		return fmt.Errorf("error while executing on new cache operation: %w", err)
	}
	return nil
}

func (m *Manager) With(name string, bucket *bbolt.Bucket, f func(c ReadWriteCache) error) error {
	// ---------------------------
	return m.with(name, bucket, false, func(c *PointCache) error {
		return f(c)
	})
}

func (m *Manager) WithReadOnly(name string, bucket *bbolt.Bucket, f func(c ReadOnlyCache) error) error {
	// ---------------------------
	return m.with(name, bucket, true, func(c *PointCache) error {
		return f(c)
	})
}

func (m *Manager) Release(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.stores, name)
}
