package cache

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// Every item that is stored in the cache must implement this interface. It is
// used to determine the overall size of the cache.
type Cachable interface {
	SizeInMemory() int64
}

// A single stored item that wraps the size. It provides when it was accessed and a lock.
type sharedCacheElem struct {
	item         Cachable
	lastAccessed time.Time
	// ---------------------------
	// We currently allow a single writer to the cache at a time. This is because
	// within a single transaction there could be multiple goroutines writing to
	// the cache and invalidating the cache is difficult with multiple writers.
	mu sync.RWMutex
	// ---------------------------
	// Indicates if the cache is not consistent with the database and must be
	// discarded, occurs if a transaction goes wrong but was operating on the
	// cache. So, we get two options, either roll back stuff in the cache
	// (difficult to do) or scrap it (easy). We are going with the easy option.
	scrapped bool
}

// The cache manager allows us to reuse the same cache for multiple operations
// such as search and insert. It will try to keep the total cache sizes under
// the maxSize but during operation it may go over.
type Manager struct {
	// In bytes, set to -1 for unlimited, 0 for no shared caching
	maxSize      int64
	sharedCaches map[string]*sharedCacheElem
	mu           sync.Mutex
}

func NewManager(maxSize int64) *Manager {
	return &Manager{
		sharedCaches: make(map[string]*sharedCacheElem),
		maxSize:      maxSize,
	}
}

func (m *Manager) Release(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sharedCaches, name)
	log.Debug().Str("name", name).Int("numCaches", len(m.sharedCaches)).Msg("Released cache")
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
		clear(m.sharedCaches)
		return
	}
	// ---------------------------
	type cacheElem struct {
		name         string
		lastAccessed time.Time
		size         int64
	}
	caches := make([]cacheElem, 0, len(m.sharedCaches))
	totalSize := int64(0)
	for n, s := range m.sharedCaches {
		ssize := s.item.SizeInMemory()
		caches = append(caches, cacheElem{name: n, size: ssize, lastAccessed: s.lastAccessed})
		totalSize += ssize
	}
	// ---------------------------
	// Check if we need to prune stores
	if totalSize <= m.maxSize {
		return
	}
	// ---------------------------
	// Prune until we are under the limit
	slices.SortFunc(caches, func(a, b cacheElem) int {
		return a.lastAccessed.Compare(b.lastAccessed)
	})
	for _, s := range caches {
		if totalSize <= m.maxSize {
			break
		}
		delete(m.sharedCaches, s.name)
		log.Debug().Str("name", s.name).Msg("Pruning cache")
		totalSize -= s.size
	}
}

type Transaction struct {
	writtenCaches map[string]*sharedCacheElem
	mu            sync.Mutex
	manager       *Manager
	failed        atomic.Bool
}

func (m *Manager) NewTransaction() *Transaction {
	return &Transaction{
		writtenCaches: make(map[string]*sharedCacheElem),
		manager:       m,
	}
}

func (t *Transaction) With(name string, readOnly bool, createFn func() (Cachable, error), f func(cacheToUse Cachable) error) error {
	if t.failed.Load() {
		return fmt.Errorf("transaction has already failed")
	}
	// ---------------------------
	/* The aim of the game is to find an appropiate value for cacheToUse variable
	 * with common enemies including concurrent read-writes to maps and scrapped
	 * caches. */
	// ---------------------------
	// We start with manager lock so others don't try to create the same cache
	t.manager.mu.Lock()
	if existingCache, ok := t.manager.sharedCaches[name]; ok {
		existingCache.lastAccessed = time.Now()
		t.manager.mu.Unlock()
		/* Bbolt allows multiple read transactions to be open at the same time
		 * but only a single write. For example, if there is an insert operation,
		 * we should still be able to search. Now, we need to make sure the cache
		 * business doesn't hinder this. If we naively just use RWMutex, and give
		 * insert a write lock and search a read lock, then the search has to
		 * wait for the insert to finish. Instead, if cannot get a lock, we can
		 * still proceed with a cold cache and serve the query. It just improves
		 * the cases where a cache exists and few concurrent requests coming in.
		 *
		 * Another concern is whether a cache is scrapped or not. So suppose we
		 * acquire a read or a write lock, we need to check if the cache is
		 * scrapped or not by the previous write owner.
		 */
		cacheToUse := existingCache
		if readOnly {
			// Do we already have a write lock on this cache? If we do we can
			// let other go routines on the same transaction to concurrently
			// read from it whilst one go routine is writing.
			t.mu.Lock()
			_, ok := t.writtenCaches[name]
			t.mu.Unlock()
			if !ok {
				/* We are using TryRLock here because we can survive if we don't get
				* the lock with a fresh cold cache. The idea is, if there is an
				* available cache then use it, otherwise use a cold cache to keep
				* running. */
				if existingCache.mu.TryRLock() {
					defer existingCache.mu.RUnlock()
				} else {
					// We couldn't get the lock, so we'll use a clean cold cache to
					// not block any ready only requests such as search and just
					// give a blank cache to operate on to keep the ball rolling. No
					// one else will benefit from this cache but it's better than
					// waiting.
					log.Debug().Str("name", name).Msg("Creating read only cold cache")
					freshCachable, err := createFn()
					if err != nil {
						t.failed.Store(true)
						return fmt.Errorf("error while creating fresh read only cold cache: %w", err)
					}
					cacheToUse = &sharedCacheElem{
						item:         freshCachable,
						lastAccessed: time.Now(),
					}
				}
			}
		} else {
			/* We are going to write, so we'll wait for a write lock, as we do that
			 * the incoming search requests will start getting a cold cache because
			 * of the TryRLock above but still keep operating. This is better than
			 * blocking the search requests. If there is already a write operation
			 * like insert, update or delete, then we'll have to wait anyway because
			 * of bbolt (recall bbolt only allows one read-write transaction at a
			 * time) which is absolutely fine for a search heavy workload. */
			t.mu.Lock()
			/* Have we locked this cache before? Within a transaction we hold
			 * onto writes until we know the transaction is committed. This is
			 * to ensure other readers or writers do not see partial results.
			 * Within a transaction a writer can write to multiple caches, e.g.
			 * multiple indices. */
			if _, ok := t.writtenCaches[name]; !ok {
				/****************************
				 * Please do not forget to unlock after the transaction is
				 * complete.
				 ***************************/
				existingCache.mu.Lock()
				t.writtenCaches[name] = existingCache
			}
			t.mu.Unlock()
		}
		if cacheToUse.scrapped {
			log.Debug().Str("name", name).Bool("readOnly", readOnly).Msg("Cache is scrapped, using temporary new cache")
			/* Cold temporary start, what has happened is although the cache was
			 * in the manager, while we were waiting for the lock on it, it got
			 * scrapped and is not longer good to use. To keep things running, we
			 * opt to create a cold cache instead of waiting for another shared
			 * cache. For example a search comes in while we are inserting
			 * points. */
			freshCachable, err := createFn()
			if err != nil {
				t.failed.Store(true)
				return fmt.Errorf("error while creating fresh cold temporary cache: %w", err)
			}
			cacheToUse = &sharedCacheElem{
				item:         freshCachable,
				lastAccessed: time.Now(),
			}
			/* It is responsibility of the goroutine which scrapped the cache to
			 * delete it from the manager which happens below. */
		}
		if cacheToUse == existingCache {
			log.Debug().Str("name", name).Bool("readOnly", readOnly).Msg("Reusing cache")
			defer t.manager.checkAndPrune()
		}
		if err := f(cacheToUse.item); err != nil {
			/* Something went wrong, we'll scrap the cache and delete it from the
			 * manager. */
			t.failed.Store(true)
			cacheToUse.scrapped = true
			t.manager.mu.Lock()
			delete(t.manager.sharedCaches, name)
			t.manager.mu.Unlock()
			return fmt.Errorf("error while executing cache operation: %w", err)
		}
		return nil
	}
	log.Debug().Str("name", name).Bool("readOnly", readOnly).Msg("Creating new cache")
	freshCachable, err := createFn()
	if err != nil {
		t.failed.Store(true)
		t.manager.mu.Unlock()
		return fmt.Errorf("error while creating fresh cache: %w", err)
	}
	s := &sharedCacheElem{
		item:         freshCachable,
		lastAccessed: time.Now(),
	}
	if t.manager.maxSize != 0 {
		t.manager.sharedCaches[name] = s
		defer t.manager.checkAndPrune()
	}
	// We know the following locks will succeed because it is a new cache.
	if readOnly {
		s.mu.RLock()
		defer s.mu.RUnlock()
	} else {
		// The following shared cache lock is released when the transaction is done.
		s.mu.Lock()
		t.mu.Lock()
		t.writtenCaches[name] = s
		t.mu.Unlock()
		// defer s.mu.Unlock()
	}
	// By unlocking after we have the cache lock, we guarantee that the cache
	// will not be scrapped by another goroutine.
	t.manager.mu.Unlock()
	if err := f(s.item); err != nil {
		t.failed.Store(true)
		s.scrapped = true
		t.manager.mu.Lock()
		delete(t.manager.sharedCaches, name)
		t.manager.mu.Unlock()
		return fmt.Errorf("error while executing on new cache operation: %w", err)
	}
	return nil
}

// Releases all the locks on the caches. Must be called after the transaction.
func (t *Transaction) Commit(fail bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.writtenCaches) == 0 {
		return
	}
	t.manager.mu.Lock()
	defer t.manager.mu.Unlock()
	failed := t.failed.Load() || fail
	for name, s := range t.writtenCaches {
		if failed {
			s.scrapped = true
			delete(t.manager.sharedCaches, name)
		}
		log.Debug().Str("name", name).Bool("failed", failed).Msg("Committing cache")
		// Recall that we should be holding all the write locks to these caches within the transaction
		s.mu.Unlock()
	}
}
