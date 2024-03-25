package cache

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/diskstore"
)

// The cache manager allows us to reuse the same cache for multiple operations
// such as search and insert. It will try to keep the total cache sizes under
// the maxSize but during operation it may go over.
type Manager struct {
	// In bytes, set to -1 for unlimited, 0 for no shared caching
	maxSize      int64
	sharedCaches map[string]*sharedInMemCache
	mu           sync.Mutex
}

func NewManager(maxSize int64) *Manager {
	return &Manager{
		sharedCaches: make(map[string]*sharedInMemCache),
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
		name   string
		sCache *sharedInMemCache
		size   int64
	}
	caches := make([]cacheElem, 0, len(m.sharedCaches))
	totalSize := int64(0)
	for n, s := range m.sharedCaches {
		ssize := s.estimatedSize.Load()
		caches = append(caches, cacheElem{name: n, sCache: s, size: ssize})
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
		return a.sCache.lastAccessed.Compare(b.sCache.lastAccessed)
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
	writtenCaches map[string]*sharedInMemCache
	mu            sync.Mutex
	manager       *Manager
	failed        atomic.Bool
}

func (m *Manager) NewTransaction() *Transaction {
	return &Transaction{
		writtenCaches: make(map[string]*sharedInMemCache),
		manager:       m,
	}
}

func (t *Transaction) with(name string, readOnly bool, f func(cacheToUse *sharedInMemCache) error) error {
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
			if _, ok := t.writtenCaches[name]; !ok {
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
					cacheToUse = newSharedInMemCache()
				}
			}
			t.mu.Unlock()
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
			cacheToUse = newSharedInMemCache()
			/* It is responsibility of the goroutine which scrapped the cache to
			 * delete it from the manager which happens below. */
		}
		if cacheToUse == existingCache {
			log.Debug().Str("name", name).Bool("readOnly", readOnly).Msg("Reusing cache")
			defer t.manager.checkAndPrune()
		}
		if err := f(cacheToUse); err != nil {
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
	s := newSharedInMemCache()
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
	if err := f(s); err != nil {
		t.failed.Store(true)
		s.scrapped = true
		t.manager.mu.Lock()
		delete(t.manager.sharedCaches, name)
		t.manager.mu.Unlock()
		return fmt.Errorf("error while executing on new cache operation: %w", err)
	}
	return nil
}

func (t *Transaction) With(name string, graphBucket diskstore.Bucket, f func(c SharedPointCache) error) error {
	return t.with(name, false, func(cacheToUse *sharedInMemCache) error {
		pc := &pointCache{
			sharedCache: cacheToUse,
			graphBucket: graphBucket,
		}
		if err := f(pc); err != nil {
			return err
		}
		// Automatic flush here, the user doesn't need to call it.
		return pc.flush()
	})
}

func (t *Transaction) WithReadOnly(name string, graphBucket diskstore.Bucket, f func(c SharedPointCache) error) error {
	return t.with(name, true, func(cacheToUse *sharedInMemCache) error {
		pc := &pointCache{
			graphBucket: graphBucket,
			sharedCache: cacheToUse,
			isReadOnly:  true,
		}
		return f(pc)
	})
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
