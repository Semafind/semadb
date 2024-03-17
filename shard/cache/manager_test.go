package cache

import (
	"errors"
	"sync"
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------

func TestManager_Prune(t *testing.T) {
	t.Run("No space", func(t *testing.T) {
		m := NewManager(1)
		tx := m.NewTransaction()
		// We'll create some points and check if the shared cache is pruned correctly
		err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
			randPoints := randCachePoints(10)
			for _, p := range randPoints {
				c.SetPoint(p.GraphNode)
			}
			return nil
		})
		require.NoError(t, err)
		require.Len(t, m.sharedCaches, 0)
	})
	t.Run("With space", func(t *testing.T) {
		m := NewManager(1000000)
		tx := m.NewTransaction()
		// We'll create some points and check if the shared cache is pruned correctly
		err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
			randPoints := randCachePoints(10)
			for _, p := range randPoints {
				c.SetPoint(p.GraphNode)
			}
			return nil
		})
		require.NoError(t, err)
		require.Len(t, m.sharedCaches, 1)
	})
}

func TestManager_Release(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	// We'll create some points and check if the shared cache is pruned correctly
	err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
		// The cache should continue to exist during operation even if it is
		// release, i.e. the point is valid but not shared caches map anymore.
		m.Release("test")
		randPoints := randCachePoints(10)
		for _, p := range randPoints {
			c.SetPoint(p.GraphNode)
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, m.sharedCaches, 0)
	m.Release("test")
	require.Len(t, m.sharedCaches, 0)
}

func TestManager_CacheReuse(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	randPoints := randCachePoints(10)
	// We'll create some points first
	err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
		for _, p := range randPoints {
			c.SetPoint(p.GraphNode)
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, m.sharedCaches, 1)
	// By giving a new blank bucket, we are checking if the GetPoint never hits
	// the disk
	err = tx.WithReadOnly("test", diskstore.NewMemBucket(true), func(c ReadOnlyCache) error {
		for _, p := range randPoints {
			_, err := c.GetPoint(p.NodeId)
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, m.sharedCaches, 1)
}

func TestManager_SharedReadWhileWrite(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	randPoints := randCachePoints(10)
	err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
		for _, p := range randPoints {
			c.SetPoint(p.GraphNode)
		}
		// While we are writing, let's access it as a read-only cache
		err := tx.WithReadOnly("test", diskstore.NewMemBucket(true), func(c ReadOnlyCache) error {
			_, err := c.GetPoint(randPoints[0].NodeId)
			// We shouldn't be able to see the writes yet
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)
		// But from another transaction, we should be able to see the writes
		tx2 := m.NewTransaction()
		err = tx2.WithReadOnly("test", diskstore.NewMemBucket(true), func(c ReadOnlyCache) error {
			for _, p := range randPoints {
				_, err := c.GetPoint(p.NodeId)
				require.Error(t, err)
			}
			return nil
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
}

func TestManager_ScrappedCache(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	oops := errors.New("oops")
	// First write fails
	err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
		randPoints := randCachePoints(10)
		for _, p := range randPoints {
			c.SetPoint(p.GraphNode)
		}
		return oops
	})
	assert.True(t, errors.Is(err, oops))
	// We shouldn't see the writes at all
	err = tx.WithReadOnly("test", diskstore.NewMemBucket(true), func(c ReadOnlyCache) error {
		assert.Fail(t, "shouldn't be here")
		return nil
	})
	assert.Error(t, err)
	require.Len(t, m.sharedCaches, 0)
}

func Test_TransactionCommit(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	randPoints := randCachePoints(10)
	err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
		for _, p := range randPoints {
			c.SetPoint(p.GraphNode)
		}
		return nil
	})
	require.NoError(t, err)
	// We shouldn't see the first transaction writes until the committed
	tx2 := m.NewTransaction()
	err = tx2.WithReadOnly("test", diskstore.NewMemBucket(true), func(c ReadOnlyCache) error {
		for _, p := range randPoints {
			_, err := c.GetPoint(p.NodeId)
			require.Error(t, err)
		}
		return nil
	})
	require.NoError(t, err)
	tx.Commit(false)
	// Now we should see the writes
	tx3 := m.NewTransaction()
	err = tx3.WithReadOnly("test", diskstore.NewMemBucket(true), func(c ReadOnlyCache) error {
		for _, p := range randPoints {
			_, err := c.GetPoint(p.NodeId)
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)
}

func Test_DoubleWriteTransaction(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	randPoints := randCachePoints(10)
	err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
		// We'll create some points and check if the shared cache is pruned correctly
		for _, p := range randPoints {
			c.SetPoint(p.GraphNode)
		}
		return nil
	})
	require.NoError(t, err)
	// We shouldn't be able to write to the same cache again
	err = tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
		for _, p := range randPoints {
			_, err := c.GetPoint(p.NodeId)
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)
}

func Test_DoubleCrossWriteTransaction(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	randPoints := randCachePoints(10)
	err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
		// We'll create some points and check if the shared cache is pruned correctly
		for _, p := range randPoints {
			c.SetPoint(p.GraphNode)
		}
		return nil
	})
	require.NoError(t, err)
	tx.Commit(false)
	// We shouldn't be able to write to the same cache again
	tx2 := m.NewTransaction()
	err = tx2.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
		for _, p := range randPoints {
			_, err := c.GetPoint(p.NodeId)
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)
}

func Test_ScrapWhileWait(t *testing.T) {
	m := NewManager(-1)
	randPoints := randCachePoints(10)
	oops := errors.New("oops")
	arrivedSignal := make(chan struct{})
	youMayContinue := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	// We want to scrap a cache while another transaction is waiting for it
	go func() {
		defer wg.Done()
		tx := m.NewTransaction()
		err := tx.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
			// We'll create some points and check if the shared cache is pruned correctly
			for _, p := range randPoints {
				c.SetPoint(p.GraphNode)
			}
			arrivedSignal <- struct{}{}
			<-youMayContinue
			return oops
		})
		assert.Error(t, err)
		tx.Commit(false)
	}()
	go func() {
		defer wg.Done()
		arrivedSignal <- struct{}{}
		<-youMayContinue
		tx2 := m.NewTransaction()
		// We shouldn't see the writes of the first transaction because it was
		// scrapped due to error
		err := tx2.With("test", diskstore.NewMemBucket(false), func(c ReadWriteCache) error {
			for _, p := range randPoints {
				_, err := c.GetPoint(p.NodeId)
				assert.Error(t, err)
			}
			return nil
		})
		require.NoError(t, err)
	}()
	for i := 0; i < 2; i++ {
		<-arrivedSignal
	}
	close(youMayContinue)
	wg.Wait()
}
