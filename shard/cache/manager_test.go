package cache

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------

func TestManager_Prune(t *testing.T) {
	t.Run("No space", func(t *testing.T) {
		m := NewManager(1)
		// We'll create some points and check if the shared cache is pruned correctly
		err := m.With("test", newTempDiskStore(false), func(c ReadWriteCache) error {
			randPoints := randCachePoints(10)
			for _, p := range randPoints {
				c.SetPoint(p.ShardPoint)
			}
			return nil
		})
		require.NoError(t, err)
		require.Len(t, m.sharedCaches, 0)
	})
	t.Run("With space", func(t *testing.T) {
		m := NewManager(1000000)
		// We'll create some points and check if the shared cache is pruned correctly
		err := m.With("test", newTempDiskStore(false), func(c ReadWriteCache) error {
			randPoints := randCachePoints(10)
			for _, p := range randPoints {
				c.SetPoint(p.ShardPoint)
			}
			return nil
		})
		require.NoError(t, err)
		require.Len(t, m.sharedCaches, 1)
	})
}

func TestManager_Release(t *testing.T) {
	m := NewManager(-1)
	// We'll create some points and check if the shared cache is pruned correctly
	err := m.With("test", newTempDiskStore(false), func(c ReadWriteCache) error {
		// The cache should continue to exist during operation even if it is
		// release, i.e. the point is valid but not shared caches map anymore.
		m.Release("test")
		randPoints := randCachePoints(10)
		for _, p := range randPoints {
			c.SetPoint(p.ShardPoint)
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
	randPoints := randCachePoints(10)
	// We'll create some points and check if the shared cache is pruned correctly
	err := m.With("test", newTempDiskStore(false), func(c ReadWriteCache) error {
		for _, p := range randPoints {
			c.SetPoint(p.ShardPoint)
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, m.sharedCaches, 1)
	// By giving a new disk store, we are checking if we every hit the disk when
	// reading the points
	err = m.with("test", newTempDiskStore(true), true, func(c *PointCache) error {
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
	randPoints := randCachePoints(10)
	err := m.With("test", newTempDiskStore(false), func(c ReadWriteCache) error {
		for _, p := range randPoints {
			c.SetPoint(p.ShardPoint)
		}
		// While we are writing, let's access it as a read-only cache
		err := m.WithReadOnly("test", newTempDiskStore(true), func(c ReadOnlyCache) error {
			_, err := c.GetPoint(randPoints[0].NodeId)
			// We shouldn't be able to see the writes yet
			require.Error(t, err)
			return nil
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
}

func TestManager_ScrappedCache(t *testing.T) {
	m := NewManager(-1)
	oops := errors.New("oops")
	// Spawn some goroutines to write to the cache that fail
	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := m.With("test", newTempDiskStore(false), func(c ReadWriteCache) error {
				randPoints := randCachePoints(10)
				for _, p := range randPoints {
					c.SetPoint(p.ShardPoint)
				}
				return oops
			})
			require.True(t, errors.Is(err, oops))
		}()
		go func() {
			defer wg.Done()
			err := m.WithReadOnly("test", newTempDiskStore(true), func(c ReadOnlyCache) error {
				// We shouldn't be able to see the writes at all
				for i := 0; i < 10; i++ {
					_, err := c.GetPoint(uint64(i))
					require.Error(t, err)
				}
				return nil
			})
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	require.Len(t, m.sharedCaches, 0)
}
