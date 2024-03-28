package cache

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------

type dummyCachable struct {
	sizeInMemory int64
	value        int64
}

func (d *dummyCachable) SizeInMemory() int64 {
	return d.sizeInMemory
}

func (d *dummyCachable) SetSizeInMemory(size int64) {
	d.sizeInMemory = size
}

func newDummyCachable(size int64, err error) func() (Cachable, error) {
	return func() (Cachable, error) { return &dummyCachable{sizeInMemory: size}, err }
}

var ErrOops = errors.New("oops")

func TestManager_Prune(t *testing.T) {
	t.Run("No space", func(t *testing.T) {
		m := NewManager(1)
		tx := m.NewTransaction()
		// We'll create some points and check if the shared cache is pruned correctly
		err := tx.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
			return nil
		})
		require.NoError(t, err)
		require.Len(t, m.sharedCaches, 0)
	})
	t.Run("With space", func(t *testing.T) {
		m := NewManager(1000000)
		tx := m.NewTransaction()
		// We'll create some points and check if the shared cache is pruned correctly
		err := tx.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
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
	err := tx.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
		// The cache should continue to exist during operation even if it is
		// release, i.e. the point is valid but not shared caches map anymore.
		m.Release("test")
		dummy := c.(*dummyCachable)
		require.Equal(t, int64(10), dummy.sizeInMemory)
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
	// We'll create some points first
	err := tx.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		dummy.value = 42
		return nil
	})
	require.NoError(t, err)
	require.Len(t, m.sharedCaches, 1)
	// By giving a new blank bucket, we are checking if the GetPoint never hits
	// the disk
	err = tx.With("test", true, newDummyCachable(10, ErrOops), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		require.Equal(t, int64(42), dummy.value)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, m.sharedCaches, 1)
}

func TestManager_SharedReadWhileWrite(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	err := tx.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		dummy.value = 42
		// While we are writing, let's access it as a read-only cache, this is
		// fine because the transaction holds the write lock and any reads
		// within a transaction sees the writes
		err := tx.With("test", true, newDummyCachable(17, nil), func(c Cachable) error {
			dummy := c.(*dummyCachable)
			require.Equal(t, int64(42), dummy.value)
			require.Equal(t, int64(10), dummy.sizeInMemory)
			return nil
		})
		require.NoError(t, err)
		// But from another transaction, we should not be able to see the writes
		tx2 := m.NewTransaction()
		err = tx2.With("test", true, newDummyCachable(21, nil), func(c Cachable) error {
			dummy := c.(*dummyCachable)
			require.Equal(t, int64(0), dummy.value)
			require.Equal(t, int64(21), dummy.sizeInMemory)
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
	// First write fails
	err := tx.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		dummy.value = 42
		return ErrOops
	})
	assert.True(t, errors.Is(err, ErrOops))
	// We shouldn't see the writes at all
	err = tx.With("test", true, newDummyCachable(10, nil), func(c Cachable) error {
		assert.Fail(t, "shouldn't be here")
		return nil
	})
	assert.Error(t, err)
	require.Len(t, m.sharedCaches, 0)
}

func Test_TransactionCommit(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	err := tx.With("test", false, newDummyCachable(42, nil), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		dummy.value = 42
		return nil
	})
	require.NoError(t, err)
	// We shouldn't see the first transaction writes until the committed
	tx2 := m.NewTransaction()
	err = tx2.With("test", true, newDummyCachable(10, nil), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		require.Equal(t, int64(0), dummy.value)
		require.Equal(t, int64(10), dummy.sizeInMemory)
		return nil
	})
	require.NoError(t, err)
	tx.Commit(false)
	// Now we should see the writes
	tx3 := m.NewTransaction()
	err = tx3.With("test", true, newDummyCachable(10, nil), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		require.Equal(t, int64(42), dummy.value)
		require.Equal(t, int64(42), dummy.sizeInMemory)
		return nil
	})
	require.NoError(t, err)
}

func Test_DoubleWriteTransaction(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	err := tx.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		dummy.value = 42
		return nil
	})
	require.NoError(t, err)
	// We shouldn't be able to write to the same cache again
	err = tx.With("test", false, newDummyCachable(10, ErrOops), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		require.Equal(t, int64(42), dummy.value)
		return nil
	})
	require.NoError(t, err)
}

func Test_DoubleCrossWriteTransaction(t *testing.T) {
	m := NewManager(-1)
	tx := m.NewTransaction()
	err := tx.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		dummy.value = 42
		return nil
	})
	require.NoError(t, err)
	tx.Commit(false)
	// We shouldn't be able to write to the same cache again
	tx2 := m.NewTransaction()
	err = tx2.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
		dummy := c.(*dummyCachable)
		require.Equal(t, int64(42), dummy.value)
		return nil
	})
	require.NoError(t, err)
}

func Test_ScrapWhileWait(t *testing.T) {
	m := NewManager(-1)
	arrivedSignal := make(chan struct{})
	youMayContinue := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	// We want to scrap a cache while another transaction is waiting for it
	go func() {
		defer wg.Done()
		tx := m.NewTransaction()
		err := tx.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
			dummy := c.(*dummyCachable)
			dummy.value = 42
			arrivedSignal <- struct{}{}
			<-youMayContinue
			return ErrOops
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
		err := tx2.With("test", false, newDummyCachable(10, nil), func(c Cachable) error {
			dummy := c.(*dummyCachable)
			require.Equal(t, int64(0), dummy.value)
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
