package collection

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

const (
	NODECOUNTKEY = "_NODECOUNT"
)

// Merge function to add two uint64 numbers
func add(existing, new []byte) []byte {
	return uint64ToBytes(bytesToUint64(existing) + bytesToUint64(new))
}

func (c *Collection) increaseNodeCount(txn *badger.Txn, count uint64) error {
	// ---------------------------
	// Get the current node count
	item, err := txn.Get([]byte(NODECOUNTKEY))
	if err == badger.ErrKeyNotFound {
		// Initialise the database with the first node
		txn.Set([]byte(NODECOUNTKEY), uint64ToBytes(count))
		return nil
	} else if err != nil {
		return fmt.Errorf("could not get initial node count: %v", err)
	}
	// ---------------------------
	// Add the new count to the existing count
	err = item.Value(func(val []byte) error {
		newCount := add(val, uint64ToBytes(count))
		return txn.Set([]byte(NODECOUNTKEY), newCount)
	})
	if err != nil {
		return fmt.Errorf("could not set new node count: %v", err)
	}
	return err
}

func (c *Collection) getNodeCount() (uint64, error) {
	var countBytes uint64
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(NODECOUNTKEY))
		if err != nil {
			return fmt.Errorf("could not get node count: %v", err)
		}
		return item.Value(func(val []byte) error {
			countBytes = bytesToUint64(val)
			return nil
		})
	})
	if err != nil {
		return 0, fmt.Errorf("could not get node count: %v", err)
	}
	return countBytes, nil
}
