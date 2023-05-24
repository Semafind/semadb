package collection

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

const (
	NODECOUNTKEY = "_NODECOUNT"
)

func uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

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
