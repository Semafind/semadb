package kvstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

type RepLogEntry struct {
	Key           string
	Value         []byte
	PrevValue     []byte
	TargetServers []string
	IsReplica     bool
	IsIndexed     bool
	Version       int64
}

type GraveyardEntry struct {
	Version int64
}

// Old timestamp gives stale error
func (kv *KVStore) WriteAsRepLog(repLog RepLogEntry) error {
	err := kv.db.Update(func(txn *badger.Txn) error {
		// ---------------------------
		// Get internal timestamp
		existingVersion := Versioned{Version: 0}
		var existingValue []byte
		item, err := txn.Get([]byte(repLog.Key))
		if err == badger.ErrKeyNotFound {
			// We don't have this key, it might have been deleted, check graveyard
			tomb, err := txn.Get([]byte(GRAVEYARD_PREFIX + repLog.Key))
			if err == nil {
				// Check time of death
				valErr := tomb.Value(func(val []byte) error {
					if err := msgpack.Unmarshal(val, &existingVersion); err != nil {
						return fmt.Errorf("failed to unmarshal: %w", err)
					}
					return nil
				})
				if valErr != nil {
					log.Error().Err(valErr).Msg("failed to get tombstone value")
				}
			}
		} else if err == nil {
			// We have this key, check version
			valErr := item.Value(func(val []byte) error {
				if err := msgpack.Unmarshal(val, &existingVersion); err != nil {
					return fmt.Errorf("failed to unmarshal: %w", err)
				}
				existingValue = val
				return nil
			})
			if valErr != nil {
				log.Error().Err(valErr).Msg("failed to get existing value")
			}
		} else {
			return fmt.Errorf("failed to get key: %w", err)
		}
		// ---------------------------
		// Check for stale data
		if existingVersion.Version > repLog.Version {
			log.Debug().Int64("requested", repLog.Version).Int64("current", existingVersion.Version).Msg("stale data")
			return ErrStaleData
		}
		if existingVersion.Version == repLog.Version {
			log.Debug().Str("key", repLog.Key).Msg("already exists")
			return ErrExistingKey
		}
		// ---------------------------
		// Handle delete case
		if bytes.Equal(repLog.Value, TOMBSTONE) {
			if err := txn.Delete([]byte(repLog.Key)); err != nil {
				return fmt.Errorf("failed to delete key: %w", err)
			}
			entry := GraveyardEntry{Version: repLog.Version}
			entryBytes, err := msgpack.Marshal(entry)
			if err != nil {
				return fmt.Errorf("failed to marshal graveyard entry: %w", err)
			}
			// TODO: Set TTL through config
			badgerEntry := badger.NewEntry([]byte(GRAVEYARD_PREFIX+repLog.Key), entryBytes).WithTTL(3 * time.Hour)
			if err := txn.SetEntry(badgerEntry); err != nil {
				return fmt.Errorf("failed to set key: %w", err)
			}
			return nil
		}
		// ---------------------------
		// Handle set case
		if err := txn.Set([]byte(repLog.Key), repLog.Value); err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
		// ---------------------------
		// Write replication log entry
		repLog.PrevValue = existingValue
		val, err := msgpack.Marshal(repLog)
		if err != nil {
			return fmt.Errorf("failed to marshal repLog: %w", err)
		}
		if err := txn.Set([]byte(REPLOG_PREFIX+repLog.Key), val); err != nil {
			return fmt.Errorf("failed to set replog key: %w", err)
		}
		return nil
	})
	return err
}

// ---------------------------

func (kv *KVStore) WriteRepLogEntry(repLog RepLogEntry) error {
	log.Debug().Str("key", repLog.Key).Msg("writing repLog entry")
	err := kv.db.Update(func(txn *badger.Txn) error {
		key := []byte(REPLOG_PREFIX + repLog.Key)
		val, err := msgpack.Marshal(repLog)
		if err != nil {
			return fmt.Errorf("failed to marshal repLog: %w", err)
		}
		if err := txn.Set(key, val); err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to add repLog entry: %w", err)
	}
	return nil
}

func (kv *KVStore) ScanRepLog() []RepLogEntry {
	out := make([]RepLogEntry, 0, 1)
	err := kv.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(REPLOG_PREFIX)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				var repLogEntry RepLogEntry
				if err := msgpack.Unmarshal(v, &repLogEntry); err != nil {
					return fmt.Errorf("failed to unmarshal repLog: %w", err)
				}
				repLogEntry.Key = string(k[len(REPLOG_PREFIX):])
				out = append(out, repLogEntry)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to scan repLog")
	}
	return out
}

func (kv *KVStore) DeleteRepLogEntry(key string) error {
	err := kv.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(REPLOG_PREFIX + key)); err != nil {
			return fmt.Errorf("failed to delete key: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to delete repLog key: %w", err)
	}
	return nil
}
