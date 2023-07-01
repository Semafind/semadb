package kvstore

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
	"github.com/vmihailenco/msgpack/v5"
)

// ---------------------------

type BadgerLogger struct {
}

func (l BadgerLogger) Errorf(format string, args ...interface{}) {
	log.Error().Str("component", "badger").Msgf(strings.TrimSpace(format), args...)
}

func (l BadgerLogger) Warningf(format string, args ...interface{}) {
	log.Warn().Str("component", "badger").Msgf(strings.TrimSpace(format), args...)
}

func (l BadgerLogger) Infof(format string, args ...interface{}) {
	log.Info().Str("component", "badger").Msgf(strings.TrimSpace(format), args...)
}

func (l BadgerLogger) Debugf(format string, args ...interface{}) {
	log.Debug().Str("component", "badger").Msgf(strings.TrimSpace(format), args...)
}

// ---------------------------

type KVStore struct {
	db *badger.DB
}

func NewKVStore() (*KVStore, error) {
	kvDir := config.Cfg.KVDir
	if kvDir == "" {
		log.Warn().Msg("kvdir not set, using temp dir")
		tempDir, err := os.MkdirTemp("", "semadb-kvdir-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp dir: %w", err)
		}
		kvDir = tempDir
	}
	log.Info().Str("kvDir", kvDir).Msg("using kvDir")
	// kvOpts := badger.DefaultOptions(kvDir).WithLogger(BadgerLogger{})
	kvOpts := badger.DefaultOptions(kvDir).WithLogger(nil)
	db, err := badger.Open(kvOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}
	return &KVStore{
		db: db,
	}, nil
}

func (kv *KVStore) Close() error {
	log.Info().Msg("closing kv store")
	return kv.db.Close()
}

// func (kv *KVStore) Get(key []byte) ([]byte, error) {
// 	var value []byte
// 	err := kv.db.View(func(txn *badger.Txn) error {
// 		item, err := txn.Get(key)
// 		if err != nil {
// 			return fmt.Errorf("failed to get key: %v", err)
// 		}
// 		value, err = item.ValueCopy(nil)
// 		if err != nil {
// 			return fmt.Errorf("failed to get value: %v", err)
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get key: %v", err)
// 	}
// 	return value, nil
// }

// func (kv *KVStore) Set(key, value []byte) error {
// 	err := kv.db.Update(func(txn *badger.Txn) error {
// 		err := txn.Set(key, value)
// 		if err != nil {
// 			return fmt.Errorf("failed to set key: %v", err)
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return fmt.Errorf("failed to set key: %v", err)
// 	}
// 	return nil
// }

// ---------------------------

type Versioned struct {
	Version int64
}

// ---------------------------

var ErrStaleData = errors.New("stale data")
var ErrExistingKey = errors.New("existing key")

type RepLogEntry struct {
	Key           string
	Value         []byte
	TargetServers []string
	IsReplica     bool
}

// Old timestamp gives stale error
func (kv *KVStore) WriteAsRepLog(repLog RepLogEntry) error {
	err := kv.db.Update(func(txn *badger.Txn) error {
		// ---------------------------
		// Get internal timestamp
		ourVersion := Versioned{Version: 0}
		item, err := txn.Get([]byte(repLog.Key))
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to get key: %w", err)
		}
		if err == nil {
			valErr := item.Value(func(val []byte) error {
				if err := msgpack.Unmarshal(val, &ourVersion); err != nil {
					return fmt.Errorf("failed to unmarshal: %w", err)
				}
				return nil
			})
			if valErr != nil {
				return fmt.Errorf("failed to get internal value: %w", err)
			}
		}
		theirVersion := Versioned{Version: time.Now().UnixNano()}
		if err := msgpack.Unmarshal(repLog.Value, &theirVersion); err != nil {
			return fmt.Errorf("failed to unmarshal given value version: %w", err)
		}
		// Check for stale data
		if ourVersion.Version > theirVersion.Version {
			log.Info().Int64("requested", theirVersion.Version).Int64("current", ourVersion.Version).Msg("stale data")
			return fmt.Errorf("stale data current > requested: %v > %v: %w", ourVersion.Version, theirVersion.Version, ErrStaleData)
		}
		if ourVersion.Version == theirVersion.Version {
			log.Debug().Str("key", repLog.Key).Msg("already exists")
			return ErrExistingKey
		}
		// ---------------------------
		// Set value
		if err := txn.Set([]byte(repLog.Key), repLog.Value); err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
		// ---------------------------
		// Write replication log entry
		if len(repLog.TargetServers) == 0 {
			log.Debug().Str("key", repLog.Key).Msg("no target servers, skipping replog")
			return nil
		}
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

// ---------------------------

var ErrKeyNotFound = errors.New("key not found")

func (kv *KVStore) Read(key string) ([]byte, error) {
	var out []byte
	err := kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return ErrKeyNotFound
		}
		if err != nil {
			return fmt.Errorf("failed to get key: %w", err)
		}
		out, err = item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get value: %w", err)
		}
		return nil
	})
	return out, err
}

// ---------------------------

type KVEntry struct {
	Key   string
	Value []byte
}

func (kv *KVStore) ScanPrefix(prefix string) ([]KVEntry, error) {
	kvEntries := make([]KVEntry, 0, 1)
	err := kv.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(prefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to get value: %w", err)
			}
			kvEntries = append(kvEntries, KVEntry{
				Key:   string(k),
				Value: val,
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan prefix: %w", err)
	}
	return kvEntries, nil
}
