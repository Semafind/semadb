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

type OnWriteEvent struct {
	Key   string
	Value []byte
}

// ---------------------------

type KVStore struct {
	db               *badger.DB
	onWriteObservers []chan OnWriteEvent
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
	kvOpts := badger.DefaultOptions(kvDir).WithLogger(BadgerLogger{})
	db, err := badger.Open(kvOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}
	return &KVStore{
		db: db,
	}, nil
}

func (kv *KVStore) RegisterOnWriteObserver(ch chan OnWriteEvent) {
	if kv.onWriteObservers == nil {
		kv.onWriteObservers = make([]chan OnWriteEvent, 0, 1)
	}
	kv.onWriteObservers = append(kv.onWriteObservers, ch)
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

// Old timestamp gives stale error
func (kv *KVStore) Insert(key, value []byte) error {
	err := kv.db.Update(func(txn *badger.Txn) error {
		// ---------------------------
		// Get internal timestamp
		ourVersion := Versioned{Version: 0}
		item, err := txn.Get(key)
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
		if err := msgpack.Unmarshal(value, &theirVersion); err != nil {
			return fmt.Errorf("failed to unmarshal given value version: %w", err)
		}
		// Check for stale data
		if ourVersion.Version > theirVersion.Version {
			log.Info().Int64("requested", theirVersion.Version).Int64("current", ourVersion.Version).Msg("stale data")
			return fmt.Errorf("stale data current > requested: %v > %v: %w", ourVersion.Version, theirVersion.Version, ErrStaleData)
		}
		if ourVersion.Version == theirVersion.Version {
			log.Debug().Str("key", string(key)).Msg("already exists")
			return ErrExistingKey
		}
		// ---------------------------
		// Set value
		if err := txn.Set(key, value); err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
		return nil
	})
	// If we have observers, notify them
	if err == nil {
		if kv.onWriteObservers != nil {
			for _, ch := range kv.onWriteObservers {
				ch <- OnWriteEvent{
					Key:   string(key),
					Value: value,
				}
			}
		}
	}
	return err
}

// ---------------------------

type WALEntry struct {
	Key           string
	Value         []byte
	TargetServers []string
	IsReplica     bool
}

func (kv *KVStore) WriteWALEntry(wal WALEntry) error {
	log.Debug().Str("key", wal.Key).Msg("writing wal entry")
	err := kv.db.Update(func(txn *badger.Txn) error {
		key := []byte(WAL_PREFIX + wal.Key)
		val, err := msgpack.Marshal(wal)
		if err != nil {
			return fmt.Errorf("failed to marshal wal: %w", err)
		}
		if err := txn.Set(key, val); err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to add wal entry: %w", err)
	}
	return nil
}

func (kv *KVStore) ScanWAL() []WALEntry {
	out := make([]WALEntry, 0, 1)
	err := kv.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(WAL_PREFIX)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				var walEntry WALEntry
				if err := msgpack.Unmarshal(v, &walEntry); err != nil {
					return fmt.Errorf("failed to unmarshal wal: %w", err)
				}
				walEntry.Key = string(k[len(WAL_PREFIX):])
				out = append(out, walEntry)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to scan wal")
	}
	return out
}

func (kv *KVStore) DeleteWALEntry(key string) error {
	err := kv.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(WAL_PREFIX + key)); err != nil {
			return fmt.Errorf("failed to delete key: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to wal key: %w", err)
	}
	return nil
}
