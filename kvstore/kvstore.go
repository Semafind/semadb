package kvstore

import (
	"errors"
	"fmt"
	"os"
	"strings"

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
	kvOpts := badger.DefaultOptions(kvDir).WithLogger(BadgerLogger{})
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

type KVInternal struct {
	Timestamp int64
}

// ---------------------------

var ErrStaleData = errors.New("stale data")

// Old timestamp gives stale error
func (kv *KVStore) Insert(key, value []byte, timestamp int64) error {
	err := kv.db.Update(func(txn *badger.Txn) error {
		// ---------------------------
		// Get internal timestamp
		item, err := txn.Get(append(key, INTERNAL_SUFFIX...))
		if err != nil {
			return fmt.Errorf("failed to get key: %v", err)
		}
		var itemInternal KVInternal
		err = item.Value(func(val []byte) error {
			if err := msgpack.Unmarshal(val, &itemInternal); err != nil {
				return fmt.Errorf("failed to unmarshal: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to get internal value: %w", err)
		}
		// Check for stale data
		if itemInternal.Timestamp > timestamp {
			log.Info().Int64("requested", timestamp).Int64("current", itemInternal.Timestamp).Msg("stale data")
			return fmt.Errorf("stale data current > requested: %v > %v: %w", itemInternal.Timestamp, timestamp, ErrStaleData)
		}
		// ---------------------------
		// Set internal value with timestamp
		internal := KVInternal{
			Timestamp: timestamp,
		}
		internalBytes, err := msgpack.Marshal(&internal)
		if err != nil {
			return fmt.Errorf("failed to marshal internal: %w", err)
		}
		if err := txn.Set(append(key, INTERNAL_SUFFIX...), internalBytes); err != nil {
			return fmt.Errorf("failed to set internal: %w", err)
		}
		if err := txn.Set(key, value); err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
		return nil
	})
	return err
}
