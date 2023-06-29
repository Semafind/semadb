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
	Key   []byte
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
		// ---------------------------
		// Set value
		if err := txn.Set(key, value); err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
		return nil
	})
	return err
}
