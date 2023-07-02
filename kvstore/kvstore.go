package kvstore

import (
	"fmt"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
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

// ---------------------------

type Versioned struct {
	Version int64
}

// ---------------------------

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
