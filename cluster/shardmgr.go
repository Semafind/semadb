package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard"
	"github.com/semafind/semadb/shard/cache"
)

type loadedShard struct {
	shardDir string
	shard    *shard.Shard
	doneCh   chan bool
	mu       sync.RWMutex // This locks stops the cleanup goroutine from unloading the shard while it is being used
}

type ShardManagerConfig struct {
	// Root directory for all shard data
	RootDir string `yaml:"rootDir"`
	// Shard timeout in seconds
	ShardTimeout int `yaml:"shardTimeout"`
	// Cache size in bytes, set to -1 for unlimited, 0 for no shared caching
	MaxCacheSize int32 `yaml:"maxCacheSize"`
}

type ShardManager struct {
	logger zerolog.Logger
	cfg    ShardManagerConfig
	// ---------------------------
	shardStore map[string]*loadedShard
	shardLock  sync.Mutex
	// Shared cache for all the shards loaded by this shard manager
	cacheManager *cache.Manager
}

func NewShardManager(config ShardManagerConfig) *ShardManager {
	logger := log.With().Str("component", "shardManager").Logger()
	return &ShardManager{
		logger:       logger,
		cfg:          config,
		shardStore:   make(map[string]*loadedShard),
		cacheManager: cache.NewManager(config.MaxCacheSize),
	}
}

// Load a shard into memory. If the shard is already loaded, the shard is
// returned from the local cache. The shard is unloaded after a timeout if it is
// not used.
func (sm *ShardManager) loadShard(collection models.Collection, shardId string) (*loadedShard, error) {
	shardDir := filepath.Join(sm.cfg.RootDir, "userCollections", collection.UserId, collection.Id, shardId)
	sm.logger.Debug().Str("shardDir", shardDir).Msg("LoadShard")
	sm.shardLock.Lock()
	defer sm.shardLock.Unlock()
	if ls, ok := sm.shardStore[shardDir]; ok {
		// We reset the timer here so that the shard is not unloaded prematurely
		sm.logger.Debug().Str("shardDir", shardDir).Msg("Returning cached shard")
		// We attempt a non-blocking send in case the clean up go routine is
		// busy unloading the shard. In that case the upstream shard client will
		// see a nil shard reference.
		select {
		case ls.doneCh <- false:
		default:
		}
		return ls, nil
	}
	// ---------------------------
	// Check shard directory exists, create if it doesn't
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return nil, fmt.Errorf("could not create shard directory: %w", err)
	}
	// Open shard
	shard, err := shard.NewShard(filepath.Join(shardDir, "sharddb.bbolt"), collection, sm.cacheManager)
	if err != nil {
		return nil, fmt.Errorf("could not open shard: %w", err)
	}
	ls := &loadedShard{
		shardDir: shardDir,
		shard:    shard,
		doneCh:   make(chan bool),
	}
	sm.shardStore[shardDir] = ls
	// ---------------------------
	// Setup cleanup goroutine
	go sm.cleanupRoutine(ls, collection.UserPlan.ShardBackupFrequency, collection.UserPlan.ShardBackupCount)
	return ls, nil
}

func (sm *ShardManager) cleanupRoutine(ls *loadedShard, backupFrequency, backupCount int) {
	shardDir := ls.shardDir
	timeoutDuration := time.Duration(sm.cfg.ShardTimeout) * time.Second
	timer := time.NewTimer(timeoutDuration)
	defer sm.logger.Debug().Str("shardDir", shardDir).Msg("Stopping shard cleanup goroutine")
	for {
		select {
		case isDone := <-ls.doneCh:
			// The following condition comes from the documentation of timer.Stop()
			if !timer.Stop() {
				<-timer.C
			}
			if isDone {
				return // stop cleanup goroutine
			} else {
				sm.logger.Debug().Str("shardDir", shardDir).Msg("Resetting shard timeout")
				// Timer must be stopped or expired before it can be reset
				timer.Reset(timeoutDuration)
			}
		case <-timer.C:
			sm.logger.Debug().Str("shardDir", shardDir).Msg("Unloading shard")
			ls.mu.Lock()
			defer ls.mu.Unlock() // we commit to exiting the cleanup goroutine here
			if ls.shard == nil {
				sm.logger.Debug().Str("shardDir", shardDir).Msg("Shard already unloaded")
				return
			}
			// ---------------------------
			// We probably should find a better place to backup the shard. The
			// original idea is that the when the shard is being unloaded it is
			// no longer busy and we can backup its content. But if a shard is
			// always busy then we never get a chance to backup its content. We
			// could backup the shard when it is being loaded, but that would be
			// a waste of resources if the shard is not used. Perhaps a
			// heuristic could be used in DoWithShard operation to determine a
			// backup is needed along side this one.
			if backupFrequency > 0 && backupCount > 0 {
				if err := ls.shard.Backup(backupFrequency, backupCount); err != nil {
					sm.logger.Error().Err(err).Str("shardDir", shardDir).Msg("Failed to backup shard")
				}
			}
			// ---------------------------
			// Time to say goodbye to the shard
			if err := ls.shard.Close(); err != nil {
				sm.logger.Error().Err(err).Str("shardDir", shardDir).Msg("Failed to close shard")
			}
			// We set the shard to nil so that other goroutines know it
			// is closed in case they are waiting on the lock
			sm.logger.Debug().Str("shardDir", shardDir).Msg("Removing loaded shard")
			ls.shard = nil
			sm.shardLock.Lock()
			delete(sm.shardStore, shardDir)
			sm.shardLock.Unlock()
			// ---------------------------
			return
		}
	}
}

// DoWithShard executes a function with a shard. The shard is loaded if it is
// not already loaded and prevents the shard from being cleaned up while the
// function is executing.
func (sm *ShardManager) DoWithShard(collection models.Collection, shardId string, f func(*shard.Shard) error) error {
	ls, err := sm.loadShard(collection, shardId)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	// This nil check is necessary because the shard may have been unloaded
	// while we were waiting for lock.
	if ls.shard == nil {
		return fmt.Errorf("shard %s is already closed", shardId)
	}
	return f(ls.shard)
}

func (sm *ShardManager) DeleteCollectionShards(collection models.Collection) ([]string, error) {
	// ---------------------------
	// We can't let shards be loaded while we are deleting them, this blocks
	// other shard loading too. In the future we can make this more efficient by
	// having a lock per collection. We don't expect too many delete collection
	// requests and this function in general should be fast.
	sm.shardLock.Lock()
	defer sm.shardLock.Unlock()
	// ---------------------------
	// Shard deletion is a best effort service, we don't return an error if
	// something goes wrong with the deletion of a shard. This is because the
	// deletion of the collection makes these shards inaccessible anyway. It is
	// a cleanup problem if a shard is not deleted.
	collectionDir := filepath.Join(sm.cfg.RootDir, "userCollections", collection.UserId, collection.Id)
	// List all shards in the collection directory
	shardDirs, err := os.ReadDir(collectionDir)
	if err != nil {
		return nil, fmt.Errorf("could not list shards: %w", err)
	}
	// Delete all shards
	deletedShardIds := make([]string, 0, len(shardDirs))
	for _, shardDirEntry := range shardDirs {
		if !shardDirEntry.IsDir() {
			continue
		}
		shardDir := filepath.Join(collectionDir, shardDirEntry.Name())
		// Is the shard already loaded?
		if ls, ok := sm.shardStore[shardDir]; ok {
			ls.mu.Lock()
			if ls.shard != nil {
				// The shard is loaded, we can't delete it before unloading it.
				// Signal in a non-blocking fashion that the cleanup goroutine
				// should stop. It may have already triggered the cleanup, in
				// that case it will see the nil shard reference.
				select {
				case ls.doneCh <- true:
				default:
				}
				if err := ls.shard.Close(); err != nil {
					// Not much we can do here, because we will be purging the shard
					sm.logger.Error().Err(err).Str("shardDir", shardDir).Msg("Failed to close shard")
				}
				ls.shard = nil
			}
			ls.mu.Unlock()
		}
		delete(sm.shardStore, shardDir)
		// The shard is not loaded, since we have exclusive lock on the
		// shardStore, we can directly delete it
		if err := os.RemoveAll(shardDir); err != nil {
			sm.logger.Error().Err(err).Str("shardDir", shardDir).Msg("Failed to delete shard")
			// Again, not much we can do here, because the shard can no longer
			// be used. We assume the collection entry is deleted.
		}
		sm.logger.Debug().Str("shardDir", shardDir).Msg("Deleted shard")
		deletedShardIds = append(deletedShardIds, shardDirEntry.Name())
	}
	// ---------------------------
	// If the collection directory is empty, delete it. There doesn't seem to be
	// a ErrDirNotEmpty error, so we manually check the string returned by
	// os.Remove. The directory is not deleted if there are any files in it.
	if err := os.Remove(collectionDir); err != nil && !strings.Contains(err.Error(), "directory not empty") {
		sm.logger.Error().Err(err).Str("collectionDir", collectionDir).Msg("Failed to delete collection directory")
	}
	userDir := filepath.Dir(collectionDir)
	if err := os.Remove(userDir); err != nil && !strings.Contains(err.Error(), "directory not empty") {
		sm.logger.Error().Err(err).Str("userDir", userDir).Msg("Failed to delete user directory")
	}
	// ---------------------------
	return deletedShardIds, nil
}
