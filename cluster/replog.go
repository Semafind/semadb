package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/kvstore"
	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack/v5"
)

func (c *ClusterNode) createRepLogEntry(key string, value []byte) (kvstore.RepLogEntry, error) {
	// ---------------------------
	repLog := kvstore.RepLogEntry{
		Key:   key,
		Value: value,
	}
	// ---------------------------
	// Our job here is to ensure this key is replicated and / or sent to the
	// correct servers based on the knowledge we have. If we make a mistake,
	// it's okay because the next server will hopefully correct it. The
	// analogy is that of post offices with a shared address book.
	// ---------------------------
	targetServers, err := c.KeyPlacement(key, nil)
	if err != nil {
		log.Error().Err(err).Str("key", key).Msg("Could not get target servers")
		// The address book is down, send to everyone. We can't ignore the
		// entry because it might lead to data / replica loss.
		return repLog, fmt.Errorf("could not get target servers: %w", err)
	}
	// ---------------------------
	// Are we suppose to replicate or send? The difference is that replicate
	// should repeat until we have confirmation from all servers. Whereas
	// send can send to only one server and handoff that responsibility.
	myIndex := -1
	for i, server := range targetServers {
		if server == c.MyHostname {
			myIndex = i
			break
		}
	}
	targetServers = append(targetServers[:myIndex], targetServers[myIndex+1:]...)
	// ---------------------------
	repLog.TargetServers = targetServers
	repLog.IsReplica = myIndex != -1
	// ---------------------------
	return repLog, nil
}

func (c *ClusterNode) handleReplication(replog kvstore.RepLogEntry) []string {
	newTargets := make([]string, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var hadConflict atomic.Bool
	var successCount atomic.Int32
	// ---------------------------
	for _, server := range replog.TargetServers {
		wg.Add(1)
		go func(dest string) {
			writeKVReq := &WriteKVRequest{
				RequestArgs: RequestArgs{
					Source: c.MyHostname,
					Dest:   dest,
				},
				Key:   replog.Key,
				Value: replog.Value,
			}
			writeKVResp := &WriteKVResponse{}
			err := c.RPCWrite(writeKVReq, writeKVResp)
			switch {
			case err == nil:
				successCount.Add(1)
			case errors.Is(err, kvstore.ErrStaleData):
				// This means this RepLog entry is stale and we give up on it.
				hadConflict.Store(true)
			case err != nil && !errors.Is(err, kvstore.ErrExistingKey):
				log.Debug().Err(err).Str("key", replog.Key).Str("dest", dest).Msg("handleReplication")
				mu.Lock()
				newTargets = append(newTargets, dest)
				mu.Unlock()
			}
			wg.Done()
		}(server)
	}
	wg.Wait()
	// If had a conflict, we know the data is stale and we can delete it.
	// If all the targets have succeeded and we have no new targets, jobs done again.
	// If this is a handover instead of a replica, we can stop after the first success.
	if hadConflict.Load() || len(newTargets) == 0 || (!replog.IsReplica && successCount.Load() > 0) {
		return nil
	}
	log.Debug().Str("key", replog.Key).Strs("newTargets", newTargets).Str("action", "update").Msg("handleReplication")
	return newTargets
}

func (c *ClusterNode) handleCollectionDelete(replog kvstore.RepLogEntry) error {
	// ---------------------------
	// - U/<user>/C/<collection> (collection key)
	if !CollectionKeyRegex.MatchString(replog.Key) {
		return fmt.Errorf("invalid collection key: %s", replog.Key)
	}
	// ---------------------------
	// Purge values and any replogs for this collection.
	err := c.kvstore.PurgePrefixes(replog.Key, kvstore.REPLOG_PREFIX+replog.Key)
	if err != nil {
		return fmt.Errorf("could not purge collection: %w", err)
	}
	// ---------------------------
	return nil
}

func (c *ClusterNode) handleCollectionUpdate(replog kvstore.RepLogEntry) error {
	// ---------------------------
	if replog.PrevValue == nil {
		// Nothing to handle
		return nil
	}
	var prevCol models.Collection
	if err := msgpack.Unmarshal(replog.Value, &prevCol); err != nil {
		return fmt.Errorf("could not unmarshal collection: %w", err)
	}
	var newCol models.Collection
	if err := msgpack.Unmarshal(replog.Value, &newCol); err != nil {
		return fmt.Errorf("could not unmarshal collection: %w", err)
	}
	// Has anything that would affect placement changed?
	if prevCol.Replicas == newCol.Replicas {
		return nil
	}
	// ---------------------------
	// We need to find which points need to move.
	pointKeys, err := c.kvstore.KeyOnlyScanPrefix(replog.Key + kvstore.SHARD_PREFIX)
	if err != nil {
		return fmt.Errorf("could not scan points: %w", err)
	}
	for _, pointKey := range pointKeys {
		// ---------------------------
		// - U/<user>/C/<collection>/S/<shard>/P/<point> (point key)
		if !PointKeyRegex.MatchString(pointKey) {
			log.Debug().Str("key", pointKey).Msg("handleCollectionUpdate: invalid point key")
			continue
		}
		pointValue, err := c.kvstore.Read(pointKey)
		if err != nil {
			log.Error().Err(err).Str("key", pointKey).Msg("handleCollectionUpdate: could not read point")
			continue
		}
		// ---------------------------
		pointId := strings.Split(pointKey, "/")[7]
		newPointKey := pointId
		if pointKey != newPointKey {
			log.Debug().Str("key", pointKey).Str("newKey", newPointKey).Msg("handleCollectionUpdate: moving point")
			// We are moving this point to a new shard.
			// Delete old point key
			deleteRepLog := kvstore.RepLogEntry{
				Key:       pointKey,
				Value:     kvstore.TOMBSTONE,
				PrevValue: pointValue,
				Version:   replog.Version,
			}
			if err := c.kvstore.WriteAsRepLog(deleteRepLog); err != nil {
				log.Error().Err(err).Str("key", pointKey).Msg("handleCollectionUpdate: could not delete point")
			}
			// ---------------------------
			// Write new point key
			targetServers, err := c.KeyPlacement(newPointKey, &newCol)
			if err != nil {
				log.Error().Err(err).Str("key", newPointKey).Msg("handleCollectionUpdate: could not get new placement")
				continue
			}
			newRepLog := kvstore.RepLogEntry{
				Key:           newPointKey,
				Value:         pointValue,
				Version:       replog.Version,
				TargetServers: targetServers,
				IsReplica:     sliceContains[string](targetServers, c.MyHostname),
			}
			if err := c.kvstore.WriteAsRepLog(newRepLog); err != nil {
				log.Error().Err(err).Str("key", newPointKey).Msg("handleCollectionUpdate: could not write new point")
			}
			continue
		}
		// ---------------------------
		prevServers, err := c.KeyPlacement(pointKey, &prevCol)
		if err != nil {
			log.Error().Err(err).Str("key", pointKey).Msg("handleCollectionUpdate: could not get previous placement")
			continue
		}
		newServers, err := c.KeyPlacement(newPointKey, &newCol)
		if err != nil {
			log.Error().Err(err).Str("key", newPointKey).Msg("handleCollectionUpdate: could not get new placement")
			continue
		}
		// ---------------------------
		if sliceEquals[string](prevServers, newServers) {
			continue
		}
		// ---------------------------
		// Write replog entry for new placement
		newRepLog := kvstore.RepLogEntry{
			Key:           pointKey,
			Value:         pointValue,
			Version:       replog.Version,
			TargetServers: newServers,
			IsReplica:     sliceContains[string](newServers, c.MyHostname),
		}
		if err := c.kvstore.WriteAsRepLog(newRepLog); err != nil {
			log.Error().Err(err).Str("key", pointKey).Msg("handleCollectionUpdate: could not write replog")
			continue
		}
	}
	// ---------------------------
	return nil
}

func (c *ClusterNode) handlePointChange(replog kvstore.RepLogEntry) error {
	// ---------------------------
	// Parse key
	if !PointKeyRegex.MatchString(replog.Key) {
		log.Debug().Str("key", replog.Key).Msg("handlePointChange: invalid point key")
		return fmt.Errorf("invalid point key: %s", replog.Key)
	}
	// - U/<user>/C/<collection>/S/<shardId>/P/<point> (point key)
	// parts := strings.Split(replog.Key, "/")
	// userId := parts[1]
	// collectionId := parts[3]
	// shardId := parts[5]
	// pointId := parts[7]
	// ---------------------------
	// Has the point been deleted or moved?
	if bytes.Equal(replog.Value, kvstore.TOMBSTONE) || !replog.IsReplica {
		log.Debug().Str("key", replog.Key).Msg("handlePointChange: delete")
		// return c.removePointFromIndex(userId, collectionId, shardId, pointId)
		return nil
	}
	// ---------------------------
	// Is it a new point?
	if replog.PrevValue == nil {
		log.Debug().Str("key", replog.Key).Msg("handlePointChange: new")
		return nil
		// return c.insertPointIntoIndex(userId, collectionId, shardId, pointId)
	}
	// ---------------------------
	// Is it an update what requires re-indexing?
	var prevPoint models.Point
	if err := msgpack.Unmarshal(replog.Value, &prevPoint); err != nil {
		return fmt.Errorf("could not unmarshal point: %w", err)
	}
	var newPoint models.Point
	if err := msgpack.Unmarshal(replog.Value, &newPoint); err != nil {
		return fmt.Errorf("could not unmarshal point: %w", err)
	}
	if sliceEquals[float32](prevPoint.Vector, newPoint.Vector) {
		log.Debug().Str("key", replog.Key).Msg("handlePointChange: no vector change")
		return nil
	}
	// ---------------------------
	// return c.insertPointIntoIndex(userId, collectionId, shardId, pointId)
	return nil
}

func (c *ClusterNode) handleRepLogEntry(replog kvstore.RepLogEntry) {
	// ---------------------------
	log.Debug().Str("key", replog.Key).Msg("handleRepLogEntry")
	// ---------------------------
	switch {
	case CollectionKeyRegex.MatchString(replog.Key):
		if bytes.Equal(replog.Value, kvstore.TOMBSTONE) {
			// Delete collection
			replog.IsIndexed = c.handleCollectionDelete(replog) == nil
		} else if replog.PrevValue != nil {
			// Update collection
			replog.IsIndexed = c.handleCollectionUpdate(replog) == nil
		} else {
			// New collection
			replog.IsIndexed = true
		}
	case PointKeyRegex.MatchString(replog.Key):
		replog.IsIndexed = c.handlePointChange(replog) == nil
	default:
		log.Error().Str("key", replog.Key).Msg("handleRepLogEntry: unknown key")
	}
	replog.TargetServers = c.handleReplication(replog)
	if len(replog.TargetServers) == 0 && replog.IsIndexed {
		if err := c.kvstore.DeleteRepLogEntry(replog.Key); err != nil {
			log.Error().Err(err).Str("key", replog.Key).Msg("handleRepLogEntry: could not delete repLog entry")
		}
	} else {
		if err := c.kvstore.WriteRepLogEntry(replog); err != nil {
			log.Error().Err(err).Str("key", replog.Key).Msg("handleRepLogEntry: could not re-write repLog entry")
		}
	}
}

func (c *ClusterNode) startRepLogService() {
	log.Debug().Msg("startRepLogService")
	for {
		repLogs := c.kvstore.ScanRepLog()
		if len(repLogs) == 0 {
			// Sleep for between 10 and 20 seconds
			time.Sleep(time.Duration(10+rand.Intn(10)) * time.Second)
			continue
		}
		log.Debug().Int("len(repLogs)", len(repLogs)).Msg("RepLogService")
		// ---------------------------
		for _, replog := range repLogs {
			c.handleRepLogEntry(replog)
		}
		// ---------------------------
		time.Sleep(time.Duration(2+rand.Intn(4)) * time.Second)
	}
}
