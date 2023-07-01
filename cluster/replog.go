package cluster

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/kvstore"
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
	targetServers, err := c.KeyPlacement(key)
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

func (c *ClusterNode) handleRepLogEntry(replog kvstore.RepLogEntry) error {
	// ---------------------------
	log.Debug().Str("key", replog.Key).Msg("handleRepLogEntry")
	// ---------------------------
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
				log.Debug().Err(err).Str("key", replog.Key).Str("dest", dest).Msg("handleRepLogEntry")
				mu.Lock()
				newTargets = append(newTargets, dest)
				mu.Unlock()
			}
			wg.Done()
		}(server)
	}
	wg.Wait()
	// ---------------------------
	// If had a conflict, we know the data is stale and we can delete it.
	// If all the targets have succeeded and we have no new targets, jobs done again.
	// If this is a handover instead of a replica, we can stop after the first success.
	if hadConflict.Load() || len(newTargets) == 0 || (!replog.IsReplica && successCount.Load() > 0) {
		// We're done here
		if err := c.kvstore.DeleteRepLogEntry(replog.Key); err != nil {
			// log.Error().Err(err).Str("key", repLog.Key).Msg("handleRepLogEntry.Delete")
			return fmt.Errorf("could not delete repLog entry: %w", err)
		}
		log.Debug().Str("key", replog.Key).Str("action", "delete").Msg("handleRepLogEntry")
		return nil
	}
	// ---------------------------
	replog.TargetServers = newTargets
	if err := c.kvstore.WriteRepLogEntry(replog); err != nil {
		// log.Error().Err(err).Str("key", repLog.Key).Msg("handleRepLogEntry.Update")
		return fmt.Errorf("could not re-write repLog entry: %w", err)
	}
	log.Debug().Str("key", replog.Key).Strs("newTargets", newTargets).Str("action", "update").Msg("handleRepLogEntry")
	return nil
}

func (c *ClusterNode) startRepLogService() {
	log.Debug().Msg("startRepLogService")
	for {
		// We have to lock to avoid any new entries from interleaving with the
		// current processing we are about to do
		c.repLogMu.Lock()
		repLogs := c.kvstore.ScanRepLog()
		if len(repLogs) == 0 {
			c.repLogMu.Unlock()
			// Sleep for between 10 and 20 seconds
			time.Sleep(time.Duration(10+rand.Intn(10)) * time.Second)
			continue
		}
		log.Debug().Int("len(repLogs)", len(repLogs)).Msg("RepLogService")
		repLogChannel := make(chan kvstore.RepLogEntry, len(repLogs))
		// ---------------------------
		// Spawn workers to process RepLog entries
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			go func() {
				for repLog := range repLogChannel {
					if err := c.handleRepLogEntry(repLog); err != nil {
						// We'll try again in the next cycle
						log.Error().Err(err).Msg("could not handle RepLog entry")
					}
					wg.Done()
				}
			}()
		}
		// ---------------------------
		for _, repLog := range repLogs {
			wg.Add(1)
			repLogChannel <- repLog
		}
		close(repLogChannel)
		wg.Wait()
		c.repLogMu.Unlock()
		time.Sleep(time.Duration(2+rand.Intn(4)) * time.Second)
	}
}
