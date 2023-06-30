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
	"github.com/semafind/semadb/rpcapi"
)

func (c *Cluster) kvOnWrite() {
	for event := range c.onWriteChannel {
		log.Debug().Str("key", event.Key).Msg("kvOnWrite")
		// ---------------------------
		// Our job here is to ensure this key is replicated and / or sent to the
		// correct servers based on the knowledge we have. If we make a mistake,
		// it's okay because the next server will hopefully correct it. The
		// analogy is that of post offices with a shared address book.
		// ---------------------------
		targetServers, err := c.KeyPlacement(event.Key)
		if err != nil {
			log.Error().Err(err).Msg("kvOnWrite")
			// The address book is down, send to everyone. We can't ignore the
			// entry because it might lead to data / replica loss.
			targetServers = c.Servers
		}
		// ---------------------------
		// Are we suppose to replicate or send? The difference is that replicate
		// should repeat until we have confirmation from all servers. Whereas
		// send can send to only one server and handoff that responsibility.
		isReplica := false
		for _, server := range targetServers {
			if server == c.rpcApi.MyHostname {
				isReplica = true
				break
			}
		}
		// ---------------------------
		repLog := kvstore.RepLogEntry{
			Key:           event.Key,
			Value:         event.Value,
			IsReplica:     isReplica,
			TargetServers: targetServers,
		}
		c.repLogMu.Lock()
		if err := c.kvstore.WriteRepLogEntry(repLog); err != nil {
			// TODO: Data loss might occur here
			log.Error().Err(err).Msg("kvOnWrite")
		}
		c.repLogMu.Unlock()
	}
}

func (c *Cluster) handleRepLogEntry(replog kvstore.RepLogEntry) error {
	// ---------------------------
	log.Debug().Str("key", replog.Key).Msg("handleRepLogEntry")
	// ---------------------------
	newTargets := make([]string, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var hadConflict atomic.Bool
	// ---------------------------
	for _, server := range replog.TargetServers {
		wg.Add(1)
		go func(dest string) {
			writeKVReq := &rpcapi.WriteKVRequest{
				RequestArgs: rpcapi.RequestArgs{
					Source: c.rpcApi.MyHostname,
					Dest:   dest,
				},
				Key:   []byte(replog.Key),
				Value: replog.Value,
			}
			writeKVResp := &rpcapi.WriteKVResponse{}
			err := c.rpcApi.WriteKV(writeKVReq, writeKVResp)
			switch {
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
	if hadConflict.Load() || len(newTargets) == 0 {
		// We're done here
		if err := c.kvstore.DeleteRepLogEntry(replog.Key); err != nil {
			// log.Error().Err(err).Str("key", repLog.Key).Msg("handleRepLogEntry.Delete")
			return fmt.Errorf("could not delete repLog entry: %w", err)
		}
		log.Debug().Str("key", replog.Key).Msg("handleRepLogEntry.Delete")
		return nil
	}
	// ---------------------------
	replog.TargetServers = newTargets
	if err := c.kvstore.WriteRepLogEntry(replog); err != nil {
		// log.Error().Err(err).Str("key", repLog.Key).Msg("handleRepLogEntry.Update")
		return fmt.Errorf("could not re-write repLog entry: %w", err)
	}
	log.Debug().Str("key", replog.Key).Strs("newTargets", newTargets).Msg("handleRepLogEntry.Update")
	return nil
}

func (c *Cluster) startRepLogService() {
	log.Debug().Msg("startRepLogService")
	for {
		// We have to lock to avoid any new entries from interleaving with the
		// current processing we are about to do
		c.repLogMu.Lock()
		repLogs := c.kvstore.ScanRepLog()
		if len(repLogs) == 0 {
			c.repLogMu.Unlock()
			time.Sleep(10 * time.Second)
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
		time.Sleep(time.Duration(rand.Intn(4)) * time.Second)
	}
}
