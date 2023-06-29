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
		wal := kvstore.WALEntry{
			Key:           event.Key,
			Value:         event.Value,
			IsReplica:     isReplica,
			TargetServers: targetServers,
		}
		if err := c.kvstore.WriteWALEntry(wal); err != nil {
			// TODO: Data loss might occur here
			log.Error().Err(err).Msg("kvOnWrite")
		}
	}
}

func (c *Cluster) handleWALEntry(wal kvstore.WALEntry) error {
	// ---------------------------
	log.Debug().Str("key", wal.Key).Msg("handleWALEntry")
	// ---------------------------
	newTargets := make([]string, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var hadConflict atomic.Bool
	// ---------------------------
	for _, server := range wal.TargetServers {
		wg.Add(1)
		go func(dest string) {
			writeKVReq := &rpcapi.WriteKVRequest{
				RequestArgs: rpcapi.RequestArgs{
					Source: c.rpcApi.MyHostname,
					Dest:   dest,
				},
				Key:   []byte(wal.Key),
				Value: wal.Value,
			}
			writeKVResp := &rpcapi.WriteKVResponse{}
			err := c.rpcApi.WriteKV(writeKVReq, writeKVResp)
			switch {
			case errors.Is(err, kvstore.ErrStaleData):
				// This means this WAL entry is stale and we give up on it.
				hadConflict.Store(true)
			case err != nil && !errors.Is(err, kvstore.ErrExistingKey):
				log.Debug().Err(err).Str("key", wal.Key).Str("dest", dest).Msg("handleWALEntry")
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
		if err := c.kvstore.DeleteWALEntry(wal.Key); err != nil {
			// log.Error().Err(err).Str("key", wal.Key).Msg("handleWALEntry.Delete")
			return fmt.Errorf("could not delete wal entry: %w", err)
		}
		log.Debug().Str("key", wal.Key).Msg("handleWALEntry.Delete")
		return nil
	}
	// ---------------------------
	wal.TargetServers = newTargets
	if err := c.kvstore.WriteWALEntry(wal); err != nil {
		// log.Error().Err(err).Str("key", wal.Key).Msg("handleWALEntry.Update")
		return fmt.Errorf("could not re-write wal entry: %w", err)
	}
	log.Debug().Str("key", wal.Key).Strs("newTargets", newTargets).Msg("handleWALEntry.Update")
	return nil
}

func (c *Cluster) startWALService() {
	log.Debug().Msg("startWALService")
	for {
		wals := c.kvstore.ScanWAL()
		if len(wals) == 0 {
			time.Sleep(10 * time.Second)
			continue
		}
		log.Debug().Int("len(wals)", len(wals)).Msg("WALService")
		walChannel := make(chan kvstore.WALEntry, len(wals))
		// ---------------------------
		// Spawn workers to process WAL entries
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			go func() {
				for wal := range walChannel {
					if err := c.handleWALEntry(wal); err != nil {
						// We'll try again in the next cycle
						log.Error().Err(err).Msg("could not handle WAL entry")
					}
					wg.Done()
				}
			}()
		}
		// ---------------------------
		for _, wal := range wals {
			wg.Add(1)
			walChannel <- wal
		}
		close(walChannel)
		wg.Wait()
		time.Sleep(time.Duration(rand.Intn(4)) * time.Second)
	}
}
