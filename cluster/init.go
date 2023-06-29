package cluster

import (
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/kvstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/rpcapi"
	"github.com/vmihailenco/msgpack"
)

type Cluster struct {
	Servers []string
	mu      sync.RWMutex
	kvstore *kvstore.KVStore
	rpcApi  *rpcapi.RPCAPI
}

func New(kvstore *kvstore.KVStore, rpcApi *rpcapi.RPCAPI) (*Cluster, error) {
	return &Cluster{Servers: config.Cfg.Servers, kvstore: kvstore, rpcApi: rpcApi}, nil
}

// ---------------------------

type CreateCollectionRequest struct {
	UserId     string
	Collection models.Collection
}

func (c *Cluster) CreateCollection(req CreateCollectionRequest) error {
	// ---------------------------
	// Construct key and value
	// e.g. U/ USERID / C/ COLLECTIONID
	userKey := kvstore.USER_PREFIX + req.UserId
	collectionKey := []byte(userKey + kvstore.DELIMITER + kvstore.COLLECTION_PREFIX + req.Collection.Id)
	collectionValue, err := msgpack.Marshal(req.Collection)
	if err != nil {
		return fmt.Errorf("could not marshal collection: %w", err)
	}
	// ---------------------------
	// Get target servers
	repCount := config.Cfg.GeneralReplication
	// These servers are responsible for the collection under the current cluster configuration
	c.mu.RLock()
	targetServers := RendezvousHash(userKey, c.Servers, repCount)
	c.mu.Unlock()
	// ---------------------------
	log.Debug().Interface("targetServers", targetServers).Msg("NewCollection")
	results := make(chan error, len(targetServers))
	for _, server := range targetServers {
		go func(dest string) {
			writeKVReq := &rpcapi.WriteKVRequest{
				RequestArgs: rpcapi.RequestArgs{
					Source: c.rpcApi.MyHostname,
					Dest:   dest,
				},
				Key:   collectionKey,
				Value: collectionValue,
			}
			writeKVResp := &rpcapi.WriteKVResponse{}
			results <- c.rpcApi.WriteKV(writeKVReq, writeKVResp)
		}(server.Server)
	}
	// ---------------------------
	successCount := 0
	conflictCount := 0
	timeoutCount := 0
	for i := 0; i < len(targetServers); i++ {
		err := <-results
		switch {
		case err == nil:
			successCount++
		case errors.Is(err, kvstore.ErrStaleData):
			conflictCount++
		case errors.Is(err, rpcapi.ErrRPCTimeout):
			timeoutCount++
		default:
			log.Error().Err(err).Msg("NewCollection")
		}
	}
	log.Debug().Int("successCount", successCount).Int("conflictCount", conflictCount).Int("timeoutCount", timeoutCount).Msg("NewCollection")
	// ---------------------------
	switch {
	case conflictCount > 0:
		// We don't need to do anything else here because we know there is a
		// newer version of the collection
		return ErrConflict
	case timeoutCount == len(targetServers):
		// Everything timed out, we can't handoff
		return ErrTimeout
	case successCount == 0:
		// Everything errored out, nothing we can do
		return ErrNoSuccess
	case successCount == len(targetServers):
		// Everything succeeded, nothing to do
		return nil
	}
	// ---------------------------
	return ErrPartialSuccess
}
