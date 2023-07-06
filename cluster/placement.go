package cluster

import (
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/kvstore"
	"github.com/semafind/semadb/models"
)

// Different key types:
// - U/<user>/C (user collections)
// - U/<user>/C/<collection> (collection key)
// - U/<user>/C/<collection>/S/<shardId>/P/<point> (point key)

var CollectionKeyRegex = regexp.MustCompile(`^U\/\w+\/C\/\w+$`)
var PointKeyRegex = regexp.MustCompile(`^U\/\w+\/C\/\w+\/S\/\w+\/P\/\w+$`)

func newCollectionKey(userId, collectionId string) string {
	return kvstore.USER_PREFIX + userId + kvstore.DELIMITER + kvstore.COLLECTION_PREFIX + collectionId
}

func newUserCollectionKeyPrefix(userId string) string {
	return kvstore.USER_PREFIX + userId + kvstore.DELIMITER + kvstore.COLLECTION_PREFIX
}

func (c *ClusterNode) KeyPlacement(key string, col *models.Collection) ([]string, error) {
	var servers []string
	switch {
	case CollectionKeyRegex.MatchString(key):
		// Collection gets placed on all servers
		return c.Servers, nil
	case PointKeyRegex.MatchString(key):
		parts := strings.Split(key, "/")
		userId := parts[1]
		collectionId := parts[3]
		shardId := parts[5]
		shardKey := userId + collectionId + shardId
		// Where does this shard live?
		c.serversMu.RLock()
		servers = RendezvousHash(shardKey, c.Servers, int(col.Replicas))
		c.serversMu.RUnlock()
	default:
		log.Error().Str("key", key).Msg("Unknown key type")
		return nil, fmt.Errorf("unknown key type %v", key)
	}
	return servers, nil
}

func (c *ClusterNode) ClusterWrite(key string, value []byte, targetServers []string) error {
	// ---------------------------
	log.Debug().Str("key", key).Strs("targetServers", targetServers).Msg("ClusterWrite")
	results := make(chan error, len(targetServers))
	for _, server := range targetServers {
		go func(dest string) {
			writeKVReq := &WriteKVRequest{
				RequestArgs: RequestArgs{
					Source: c.MyHostname,
					Dest:   dest,
				},
				Key:   key,
				Value: value,
			}
			writeKVResp := &WriteKVResponse{}
			results <- c.RPCWrite(writeKVReq, writeKVResp)
		}(server)
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
		case errors.Is(err, ErrTimeout):
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

// ---------------------------

// Consistency level -1 = ALL, 0 = Quorum, 1 = One, 2 = Two etc.
func (c *ClusterNode) ClusterGet(key string, targetServers []string, consistencyLevel int) ([][]byte, error) {
	// ---------------------------
	// Permute target servers
	rand.Shuffle(len(targetServers), func(i, j int) {
		targetServers[i], targetServers[j] = targetServers[j], targetServers[i]
	})
	// ---------------------------
	switch consistencyLevel {
	case -1:
		// ALL
	case 0:
		// Quorum
		quorum := len(targetServers)/2 + 1
		targetServers = targetServers[:quorum]
	default:
		// One, Two etc.
		targetServers = targetServers[:consistencyLevel]
	}
	// ---------------------------
	type getKVResult struct {
		value []byte
		err   error
	}
	// ---------------------------
	// Contact target servers
	results := make(chan getKVResult, len(targetServers))
	for _, server := range targetServers {
		go func(dest string) {
			getKVReq := &ReadKVRequest{
				RequestArgs: RequestArgs{
					Source: c.MyHostname,
					Dest:   dest,
				},
				Key: key,
			}
			getKVResp := new(ReadKVResponse)
			err := c.RPCRead(getKVReq, getKVResp)
			results <- getKVResult{
				value: getKVResp.Value,
				err:   err,
			}

		}(server)
	}
	// ---------------------------
	// Collection results
	values := make([][]byte, 0, len(targetServers))
	timeoutCount := 0
	notFoundCount := 0
	failCount := 0
	for i := 0; i < len(targetServers); i++ {
		result := <-results
		switch {
		case result.err == nil:
			values = append(values, result.value)
		case errors.Is(result.err, kvstore.ErrKeyNotFound):
			notFoundCount++
		case errors.Is(result.err, ErrTimeout):
			timeoutCount++
		default:
			log.Error().Err(result.err).Msg("ClusterGet")
			failCount++
		}
	}
	log.Debug().Int("timeoutCount", timeoutCount).Int("notFoundCount", notFoundCount).Int("failCount", failCount).Msg("ClusterGet")
	// ---------------------------
	var finalError error = nil
	switch {
	case timeoutCount == len(targetServers):
		finalError = ErrTimeout
	case notFoundCount == len(targetServers):
		finalError = ErrNotFound
	case failCount == len(targetServers):
		finalError = ErrNoSuccess
	case failCount > 0:
		finalError = ErrPartialSuccess
	}
	// ---------------------------
	return values, finalError
}

// ---------------------------

func (c *ClusterNode) ClusterScan(prefix string, targetServers []string) ([]kvstore.KVEntry, error) {
	// ---------------------------
	type scanKVResult struct {
		entries []kvstore.KVEntry
		err     error
	}
	// ---------------------------
	results := make(chan scanKVResult, len(targetServers))
	for _, server := range targetServers {
		go func(dest string) {
			scanKVReq := &ScanKVRequest{
				RequestArgs: RequestArgs{
					Source: c.MyHostname,
					Dest:   dest,
				},
				Prefix: prefix,
			}
			scanKVResp := new(ScanKVResponse)
			err := c.RPCScan(scanKVReq, scanKVResp)
			results <- scanKVResult{
				entries: scanKVResp.Entries,
				err:     err,
			}
		}(server)
	}
	// ---------------------------
	// Collect results
	var entries []kvstore.KVEntry
	errorCount := 0
	timeoutCount := 0
	for i := 0; i < len(targetServers); i++ {
		result := <-results
		switch {
		case errors.Is(result.err, ErrTimeout):
			timeoutCount++
		case result.err != nil:
			log.Error().Err(result.err).Msg("ClusterScan")
			errorCount++
		default:
			entries = append(entries, result.entries...)
		}
	}
	if timeoutCount == len(targetServers) {
		return nil, ErrTimeout
	}
	if errorCount == len(targetServers) {
		return nil, ErrNoSuccess
	}
	// ---------------------------
	return entries, nil
}
