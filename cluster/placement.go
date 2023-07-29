package cluster

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/semafind/semadb/models"
)

// Different key types:
// - U/<user>/C (user collections)
// - U/<user>/C/<collection> (collection key)
// - U/<user>/C/<collection>/S/<shardId>/P/<point> (point key)

var CollectionKeyRegex = regexp.MustCompile(`^U\/\w+\/C\/\w+$`)
var PointKeyRegex = regexp.MustCompile(`^U\/\w+\/C\/\w+\/S\/\w+\/P\/\w+$`)

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
		c.logger.Error().Str("key", key).Msg("Unknown key type")
		return nil, fmt.Errorf("unknown key type %v", key)
	}
	return servers, nil
}

// ---------------------------
