package cluster

import (
	"cmp"
	"slices"

	"github.com/cespare/xxhash"
)

// RendezvousHash returns a list of servers sorted by their score for the given key.
func RendezvousHash(key string, servers []string, topK int) []string {
	// ---------------------------
	type ServerScore struct {
		Server string
		Score  uint64
	}
	// ---------------------------
	scores := make([]ServerScore, len(servers))
	for i, server := range servers {
		// combinedKey := append(key, []byte(server)...)
		hash := xxhash.Sum64String(key + server)
		scores[i] = ServerScore{server, hash}
	}
	// Sort by score
	slices.SortFunc(scores, func(a, b ServerScore) int {
		return cmp.Compare(a.Score, b.Score)
	})
	// Convert back to string slice
	if topK > len(servers) {
		topK = len(servers)
	}
	res := make([]string, topK)
	for i := 0; i < topK; i++ {
		res[i] = scores[i].Server
	}
	return res
}
