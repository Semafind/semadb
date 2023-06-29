package cluster

import (
	"sort"

	"github.com/cespare/xxhash"
)

type ServerScore struct {
	Server string
	Score  uint64
}

type ByScore []ServerScore

func (a ByScore) Len() int           { return len(a) }
func (a ByScore) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByScore) Less(i, j int) bool { return a[i].Score < a[j].Score }

// RendezvousHash returns a list of servers sorted by their score for the given key.
func RendezvousHash(key string, servers []string, topK int) []string {
	scores := make([]ServerScore, len(servers))
	for i, server := range servers {
		// combinedKey := append(key, []byte(server)...)
		hash := xxhash.Sum64String(key + server)
		scores[i] = ServerScore{server, hash}
	}
	sort.Sort(ByScore(scores))
	// Convert back to string slice
	resSize := len(servers)
	if topK < resSize {
		resSize = topK
	}
	res := make([]string, resSize)
	for i := 0; i < resSize; i++ {
		res[i] = scores[i].Server
	}
	return res
}
