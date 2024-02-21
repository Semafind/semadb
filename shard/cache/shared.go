package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

type sharedInMemCache struct {
	points   map[uint64]*CachePoint
	pointsMu sync.Mutex
	// ---------------------------
	lastAccessed time.Time
	// We use this to estimate the size of the cache. This is not accurate but
	// it's good enough for the manager to decide when to discard.
	estimatedSize atomic.Int64
	// ---------------------------
	// We currently allow a single writer to the cache at a time. This is because
	// within a single transaction there could be multiple goroutines writing to
	// the cache and invalidating the cache is difficult with multiple writers.
	mu sync.RWMutex
	// ---------------------------
	// Indicates if the cache is not consistent with the database and must be
	// discarded, occurs if a transaction goes wrong but was operating on the
	// cache. So, we get two options, either roll back stuff in the cache
	// (difficult to do) or scrap it (easy). We are going with the easy option.
	scrapped bool
}

func newSharedInMemCache() *sharedInMemCache {
	return &sharedInMemCache{
		points:       make(map[uint64]*CachePoint),
		lastAccessed: time.Now(),
	}
}

// Represents a single point in the cache, it uses dirty flags to determine
// whether it needs to be flushed to the database. Access to the point via the
// cache is protected by a mutex.
type CachePoint struct {
	ShardPoint
	isDirty     bool
	isEdgeDirty bool
	isDeleted   bool
	// ---------------------------
	// Neighbours are loaded lazily. This is because we don't want to load the
	// entire graph into memory. We only load the neighbours when we need them.
	neighbours       []*CachePoint
	neighboursMu     sync.RWMutex
	loadMu           sync.Mutex
	loadedNeighbours bool
}

func (p *CachePoint) ClearNeighbours() {
	p.edges = p.edges[:0]
	p.neighbours = p.neighbours[:0]
	p.isEdgeDirty = true
}

func (p *CachePoint) AddNeighbour(neighbour *CachePoint) int {
	p.edges = append(p.edges, neighbour.NodeId)
	p.neighbours = append(p.neighbours, neighbour)
	p.isEdgeDirty = true
	return len(p.edges)
}

func (p *CachePoint) AddNeighbourIfNotExists(neighbour *CachePoint) int {
	for _, n := range p.neighbours {
		if n.NodeId == neighbour.NodeId {
			return len(p.edges)
		}
	}
	return p.AddNeighbour(neighbour)
}

func (p *CachePoint) Delete() {
	p.isDeleted = true
}

func (cp *CachePoint) estimateSize() int64 {
	return int64(len(cp.edges)*8 + len(cp.Vector)*4 + len(cp.Metadata))
}
