package cache

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

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

func (cp *CachePoint) ClearNeighbours() {
	cp.edges = cp.edges[:0]
	cp.neighbours = cp.neighbours[:0]
	cp.isEdgeDirty = true
}

func (cp *CachePoint) AddNeighbour(neighbour *CachePoint) int {
	cp.edges = append(cp.edges, neighbour.NodeId)
	cp.neighbours = append(cp.neighbours, neighbour)
	cp.isEdgeDirty = true
	return len(cp.edges)
}

func (cp *CachePoint) Delete() {
	cp.isDeleted = true
}

// ---------------------------

type PointCache struct {
	bucket *bbolt.Bucket
	points map[uint64]*CachePoint
	mu     sync.Mutex
}

func NewPointCache(bucket *bbolt.Bucket) *PointCache {
	return &PointCache{
		bucket: bucket,
		points: make(map[uint64]*CachePoint),
	}
}

func (pc *PointCache) GetPoint(nodeId uint64) (*CachePoint, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	// ---------------------------
	if point, ok := pc.points[nodeId]; ok {
		return point, nil
	}
	// ---------------------------
	point, err := getNode(pc.bucket, nodeId)
	if err != nil {
		return nil, err
	}
	newPoint := &CachePoint{
		ShardPoint: point,
	}
	pc.points[nodeId] = newPoint
	return newPoint, nil
}

func (pc *PointCache) GetPointByUUID(pointId uuid.UUID) (*CachePoint, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	point, err := getPointByUUID(pc.bucket, pointId)
	if err != nil {
		return nil, err
	}
	newPoint := &CachePoint{
		ShardPoint: point,
	}
	pc.points[point.NodeId] = newPoint
	return newPoint, nil
}

// Operate with a lock on the point neighbours. If the neighbours are not
// loaded, load them from the database.
func (pc *PointCache) WithPointNeighbours(point *CachePoint, readOnly bool, fn func([]*CachePoint) error) error {
	/* We have to lock here because we can't have another goroutine changing the
	 * edges while we are using them. The read only case mainly occurs in
	 * searching whereas the writes happen for pruning edges. By using
	 * read-write lock, we are hoping the search doesn't get blocked too much in
	 * case there are concurrent insert, update, delete operations.
	 *
	 * Why are we not just locking each point, reading the neighbours and
	 * unlocking as opposed to locking throughout an operation. This is because
	 * if we know a goroutine has a chance to change the neighbours, another go
	 * routine might read outdated edges that might lead to disconnected graph.
	 * Consider the base case, 1 node with no edges, 2 go routines trying to
	 * insert. If locked only for reading, they'll both think there are no edges
	 * and race to add the first connection.
	 *
	 * Hint: to check if things are working, run:
	 * go test -race ./shard */
	// ---------------------------
	point.loadMu.Lock()
	// This check needs to be syncronized because we don't want two go routines
	// to load the neighbours at the same time.
	if point.loadedNeighbours {
		// Early return if the neighbours are already loaded, what would the
		// goroutine like to do?
		point.loadMu.Unlock()
		if readOnly {
			point.neighboursMu.RLock()
			defer point.neighboursMu.RUnlock()
		} else {
			point.neighboursMu.Lock()
			defer point.neighboursMu.Unlock()
		}
		return fn(point.neighbours)
	}
	defer point.loadMu.Unlock()
	// ---------------------------
	neighbours := make([]*CachePoint, 0, len(point.edges))
	for _, edgeId := range point.edges {
		edge, err := pc.GetPoint(edgeId)
		if err != nil {
			return err
		}
		neighbours = append(neighbours, edge)
	}
	point.neighbours = neighbours
	point.loadedNeighbours = true
	// Technically we unlock loading lock here and use the neighboursMu lock to
	// have even more fine grain control. But that seems overkill for what is to
	// happen once.
	return fn(point.neighbours)
}

func (pc *PointCache) SetPoint(point ShardPoint) (*CachePoint, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	newPoint := &CachePoint{
		ShardPoint: point,
		isDirty:    true,
	}
	if newPoint.NodeId == 0 {
		return nil, fmt.Errorf("node id cannot be 0")
	}
	pc.points[newPoint.NodeId] = newPoint
	return newPoint, nil
}

func (pc *PointCache) GetMetadata(nodeId uint64) ([]byte, error) {
	cp, err := pc.GetPoint(nodeId)
	if err != nil {
		return nil, err
	}
	// Backfill metadata if it's not set
	if cp.Metadata == nil {
		mdata, err := getPointMetadata(pc.bucket, nodeId)
		if err != nil {
			return nil, err
		}
		cp.Metadata = mdata
	}
	return cp.Metadata, nil
}

func (pc *PointCache) EdgeScan(deleteSet map[uint64]struct{}) ([]uint64, error) {
	return scanPointEdges(pc.bucket, deleteSet)
}

func (pc *PointCache) Flush() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, point := range pc.points {
		if point.isDeleted {
			if err := deletePoint(pc.bucket, point.ShardPoint); err != nil {
				return err
			}
			continue
		}
		if point.isDirty {
			if err := setPoint(pc.bucket, point.ShardPoint); err != nil {
				return err
			}
			continue
		}
		if point.isEdgeDirty {
			if err := setPointEdges(pc.bucket, point.ShardPoint); err != nil {
				return err
			}
		}
	}
	return nil
}
