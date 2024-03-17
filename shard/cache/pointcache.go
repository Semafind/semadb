package cache

import (
	"fmt"

	"github.com/semafind/semadb/diskstore"
)

// ---------------------------

/* We are creating these two type interfaces so that the compiler can stop us
 * from accidentally writing to a read only cache. This way, things will be
 * enforced at compile time. */

// A read only cache that only exposes the functions that are safe to use in
// read only mode.
type ReadOnlyCache interface {
	GetPoint(uint64) (*CachePoint, error)
	WithPointNeighbours(point *CachePoint, readOnly bool, fn func([]*CachePoint) error) error
}

type ReadWriteCache interface {
	ReadOnlyCache
	SetPoint(GraphNode) (*CachePoint, error)
	EdgeScan(deleteSet map[uint64]struct{}) (toPrune, toSave []uint64, err error)
	flush() error
}

// ---------------------------

type pointCache struct {
	sharedCache *sharedInMemCache
	graphBucket diskstore.Bucket // This takes precedence over the read only bucket.
}

func (pc *pointCache) GetPoint(nodeId uint64) (*CachePoint, error) {
	pc.sharedCache.pointsMu.Lock()
	defer pc.sharedCache.pointsMu.Unlock()
	// ---------------------------
	if point, ok := pc.sharedCache.points[nodeId]; ok {
		if point.isDeleted {
			return nil, fmt.Errorf("point is deleted")
		}
		return point, nil
	}
	// ---------------------------
	point, err := getNode(pc.graphBucket, nodeId)
	if err != nil {
		return nil, err
	}
	newPoint := &CachePoint{
		GraphNode: point,
	}
	pc.sharedCache.points[nodeId] = newPoint
	pc.sharedCache.estimatedSize.Add(newPoint.estimateSize())
	return newPoint, nil
}

// Operate with a lock on the point neighbours. If the neighbours are not
// loaded, load them from the database.
func (pc *pointCache) WithPointNeighbours(point *CachePoint, readOnly bool, fn func([]*CachePoint) error) error {
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
	// Technically we can unlock loading lock here and use the neighboursMu lock
	// to have even more fine grain control. But that seems overkill for what is
	// to happen once.
	return fn(point.neighbours)
}

func (pc *pointCache) SetPoint(point GraphNode) (*CachePoint, error) {
	pc.sharedCache.pointsMu.Lock()
	defer pc.sharedCache.pointsMu.Unlock()
	newPoint := &CachePoint{
		GraphNode: point,
		isDirty:   true,
	}
	if newPoint.NodeId == 0 {
		return nil, fmt.Errorf("node id cannot be 0")
	}
	pc.sharedCache.points[newPoint.NodeId] = newPoint
	pc.sharedCache.estimatedSize.Add(newPoint.estimateSize())
	return newPoint, nil
}

func (pc *pointCache) EdgeScan(deleteSet map[uint64]struct{}) (toPrune, toSave []uint64, err error) {
	return scanNodeEdges(pc.graphBucket, deleteSet)
}

func (pc *pointCache) flush() error {
	pc.sharedCache.pointsMu.Lock()
	defer pc.sharedCache.pointsMu.Unlock()
	for _, point := range pc.sharedCache.points {
		if point.isDeleted {
			if err := deleteNode(pc.graphBucket, point.GraphNode); err != nil {
				return err
			}
			delete(pc.sharedCache.points, point.NodeId)
			pc.sharedCache.estimatedSize.Add(-point.estimateSize())
			continue
		}
		if point.isDirty {
			if err := setNode(pc.graphBucket, point.GraphNode); err != nil {
				return err
			}
			// Only one goroutine flushes the point cache so we are not locking
			// here.
			point.isDirty = false
			point.isEdgeDirty = false
			continue
		}
		if point.isEdgeDirty {
			if err := setNodeEdges(pc.graphBucket, point.GraphNode); err != nil {
				return err
			}
			point.isEdgeDirty = false
		}
	}
	return nil
}
