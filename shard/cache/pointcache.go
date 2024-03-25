package cache

import (
	"errors"
	"fmt"

	"github.com/semafind/semadb/diskstore"
)

// ---------------------------

type SharedPointCache interface {
	IsReadOnly() bool
	GetPoint(uint64) (*CachePoint, error)
	WithPointNeighbours(point *CachePoint, readOnly bool, fn func([]*CachePoint) error) error
	SetPoint(GraphNode) (*CachePoint, error)
	EdgeScan(deleteSet map[uint64]struct{}) (toPrune, toSave []uint64, err error)
	flush() error
}

// ---------------------------

type pointCache struct {
	isReadOnly  bool
	sharedCache *sharedInMemCache
	graphBucket diskstore.Bucket // This takes precedence over the read only bucket.
}

func NewMemPointCache() SharedPointCache {
	return &pointCache{
		sharedCache: newSharedInMemCache(),
		graphBucket: diskstore.NewMemBucket(false),
	}
}

func (pc *pointCache) IsReadOnly() bool {
	return pc.isReadOnly
}

var ErrIsDeleted = errors.New("point is deleted")

func (pc *pointCache) GetPoint(nodeId uint64) (*CachePoint, error) {
	pc.sharedCache.pointsMu.Lock()
	defer pc.sharedCache.pointsMu.Unlock()
	// ---------------------------
	if point, ok := pc.sharedCache.points[nodeId]; ok {
		if point.isDeleted {
			return nil, ErrIsDeleted
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
	if pc.isReadOnly && !readOnly {
		return fmt.Errorf("read only cache cannot be used for writing neighbours")
	}
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
			return fmt.Errorf("error getting neighbour %d: %w", edgeId, err)
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
	if pc.isReadOnly {
		return nil, fmt.Errorf("read only cache cannot be used for writing points")
	}
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

// This function is used to check if the edges of a point are valid. That is,
// are any of the nodes have edges to deletedSet.
// NOTE: This loads the entire graph into the cache.
func (pc *pointCache) EdgeScan(deleteSet map[uint64]struct{}) (toPrune, toSave []uint64, err error) {
	// ---------------------------
	/* toPrune is a list of nodes that have edges to nodes in the delete set.
	 * toSave are nodes that have no inbound edges left.
	 * For example, A -> B -> C, if B is in the delete set, A is in toPrune and C
	 * is in toSave.
	 *
	 * This is probably one of the most inefficient components of the index but
	 * it's correct. One can ignore this edge scanning business but may obtain
	 * disconnected graphs.*/
	// ---------------------------
	// We set capacity to the length of the delete set because we guess there is
	// at least one node pointing to each deleted node.
	toPrune = make([]uint64, 0, len(deleteSet))
	validNodes := make(map[uint64]struct{})
	hasInbound := make(map[uint64]struct{})
	// ---------------------------
	/* We first collect all the point ids in this bucket. Recall that some may be
	 * in cache and some may be flushed out. So we first scan the cache then go
	 * on to scan the bucket. */
	allPointIds := make(map[uint64]struct{})
	pc.sharedCache.pointsMu.Lock()
	for _, point := range pc.sharedCache.points {
		allPointIds[point.NodeId] = struct{}{}
	}
	pc.sharedCache.pointsMu.Unlock()
	pc.graphBucket.ForEach(func(k, v []byte) error {
		if k[len(k)-1] == 'v' {
			nodeId := NodeIdFromKey(k)
			allPointIds[nodeId] = struct{}{}
		}
		return nil
	})
	// ---------------------------
	for pointId := range allPointIds {
		if _, ok := deleteSet[pointId]; ok {
			continue
		}
		p, err := pc.GetPoint(pointId)
		if err != nil {
			return nil, nil, fmt.Errorf("error getting point %d during scan: %w", pointId, err)
		}
		// ---------------------------
		validNodes[pointId] = struct{}{}
		/* We now check if the neighbours of this point are in the delete set. If
		 * they are, we add this point to the toPrune list whilst also
		 * maintaining which nodes have inbound edges. */
		err = pc.WithPointNeighbours(p, true, func(neighbours []*CachePoint) error {
			addedToPrune := false
			for _, n := range neighbours {
				hasInbound[n.NodeId] = struct{}{}
				if !addedToPrune {
					_, inDeleteSet := deleteSet[n.NodeId]
					if inDeleteSet || n.isDeleted {
						toPrune = append(toPrune, p.NodeId)
						addedToPrune = true
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("error getting neighbours of point %d during scan: %w", pointId, err)
		}
		// ---------------------------
	}
	// ---------------------------
	toSave = make([]uint64, 0)
	for nodeId := range validNodes {
		if _, ok := hasInbound[nodeId]; !ok {
			toSave = append(toSave, nodeId)
		}
	}
	// ---------------------------
	return toPrune, toSave, nil
}

func (pc *pointCache) flush() error {
	if pc.isReadOnly {
		return fmt.Errorf("read only cache cannot be flushed")
	}
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
