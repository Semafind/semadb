package shard

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
	neighbours  []*CachePoint
	mu          sync.Mutex
}

func (cp *CachePoint) ClearNeighbours() {
	cp.Edges = cp.Edges[:0]
	cp.neighbours = cp.neighbours[:0]
	cp.isEdgeDirty = true
}

func (cp *CachePoint) AddNeighbour(neighbour *CachePoint) int {
	cp.Edges = append(cp.Edges, neighbour.NodeId)
	cp.neighbours = append(cp.neighbours, neighbour)
	cp.isEdgeDirty = true
	return len(cp.Edges)
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

func (pc *PointCache) GetPointNeighbours(point *CachePoint) ([]*CachePoint, error) {
	if point.neighbours != nil {
		return point.neighbours, nil
	}
	neighbours := make([]*CachePoint, 0, len(point.Edges))
	for _, edgeId := range point.Edges {
		edge, err := pc.GetPoint(edgeId)
		if err != nil {
			return nil, err
		}
		neighbours = append(neighbours, edge)
	}
	point.neighbours = neighbours
	return point.neighbours, nil
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
