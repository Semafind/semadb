package shard

import (
	"sync"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

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
	cp.Edges = append(cp.Edges, neighbour.Id)
	cp.neighbours = append(cp.neighbours, neighbour)
	cp.isEdgeDirty = true
	return len(cp.Edges)
}

// ---------------------------

type PointCache struct {
	bucket *bbolt.Bucket
	points map[uuid.UUID]*CachePoint
	mu     sync.Mutex
}

func NewPointCache(bucket *bbolt.Bucket) *PointCache {
	return &PointCache{
		bucket: bucket,
		points: make(map[uuid.UUID]*CachePoint),
	}
}

func (pc *PointCache) GetPoint(pointId uuid.UUID) (*CachePoint, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if point, ok := pc.points[pointId]; ok {
		return point, nil
	}
	point, err := getPoint(pc.bucket, pointId)
	if err != nil {
		return nil, err
	}
	newPoint := &CachePoint{
		ShardPoint: point,
	}
	pc.points[pointId] = newPoint
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

func (pc *PointCache) SetPoint(point ShardPoint) *CachePoint {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	newPoint := &CachePoint{
		ShardPoint: point,
		isDirty:    true,
	}
	pc.points[point.Id] = newPoint
	return newPoint
}

func (pc *PointCache) EdgeScan(deleteSet map[uuid.UUID]struct{}) ([]uuid.UUID, error) {
	return scanPointEdges(pc.bucket, deleteSet)
}

func (pc *PointCache) Flush() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, point := range pc.points {
		if point.isDeleted {
			if err := deletePoint(pc.bucket, point.Id); err != nil {
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
