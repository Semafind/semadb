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
	neighbours  []*CachePoint
	mu          sync.Mutex
}

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
	point.mu.Lock()
	defer point.mu.Unlock()
	if point.neighbours != nil {
		return point.neighbours, nil
	}
	point.neighbours = make([]*CachePoint, len(point.Edges))
	for i, edgeId := range point.Edges {
		edge, err := pc.GetPoint(edgeId)
		if err != nil {
			return nil, err
		}
		point.neighbours[i] = edge
	}
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

func (pc *PointCache) AddNeighbour(point *CachePoint, neighbour *CachePoint) {
	point.mu.Lock()
	defer point.mu.Unlock()
	point.Edges = append(point.Edges, neighbour.Id)
	point.neighbours = append(point.neighbours, neighbour)
	point.isEdgeDirty = true
}

func (pc *PointCache) Flush() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, point := range pc.points {
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
