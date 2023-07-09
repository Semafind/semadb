package shard

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/semafind/semadb/models"
	"go.etcd.io/bbolt"
)

type Shard struct {
	db         *bbolt.DB
	collection models.Collection
	closeCount int
	closeLock  sync.Mutex
}

func NewShard(shardDir string, collection models.Collection) (*Shard, error) {
	// dbDir := filepath.Join(shardDir, "db")
	// opts := badger.DefaultOptions(dbDir).WithLogger(nil)
	db, err := bbolt.Open(filepath.Join(shardDir, "db"), 0666, nil)
	if err != nil {
		return nil, fmt.Errorf("could not open shard db: %w", err)
	}
	db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("points"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	return &Shard{
		db:         db,
		collection: collection,
	}, nil
}

func (s *Shard) changeLockCount(delta int) {
	s.closeLock.Lock()
	defer s.closeLock.Unlock()
	s.closeCount += delta
	if s.closeCount <= 0 {
		s.closeCount = 0
		s.closeLock.Unlock()
		s.Close()
		s.closeLock.Lock()
	}
}

func (s *Shard) Close() error {
	s.closeLock.Lock()
	defer s.closeLock.Unlock()
	return s.db.Close()
}

// ---------------------------

func (s *Shard) insertPoint(b *bbolt.Bucket, startPoint ShardPoint, point ShardPoint) error {
	// ---------------------------
	_, visitedSet, err := s.greedySearch(b, startPoint, point.Vector, 1, s.collection.Parameters.SearchSize)
	if err != nil {
		return fmt.Errorf("could not greedy search: %w", err)
	}
	// ---------------------------
	s.robustPrune(&point, visitedSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
	// ---------------------------
	// Add the bi-directional edges
	for _, nId := range point.Edges {
		n, err := s.getPoint(b, nId)
		if err != nil {
			return fmt.Errorf("could not get neighbour point: %w", err)
		}
		if len(n.Edges)+1 > s.collection.Parameters.DegreeBound {
			// ---------------------------
			// We need to prune the neighbour as well to keep the degree bound
			nn, err := s.getPointNeighbours(b, n)
			if err != nil {
				return fmt.Errorf("could not get neighbour neighbours: %w", err)
			}
			candidateSet := NewDistSet(n.Vector, len(n.Edges)+1, s.dist)
			candidateSet.AddPoint(nn...)
			candidateSet.AddPoint(point)
			candidateSet.Sort()
			s.robustPrune(&n, candidateSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
		} else {
			// ---------------------------
			// Add the edge
			n.Edges = append(n.Edges, point.Id)
		}
		if err := s.setPointEdges(b, n); err != nil {
			return fmt.Errorf("could not set neighbour point: %w", err)
		}
	}
	// ---------------------------
	if err := s.setPoint(b, point); err != nil {
		return fmt.Errorf("could not set point: %w", err)
	}
	// ---------------------------
	return nil
}

// ---------------------------

func (s *Shard) UpsertPoints(points []models.Point) (map[uuid.UUID]error, error) {
	// ---------------------------
	log.Debug().Str("component", "shard").Int("count", len(points)).Msg("UpsertPoints")
	// ---------------------------
	// This is to stop the shard from being closed while we're writing to it
	s.changeLockCount(1)
	// ---------------------------
	// Get start point
	var startPoint ShardPoint
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("points"))
		var err error
		candidate := ShardPoint{Point: points[0]}
		startPoint, err = s.getOrSetStartPoint(b, candidate)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("could not get start point: %w", err)
	}
	// ---------------------------
	// Upsert points
	results := make(map[uuid.UUID]error)
	bar := progressbar.Default(int64(len(points)))
	pointC := make(chan models.Point)
	// Start workers
	for i := 0; i < 8; i++ {
		go func() {
			for point := range pointC {
				// ---------------------------
				// Write point to shard
				err := s.db.Batch(func(tx *bbolt.Tx) error {
					b := tx.Bucket([]byte("points"))
					return s.insertPoint(b, startPoint, ShardPoint{Point: point})
				})
				if err != nil {
					log.Debug().Err(err).Msg("could not insert point")
					results[point.Id] = err
				}
				bar.Add(1)
				// ---------------------------
			}
		}()
	}
	for _, point := range points {
		// ---------------------------
		// Write point to shard
		pointC <- point
		// err := s.db.Update(func(tx *bbolt.Tx) error {
		// 	bar.Add(1)
		// 	b := tx.Bucket([]byte("points"))
		// 	return s.insertPoint(b, startPoint, ShardPoint{Point: point})
		// })
		// if err != nil {
		// 	log.Debug().Err(err).Msg("could not insert point")
		// 	results[point.Id] = err
		// }
		// ---------------------------
	}
	close(pointC)
	bar.Close()
	// ---------------------------
	return results, nil
}

// ---------------------------

func (s *Shard) SearchPoints(query []float32, k int) ([]models.Point, error) {
	// ---------------------------
	// Get start point
	var startPoint ShardPoint
	err := s.db.Update(func(tx *bbolt.Tx) error {
		var err error
		startPoint, err = s.getStartPoint(tx.Bucket([]byte("points")))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("could not get start point: %w", err)
	}
	// ---------------------------
	// Perform search
	var searchSet DistSet
	err = s.db.View(func(tx *bbolt.Tx) error {
		var err error
		b := tx.Bucket([]byte("points"))
		searchSet, _, err = s.greedySearch(b, startPoint, query, k, s.collection.Parameters.SearchSize)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("could not perform search: %w", err)
	}
	// ---------------------------
	// Clean up results and backfill metadata
	searchSet.KeepFirstK(k)
	results := make([]models.Point, len(searchSet.items))
	err = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("points"))
		for i, distElem := range searchSet.items {
			results[i] = distElem.Point
			timestamp, mdata, err := s.getPointTimestampMetadata(b, distElem.Point.Id)
			if err != nil {
				return fmt.Errorf("could not get point timestamp, metadata: %w", err)
			}
			results[i].Timestamp = timestamp
			results[i].Metadata = mdata
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not get point metadata: %w", err)
	}
	return results, nil
}
