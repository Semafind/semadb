package shard

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/semafind/semadb/models"
)

type Shard struct {
	db         *badger.DB
	collection models.Collection
	closeCount int
	closeLock  sync.Mutex
}

func NewShard(shardDir string, collection models.Collection) (*Shard, error) {
	dbDir := filepath.Join(shardDir, "db")
	opts := badger.DefaultOptions(dbDir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("could not open shard db: %w", err)
	}
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

func (s *Shard) insertPoint(txn *badger.Txn, startPointId uuid.UUID, point ShardPoint) error {
	// ---------------------------
	sp, err := s.getPoint(txn, startPointId)
	if err != nil {
		return fmt.Errorf("could not get start point: %w", err)
	}
	// ---------------------------
	_, visitedSet, err := s.greedySearch(txn, sp, point.Vector, 1, s.collection.Parameters.SearchSize)
	if err != nil {
		return fmt.Errorf("could not greedy search: %w", err)
	}
	// ---------------------------
	s.robustPrune(&point, visitedSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
	// ---------------------------
	// Add the bi-directional edges
	for _, nId := range point.Edges {
		n, err := s.getPoint(txn, nId)
		if err != nil {
			return fmt.Errorf("could not get neighbour point: %w", err)
		}
		if len(n.Edges)+1 > s.collection.Parameters.DegreeBound {
			// ---------------------------
			// We need to prune the neighbour as well to keep the degree bound
			nn, err := s.getPointNeighbours(txn, n)
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
		if err := s.setPointEdges(txn, n); err != nil {
			return fmt.Errorf("could not set neighbour point: %w", err)
		}
	}
	// ---------------------------
	if err := s.setPoint(txn, point); err != nil {
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
	// Does the points exist?
	pointExists := make(map[uuid.UUID]struct{})
	err := s.db.View(func(txn *badger.Txn) error {
		for _, point := range points {
			if _, err := txn.Get(point.Id[:]); err != nil {
				pointExists[point.Id] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not check if points exist: %w", err)
	}
	// ---------------------------
	// Get start point
	var startPoint ShardPoint
	err = s.db.Update(func(txn *badger.Txn) error {
		candidate := ShardPoint{Point: points[0]}
		startPoint, err = s.getOrSetStartPoint(txn, candidate)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("could not get start point: %w", err)
	}
	// ---------------------------
	// Upsert points
	results := make(map[uuid.UUID]error)
	bar := progressbar.Default(int64(len(points)))
	for _, point := range points {
		// ---------------------------
		// Write point to shard
		err := s.db.Update(func(txn *badger.Txn) error {
			return s.insertPoint(txn, startPoint.Id, ShardPoint{Point: point})
		})
		if err != nil {
			log.Debug().Err(err).Msg("could not insert point")
			results[point.Id] = err
		}
		bar.Add(1)
		// ---------------------------
	}
	bar.Close()
	// ---------------------------
	return results, nil
}

// ---------------------------

func (s *Shard) SearchPoints(query []float32, k int) ([]models.Point, error) {
	// ---------------------------
	// Perform search
	var searchSet DistSet
	err := s.db.View(func(txn *badger.Txn) error {
		startPoint, err := s.getStartPoint(txn)
		if err != nil {
			return fmt.Errorf("could not get start point: %w", err)
		}
		searchSet, _, err = s.greedySearch(txn, startPoint, query, k, s.collection.Parameters.SearchSize)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("could not perform search: %w", err)
	}
	// ---------------------------
	// Clean up results and backfill metadata
	searchSet.KeepFirstK(k)
	results := make([]models.Point, len(searchSet.items))
	err = s.db.View(func(txn *badger.Txn) error {
		for i, distElem := range searchSet.items {
			results[i] = distElem.Point
			timestamp, mdata, err := s.getPointTimestampMetadata(txn, distElem.Point.Id)
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
