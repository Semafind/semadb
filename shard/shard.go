package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"go.etcd.io/bbolt"
)

type Shard struct {
	db         *bbolt.DB
	collection models.Collection
	distFn     distance.DistFunc
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
	// ---------------------------
	distFn := distance.EuclideanDistance
	if collection.DistMetric == "cosine" {
		distFn = distance.CosineDistance
	}
	// ---------------------------
	return &Shard{
		db:         db,
		collection: collection,
		distFn:     distFn,
	}, nil
}

func (s *Shard) Close() error {
	return s.db.Close()
}

// ---------------------------

func (s *Shard) insertPoint(pc *PointCache, startPointId uuid.UUID, shardPoint ShardPoint) error {
	// ---------------------------
	sp, err := pc.GetPoint(startPointId)
	if err != nil {
		return fmt.Errorf("could not get start point: %w", err)
	}
	// ---------------------------
	point := pc.SetPoint(shardPoint)
	// ---------------------------
	_, visitedSet, err := s.greedySearch(pc, sp, point.Vector, 1, s.collection.Parameters.SearchSize)
	if err != nil {
		return fmt.Errorf("could not greedy search: %w", err)
	}
	// ---------------------------
	s.robustPrune(point, visitedSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
	// ---------------------------
	// Add the bi-directional edges
	for _, nId := range point.Edges {
		n, err := pc.GetPoint(nId)
		if err != nil {
			return fmt.Errorf("could not get neighbour point: %w", err)
		}
		if len(n.Edges)+1 > s.collection.Parameters.DegreeBound {
			// ---------------------------
			// We need to prune the neighbour as well to keep the degree bound
			nn, err := pc.GetPointNeighbours(n)
			if err != nil {
				return fmt.Errorf("could not get neighbour neighbours: %w", err)
			}
			candidateSet := NewDistSet(n.Vector, len(n.Edges)+1, s.distFn)
			candidateSet.AddPoint(nn...)
			candidateSet.AddPoint(point)
			candidateSet.Sort()
			s.robustPrune(n, candidateSet, s.collection.Parameters.Alpha, s.collection.Parameters.DegreeBound)
		} else {
			// ---------------------------
			// Add the edge
			pc.AddNeighbour(n, point)
		}
		// if err := setPointEdges(b, n); err != nil {
		// 	return fmt.Errorf("could not set neighbour point: %w", err)
		// }
	}
	// ---------------------------
	return nil
}

// ---------------------------

func (s *Shard) UpsertPoints(points []models.Point) (map[uuid.UUID]error, error) {
	// ---------------------------
	if config.Cfg.Debug {
		profileFile, _ := os.Create("dump/cpu.prof")
		defer profileFile.Close()
		pprof.StartCPUProfile(profileFile)
		defer pprof.StopCPUProfile()
	}
	// ---------------------------
	log.Debug().Str("component", "shard").Int("count", len(points)).Msg("UpsertPoints")
	// ---------------------------
	// Get start point
	var startPoint ShardPoint
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("points"))
		var err error
		candidate := ShardPoint{Point: points[0]}
		startPoint, err = getOrSetStartPoint(b, candidate)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("could not get start point: %w", err)
	}
	// ---------------------------
	// Upsert points
	results := make(map[uuid.UUID]error)
	bar := progressbar.Default(int64(len(points)))
	// ---------------------------
	// Write points to shard
	err = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("points"))
		pc := NewPointCache(b)
		// ---------------------------
		for _, point := range points {
			if err := s.insertPoint(pc, startPoint.Id, ShardPoint{Point: point}); err != nil {
				log.Debug().Err(err).Msg("could not insert point")
				return fmt.Errorf("could not insert point: %w", err)
			}
			bar.Add(1)
		}
		// ---------------------------
		currentTime := time.Now()
		err := pc.Flush()
		log.Debug().Str("component", "shard").Str("duration", time.Since(currentTime).String()).Msg("UpsertPoints - Flush")
		return err
	})
	if err != nil {
		log.Debug().Err(err).Msg("could not insert point")
		return nil, fmt.Errorf("could not insert point: %w", err)
	}
	// ---------------------------
	bar.Close()
	// ---------------------------
	return results, nil
}

// ---------------------------

func (s *Shard) SearchPoints(query []float32, k int) ([]models.Point, error) {
	// ---------------------------
	// Perform search
	var searchSet DistSet
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("points"))
		startPoint, err := getStartPoint(b)
		if err != nil {
			return fmt.Errorf("could not get start point: %w", err)
		}
		pc := NewPointCache(b)
		sp, err := pc.GetPoint(startPoint.Id)
		if err != nil {
			return fmt.Errorf("could not get start point: %w", err)
		}
		searchSet, _, err = s.greedySearch(pc, sp, query, k, s.collection.Parameters.SearchSize)
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
			results[i] = distElem.point.Point
			timestamp, mdata, err := getPointTimestampMetadata(b, distElem.point.Id)
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
