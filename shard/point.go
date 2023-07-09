package shard

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"go.etcd.io/bbolt"
)

type ShardPoint struct {
	models.Point
	Edges []uuid.UUID
}

var ErrNotFound = errors.New("not found")

// ---------------------------

func suffixedKey(id uuid.UUID, suffix byte) []byte {
	key := [18]byte{}
	copy(key[:], id[:])
	key[16] = '_'
	key[17] = suffix
	return key[:]
}

// ---------------------------

func (s *Shard) getPoint(b *bbolt.Bucket, id uuid.UUID) (ShardPoint, error) {
	// ---------------------------
	shardPoint := ShardPoint{Point: models.Point{Id: id}}
	// ---------------------------
	// Get vector
	vecVal := b.Get(suffixedKey(id, 'v'))
	if vecVal == nil {
		return shardPoint, fmt.Errorf("could not get vector %s", id)
	}
	shardPoint.Vector = bytesToFloat32(vecVal)
	// ---------------------------
	// Get edges
	edgeVal := b.Get(suffixedKey(id, 'e'))
	if edgeVal == nil {
		return shardPoint, fmt.Errorf("could not get edges %s", id)
	}
	shardPoint.Edges = bytesToEdgeList(edgeVal)
	// ---------------------------
	return shardPoint, nil
}

func (s *Shard) getPointNeighbours(b *bbolt.Bucket, p ShardPoint) ([]ShardPoint, error) {
	neighbours := make([]ShardPoint, len(p.Edges))
	for i, edge := range p.Edges {
		neigh, err := s.getPoint(b, edge)
		if err != nil {
			return nil, fmt.Errorf("could not get neighbour: %w", err)
		}
		neighbours[i] = neigh
	}
	return neighbours, nil
}

func (s *Shard) setPointEdges(b *bbolt.Bucket, point ShardPoint) error {
	// ---------------------------
	// Set edges
	if err := b.Put(suffixedKey(point.Id, 'e'), edgeListToBytes(point.Edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	return nil
}

func (s *Shard) setPoint(b *bbolt.Bucket, point ShardPoint) error {
	// ---------------------------
	/* Sharing suffix keys with the same point id does not work because the
	 * underlying array the slice points to gets modified and badger does not
	 * like that. For example, suffixedKey[17] = 'e' and re-use, will not work. */
	// ---------------------------
	// Set vector
	if err := b.Put(suffixedKey(point.Id, 'v'), float32ToBytes(point.Vector)); err != nil {
		return fmt.Errorf("could not set vector: %w", err)
	}
	// ---------------------------
	// Set edges
	if err := b.Put(suffixedKey(point.Id, 'e'), edgeListToBytes(point.Edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	// Set timestamp
	if point.Timestamp != 0 {
		if err := b.Put(suffixedKey(point.Id, 't'), int64ToBytes(point.Timestamp)); err != nil {
			return fmt.Errorf("could not set timestamp: %w", err)
		}
	}
	// ---------------------------
	// Set metadata if any
	if point.Metadata != nil {
		if err := b.Put(suffixedKey(point.Id, 'm'), point.Metadata); err != nil {
			return fmt.Errorf("could not set metadata: %w", err)
		}
	}
	return nil
}

func (s *Shard) getStartPoint(b *bbolt.Bucket) (ShardPoint, error) {
	// ---------------------------
	var startPoint ShardPoint
	// ---------------------------
	// Get start id
	startVal := b.Get([]byte("_sid"))
	if startVal == nil {
		return startPoint, ErrNotFound
	}
	startId, err := uuid.FromBytes(startVal)
	if err != nil {
		return startPoint, fmt.Errorf("could not get start id value: %w", err)
	}
	// ---------------------------
	startPoint, err = s.getPoint(b, startId)
	return startPoint, err
}

func (s *Shard) getOrSetStartPoint(b *bbolt.Bucket, candidate ShardPoint) (ShardPoint, error) {
	// ---------------------------
	// Get start point, set if not found
	startPoint, err := s.getStartPoint(b)
	if err == ErrNotFound {
		// ---------------------------
		if err := s.setPoint(b, candidate); err != nil {
			return candidate, fmt.Errorf("could not set start point: %w", err)
		}
		if err := b.Put([]byte("_sid"), candidate.Id[:]); err != nil {
			return candidate, fmt.Errorf("could not set start id: %w", err)
		}
		return candidate, nil
	}
	// ---------------------------
	return startPoint, err
}

func (s *Shard) getPointTimestampMetadata(b *bbolt.Bucket, id uuid.UUID) (int64, []byte, error) {
	// ---------------------------
	// Get timestamp
	timestampVal := b.Get(suffixedKey(id, 't'))
	if timestampVal == nil {
		return 0, nil, fmt.Errorf("could not get timestamp key %s", id)
	}
	timestamp := bytesToInt64(timestampVal)
	// ---------------------------
	// Get metadata
	metadataVal := b.Get(suffixedKey(id, 'm'))
	if metadataVal == nil {
		return timestamp, nil, nil
	}
	// ---------------------------
	return timestamp, metadataVal, nil
}
