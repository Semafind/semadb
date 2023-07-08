package shard

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
)

type ShardPoint struct {
	models.Point
	Edges []uuid.UUID
}

// ---------------------------

func suffixedKey(id uuid.UUID, suffix byte) []byte {
	key := [18]byte{}
	copy(key[:], id[:])
	key[16] = '_'
	key[17] = suffix
	return key[:]
}

// ---------------------------

func (s *Shard) getPoint(txn *badger.Txn, id uuid.UUID) (ShardPoint, error) {
	// ---------------------------
	shardPoint := ShardPoint{Point: models.Point{Id: id}}
	// ---------------------------
	// Get vector
	vecItem, err := txn.Get(suffixedKey(id, 'v'))
	if err != nil {
		return shardPoint, fmt.Errorf("could not get vector: %w", err)
	}
	err = vecItem.Value(func(val []byte) error {
		shardPoint.Vector = bytesToFloat32(val)
		return nil
	})
	if err != nil {
		return shardPoint, fmt.Errorf("could not get vector value: %w", err)
	}
	// ---------------------------
	// Get edges
	edgeItem, err := txn.Get(suffixedKey(id, 'e'))
	if err != nil {
		return shardPoint, fmt.Errorf("could not get edge key: %w", err)
	}
	err = edgeItem.Value(func(val []byte) error {
		shardPoint.Edges = bytesToEdgeList(val)
		return nil
	})
	if err != nil {
		return shardPoint, fmt.Errorf("could not get edge value: %w", err)
	}
	// ---------------------------
	return shardPoint, nil
}

func (s *Shard) getPointNeighbours(txn *badger.Txn, p ShardPoint) ([]ShardPoint, error) {
	neighbours := make([]ShardPoint, len(p.Edges))
	for i, edge := range p.Edges {
		neigh, err := s.getPoint(txn, edge)
		if err != nil {
			return nil, fmt.Errorf("could not get neighbour: %w", err)
		}
		neighbours[i] = neigh
	}
	return neighbours, nil
}

func (s *Shard) setPointEdges(txn *badger.Txn, point ShardPoint) error {
	// ---------------------------
	// Set edges
	if err := txn.Set(suffixedKey(point.Id, 'e'), edgeListToBytes(point.Edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	return nil
}

func (s *Shard) setPoint(txn *badger.Txn, point ShardPoint) error {
	// ---------------------------
	/* Sharing suffix keys with the same point id does not work because the
	 * underlying array the slice points to gets modified and badger does not
	 * like that. For example, suffixedKey[17] = 'e' and re-use, will not work. */
	// ---------------------------
	// Set vector
	if err := txn.Set(suffixedKey(point.Id, 'v'), float32ToBytes(point.Vector)); err != nil {
		return fmt.Errorf("could not set vector: %w", err)
	}
	// ---------------------------
	// Set edges
	if err := txn.Set(suffixedKey(point.Id, 'e'), edgeListToBytes(point.Edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	// Set timestamp
	if point.Timestamp != 0 {
		if err := txn.Set(suffixedKey(point.Id, 't'), int64ToBytes(point.Timestamp)); err != nil {
			return fmt.Errorf("could not set timestamp: %w", err)
		}
	}
	// ---------------------------
	// Set metadata if any
	if point.Metadata != nil {
		if err := txn.Set(suffixedKey(point.Id, 'm'), point.Metadata); err != nil {
			return fmt.Errorf("could not set metadata: %w", err)
		}
	}
	return nil
}

func (s *Shard) getOrSetStartPoint(txn *badger.Txn, candidate ShardPoint) (ShardPoint, error) {
	// ---------------------------
	// Get start id
	startItem, err := txn.Get([]byte("_sid"))
	if err == badger.ErrKeyNotFound {
		// ---------------------------
		if err := s.setPoint(txn, candidate); err != nil {
			return candidate, fmt.Errorf("could not set start point: %w", err)
		}
		if err := txn.Set([]byte("_sid"), candidate.Id[:]); err != nil {
			return candidate, fmt.Errorf("could not set start id: %w", err)
		}
		return candidate, nil
	}
	if err != nil {
		return candidate, fmt.Errorf("could not get start id: %w", err)
	}
	var startId uuid.UUID
	err = startItem.Value(func(val []byte) error {
		startId, err = uuid.FromBytes(val)
		return err
	})
	if err != nil {
		return candidate, fmt.Errorf("could not get start id value: %w", err)
	}
	// ---------------------------
	startPoint, err := s.getPoint(txn, startId)
	return startPoint, err
}
