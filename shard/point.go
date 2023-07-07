package shard

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack/v5"
)

type ShardPoint struct {
	models.Point
	Edges []uuid.UUID
}

// ---------------------------

func (s *Shard) getPoint(txn *badger.Txn, id uuid.UUID) (ShardPoint, error) {
	// ---------------------------
	shardPoint := ShardPoint{Point: models.Point{Id: id}}
	// ---------------------------
	// Get vector
	suffixedKey := [18]byte{}
	copy(suffixedKey[:], id[:])
	suffixedKey[16] = '_'
	suffixedKey[17] = 'v'
	vecItem, err := txn.Get(suffixedKey[:])
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
	suffixedKey[17] = 'e'
	edgeItem, err := txn.Get(suffixedKey[:])
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
	suffixedKey := [18]byte{}
	copy(suffixedKey[:], point.Id[:])
	suffixedKey[16] = '_'
	suffixedKey[17] = 'e'
	if err := txn.Set(suffixedKey[:], edgeListToBytes(point.Edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	return nil
}

func (s *Shard) setPoint(txn *badger.Txn, point ShardPoint) error {
	// ---------------------------
	// Set vector
	suffixedKey := [18]byte{}
	copy(suffixedKey[:], point.Id[:])
	suffixedKey[16] = '_'
	suffixedKey[17] = 'v'
	if err := txn.Set(suffixedKey[:], float32ToBytes(point.Vector)); err != nil {
		return fmt.Errorf("could not set vector: %w", err)
	}
	// ---------------------------
	// Set edges
	suffixedKey[17] = 'e'
	if err := txn.Set(suffixedKey[:], edgeListToBytes(point.Edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	// Set timestamp
	if point.Timestamp != 0 {
		suffixedKey[17] = 't'
		if err := txn.Set(suffixedKey[:], int64ToBytes(point.Timestamp)); err != nil {
			return fmt.Errorf("could not set timestamp: %w", err)
		}
	}
	// ---------------------------
	// Set metadata if any
	if point.Metadata != nil {
		suffixedKey[17] = 'm'
		mBytes, err := msgpack.Marshal(point.Metadata)
		if err != nil {
			return fmt.Errorf("could not marshal metadata: %w", err)
		}
		if err := txn.Set(suffixedKey[:], mBytes); err != nil {
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
