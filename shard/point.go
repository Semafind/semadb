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

func getPoint(b *bbolt.Bucket, id uuid.UUID) (ShardPoint, error) {
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

func setPointEdges(b *bbolt.Bucket, point ShardPoint) error {
	// ---------------------------
	// Set edges
	if err := b.Put(suffixedKey(point.Id, 'e'), edgeListToBytes(point.Edges)); err != nil {
		return fmt.Errorf("could not set edge: %w", err)
	}
	// ---------------------------
	return nil
}

func setPoint(b *bbolt.Bucket, point ShardPoint) error {
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
	// Set metadata if any
	if point.Metadata != nil {
		if err := b.Put(suffixedKey(point.Id, 'm'), point.Metadata); err != nil {
			return fmt.Errorf("could not set metadata: %w", err)
		}
	}
	return nil
}

func deletePoint(b *bbolt.Bucket, id uuid.UUID) error {
	// ---------------------------
	// Delete vector
	if err := b.Delete(suffixedKey(id, 'v')); err != nil {
		return fmt.Errorf("could not delete vector: %w", err)
	}
	// ---------------------------
	// Delete edges
	if err := b.Delete(suffixedKey(id, 'e')); err != nil {
		return fmt.Errorf("could not delete edges: %w", err)
	}
	// ---------------------------
	// Delete metadata
	if err := b.Delete(suffixedKey(id, 'm')); err != nil {
		return fmt.Errorf("could not delete metadata: %w", err)
	}
	// ---------------------------
	return nil
}

func getPointMetadata(b *bbolt.Bucket, id uuid.UUID) ([]byte, error) {
	// ---------------------------
	// Get metadata
	metadataVal := b.Get(suffixedKey(id, 'm'))
	// ---------------------------
	return metadataVal, nil
}
