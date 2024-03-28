package vectorstore

import (
	"fmt"

	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
)

// ---------------------------

// This function type is used to compute distances between given point Ids. For
// example, we setup a function that takes a vector []float32 and return this
// type. It mainly wraps around quantised distance functions.
type PointIdDistFn func(id uint64) float32

type VectorStore interface {
	Exists(id uint64) bool
	Set(id uint64, vector []float32) error
	Delete(id uint64) error
	// Update vector store parameters based on the data it has seen so far. The
	// vector store may ignore this call for example if it has already been
	// optimised.
	Fit() error
	DistanceFromFloat(x []float32) PointIdDistFn
	DistanceFromPoint(xId uint64) PointIdDistFn
	Flush() error
}

// ---------------------------

func New(storeType string, bucket diskstore.Bucket, distFn distance.DistFunc) (VectorStore, error) {
	switch storeType {
	case models.QuantizerNone:
		ps := plainStore{
			items:  cache.NewItemCache[plainPoint](bucket),
			distFn: distFn,
		}
		return ps, nil
	default:
		return nil, fmt.Errorf("unknown vector store type %s", storeType)
	}
}
