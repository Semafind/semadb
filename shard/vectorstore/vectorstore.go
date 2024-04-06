package vectorstore

import (
	"fmt"

	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
)

// ---------------------------

type VectorStorePoint interface {
	Id() uint64
}

// This function type is used to compute distances between given point Ids. For
// example, we setup a function that takes a vector []float32 and return this
// type. It mainly wraps around quantised distance functions.
type PointIdDistFn func(y VectorStorePoint) float32

type VectorStore interface {
	Exists(id uint64) bool
	Get(id uint64) (VectorStorePoint, error)
	GetMany(ids ...uint64) ([]VectorStorePoint, error)
	Set(id uint64, vector []float32) (VectorStorePoint, error)
	Delete(ids ...uint64) error
	cache.Cachable
	// Update bucket can be used to swap out the underlying disk storage. This
	// is useful for cached items for which the underlying storage has changed.
	// For example, we read and cache items, but the bucket is no longer valid.
	// During a new request we want to reuse the cache with the new bucket.
	UpdateBucket(bucket diskstore.Bucket)
	// Update vector store parameters based on the data it has seen so far. The
	// vector store may ignore this call for example if it has already been
	// optimised.
	Fit() error
	DistanceFromFloat(x []float32) PointIdDistFn
	DistanceFromPoint(x VectorStorePoint) PointIdDistFn
	Flush() error
}

// ---------------------------

func New(params *models.Quantizer, bucket diskstore.Bucket, distFnName string, vectorLength int) (VectorStore, error) {
	distFn, err := distance.GetDistanceFn(distFnName)
	if err != nil {
		return nil, err
	}
	if params == nil || params.Type == models.QuantizerNone {
		ps := plainStore{
			items:  cache.NewItemCache[uint64, plainPoint](bucket),
			distFn: distFn,
		}
		return ps, nil
	}
	switch params.Type {
	case models.QuantizerBinary:
		if params.Binary == nil {
			return nil, fmt.Errorf("binary quantizer parameters are nil")
		}
		return newBinaryQuantizer(bucket, distFn, *params.Binary), nil
	case models.QuantizerProduct:
		if params.Product == nil {
			return nil, fmt.Errorf("product quantizer parameters are nil")
		}
		return newProductQuantizer(bucket, distFnName, *params.Product, vectorLength)
	}
	return nil, fmt.Errorf("unknown vector store type %T", params.Type)
}
