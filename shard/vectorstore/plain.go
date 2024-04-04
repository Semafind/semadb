package vectorstore

import (
	"fmt"
	"math"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/shard/cache"
)

/* Stores vectors as they are with no quantization. This is the basic vector
 * store option. */
type plainStore struct {
	items  *cache.ItemCache[uint64, plainPoint]
	distFn distance.DistFunc
}

func (ps plainStore) Exists(id uint64) bool {
	_, err := ps.items.Get(id)
	return err == nil
}

func (ps plainStore) Get(ids ...uint64) ([]VectorStorePoint, error) {
	points, err := ps.items.GetMany(ids...)
	if err != nil {
		return nil, err
	}
	// Amazing casting here, why not just return points, nil? Oh well, it's not
	// the same type.
	ret := make([]VectorStorePoint, len(points))
	for i, p := range points {
		ret[i] = p
	}
	return ret, nil
}

func (ps plainStore) SizeInMemory() int64 {
	return ps.items.SizeInMemory()
}

func (ps plainStore) UpdateBucket(bucket diskstore.Bucket) {
	ps.items.UpdateBucket(bucket)
}

func (ps plainStore) Set(id uint64, vector []float32) (VectorStorePoint, error) {
	point := plainPoint{
		id:     id,
		Vector: vector,
	}
	return point, ps.items.Put(id, point)
}

func (ps plainStore) Delete(ids ...uint64) error {
	return ps.items.Delete(ids...)

}

func (ps plainStore) Fit() error {
	return nil
}

func (ps plainStore) DistanceFromFloat(x []float32) PointIdDistFn {
	return func(y VectorStorePoint) float32 {
		point, ok := y.(plainPoint)
		if !ok {
			log.Warn().Uint64("id", y.Id()).Msg("point not found for distance calculation")
			return math.MaxFloat32
		}
		return ps.distFn(x, point.Vector)
	}
}

func (ps plainStore) DistanceFromPoint(x VectorStorePoint) PointIdDistFn {
	pointX, okX := x.(plainPoint)
	return func(y VectorStorePoint) float32 {
		pointY, okY := y.(plainPoint)
		if !okX || !okY {
			log.Warn().Uint64("idX", x.Id()).Uint64("idY", y.Id()).Msg("point not found for distance calculation")
			return math.MaxFloat32
		}
		return ps.distFn(pointX.Vector, pointY.Vector)
	}
}

func (ps plainStore) Flush() error {
	return ps.items.Flush()
}

type plainPoint struct {
	id     uint64
	Vector []float32
}

func (pp plainPoint) Id() uint64 {
	return pp.id
}

func (pp plainPoint) IdFromKey(key []byte) (uint64, bool) {
	return conversion.NodeIdFromKey(key, 'v')
}

func (pp plainPoint) SizeInMemory() int64 {
	return int64(8 + 4*len(pp.Vector))
}

// Always returns false as we don't track dirty state.
func (pp plainPoint) CheckAndClearDirty() bool {
	return false
}

func (pp plainPoint) ReadFrom(id uint64, bucket diskstore.Bucket) (point plainPoint, err error) {
	point.id = id
	vectorBytes := bucket.Get(conversion.NodeKey(id, 'v'))
	if vectorBytes == nil {
		err = cache.ErrNotFound
		return
	}
	point.Vector = conversion.BytesToFloat32(vectorBytes)
	return
}

func (pp plainPoint) WriteTo(id uint64, bucket diskstore.Bucket) error {
	if err := bucket.Put(conversion.NodeKey(id, 'v'), conversion.Float32ToBytes(pp.Vector)); err != nil {
		return fmt.Errorf("could not write plain point vector: %w", err)
	}
	return nil
}
func (pp plainPoint) DeleteFrom(id uint64, bucket diskstore.Bucket) error {
	if err := bucket.Delete(conversion.NodeKey(id, 'v')); err != nil {
		return fmt.Errorf("could not delete plain point vector: %w", err)
	}
	return nil
}
