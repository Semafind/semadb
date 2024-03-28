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
	items  *cache.ItemCache[plainPoint]
	distFn distance.DistFunc
}

func (ps plainStore) Exists(id uint64) bool {
	_, err := ps.items.Get(id)
	return err == nil
}

func (ps plainStore) Set(id uint64, vector []float32) error {
	return ps.items.Put(id, plainPoint{
		Id:     id,
		Vector: vector,
	})
}

func (ps plainStore) Delete(id uint64) error {
	return ps.items.Delete(id)

}

func (ps plainStore) Fit() error {
	return nil
}

func (ps plainStore) DistanceFromFloat(x []float32) PointIdDistFn {
	return func(id uint64) float32 {
		point, err := ps.items.Get(id)
		if err != nil {
			log.Warn().Err(err).Uint64("id", id).Msg("point not found for distance calculation")
			return math.MaxFloat32
		}
		return ps.distFn(x, point.Vector)
	}
}

func (ps plainStore) DistanceFromPoint(xId uint64) PointIdDistFn {
	a, errA := ps.items.Get(xId)
	return func(id uint64) float32 {
		b, errB := ps.items.Get(id)
		if errA != nil || errB != nil {
			log.Warn().AnErr("errA", errA).AnErr("errB", errB).Uint64("idA", xId).Uint64("idB", id).Msg("point not found for distance calculation")
			return math.MaxFloat32
		}
		return ps.distFn(a.Vector, b.Vector)
	}
}

func (ps plainStore) Flush() error {
	return ps.items.Flush()
}

type plainPoint struct {
	Id     uint64
	Vector []float32
}

func (pp plainPoint) IdFromKey(key []byte) (uint64, bool) {
	return conversion.NodeIdFromKey(key, 'v')
}

// Always returns false as we don't track dirty state.
func (pp plainPoint) CheckAndClearDirty() bool {
	return false
}

func (pp plainPoint) ReadFrom(id uint64, bucket diskstore.Bucket) (point plainPoint, err error) {
	point.Id = id
	vectorBytes := bucket.Get(conversion.NodeKey(id, 'v'))
	if vectorBytes == nil {
		err = cache.ErrNotFound
		return
	}
	point.Vector = conversion.BytesToFloat32(vectorBytes)
	return
}

func (pp plainPoint) WriteTo(bucket diskstore.Bucket) error {
	if err := bucket.Put(conversion.NodeKey(pp.Id, 'v'), conversion.Float32ToBytes(pp.Vector)); err != nil {
		return fmt.Errorf("could not write plain point vector: %w", err)
	}
	return nil
}
func (pp plainPoint) DeleteFrom(bucket diskstore.Bucket) error {
	if err := bucket.Delete(conversion.NodeKey(pp.Id, 'v')); err != nil {
		return fmt.Errorf("could not delete plain point vector: %w", err)
	}
	return nil
}
