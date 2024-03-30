package vectorstore

import (
	"math"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
)

const binaryQuantizerThresholdKey = "_binaryQuantizerThreshold"

/* Stores vectors as a binary array to save storage. These binary arrays are
 * actually stored as uint64s to align with the CPU architecture.
 *
 * The quantiser can be trigger after a certain number of vectors have been
 * added. This is useful for example if we want to set the threshold to the
 * median or mean value of the vectors.
 */
type binaryQuantizer struct {
	// The threshold value for the binary quantizer. It is a pointer to distinguish
	// between 0 value vs not set.
	threshold *float32
	params    models.BinaryQuantizerParamaters
	items     *cache.ItemCache[*binaryQuantizedPoint]
	bucket    diskstore.Bucket
	distFn    distance.DistFunc
}

func newBinaryQuantizer(bucket diskstore.Bucket, distFn distance.DistFunc, params models.BinaryQuantizerParamaters) *binaryQuantizer {
	bq := &binaryQuantizer{
		items:     cache.NewItemCache[*binaryQuantizedPoint](bucket),
		params:    params,
		distFn:    distFn,
		threshold: params.Threshold,
		bucket:    bucket,
	}
	if bq.threshold == nil {
		// Check storage
		floatBytes := bucket.Get([]byte(binaryQuantizerThresholdKey))
		if floatBytes != nil {
			t := conversion.BytesToSingleFloat32(floatBytes)
			bq.threshold = &t
		}
	}
	return bq
}

func (bq *binaryQuantizer) Exists(id uint64) bool {
	_, err := bq.items.Get(id)
	return err == nil
}

func (bq *binaryQuantizer) encode(vector []float32) []uint64 {
	if bq.threshold == nil {
		return nil
	}
	// How many uint64s do we need?
	numUint64s := len(vector) / 64
	if len(vector)%64 != 0 {
		numUint64s++
	}
	encoded := make([]uint64, numUint64s)
	/* Our goal here is to convert the float32 vector into a binary vector. We
	 * do this by setting the bit at position i in the binary vector to 1 if the
	 * value at position i in the float32 vector is greater than the threshold.
	 *
	 * For example: if the threshold is 0.5 and the float32 vector is [0.1, 0.6,
	 * 0.7, 0.4], the binary vector would be [0, 1, 1, 0].
	 *
	 * This is then encoded into a uint64 array where each uint64 represents 64
	 * bits of the binary vector.
	 */
	for i, v := range vector {
		if v > *bq.threshold {
			encoded[i/64] |= 1 << (63 - (i % 64))
		}
	}
	return encoded
}

func (bq *binaryQuantizer) Set(id uint64, vector []float32) error {
	point := &binaryQuantizedPoint{
		Id:           id,
		Vector:       vector,
		BinaryVector: bq.encode(vector),
	}
	return bq.items.Put(id, point)
}

func (bq *binaryQuantizer) Delete(id uint64) error {
	return bq.items.Delete(id)
}

func (bq *binaryQuantizer) Fit() error {
	// Have we already fitted the quantizer or are there enough points to fit it? The short-circuiting
	// here is important to avoid unnecessary work of counting the items.
	if bq.threshold != nil || bq.items.Count() < bq.params.TriggerThreshold {
		return nil
	}
	// ---------------------------
	/* Time to fit. We are doing two passes. First pass computes the mean of the
	 * vectors. The second pass encodes the vectors. */
	count := 0
	sum := float32(0)
	startTime := time.Now()
	err := bq.items.ForEach(func(id uint64, point *binaryQuantizedPoint) error {
		for _, v := range point.Vector {
			sum += v
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}
	threshold := sum / float32(count)
	bq.threshold = &threshold
	// ---------------------------
	// Second pass to encode
	err = bq.items.ForEach(func(id uint64, point *binaryQuantizedPoint) error {
		point.BinaryVector = bq.encode(point.Vector)
		point.isDirty = true
		return nil
	})
	log.Debug().Dur("duration", time.Since(startTime)).Float32("threshold", threshold).Msg("fitted binary quantizer")
	// ---------------------------
	return err

}

func (bq *binaryQuantizer) DistanceFromFloat(x []float32) PointIdDistFn {
	if bq.threshold != nil {
		encodedX := bq.encode(x)
		return func(id uint64) float32 {
			point, err := bq.items.Get(id)
			if err != nil {
				log.Warn().Err(err).Uint64("id", id).Msg("point not found for distance calculation")
				return math.MaxFloat32
			}
			encodedY := point.BinaryVector
			return distance.HammingDistance(encodedX, encodedY)
		}
	}
	/* Here we fall back to the original vector if the threshold is not set. */
	return func(id uint64) float32 {
		point, err := bq.items.Get(id)
		if err != nil {
			log.Warn().Err(err).Uint64("id", id).Msg("point not found for distance calculation")
			return math.MaxFloat32
		}
		return bq.distFn(x, point.Vector)
	}
}

func (bq *binaryQuantizer) DistanceFromPoint(xId uint64) PointIdDistFn {
	if bq.threshold != nil {
		a, errA := bq.items.Get(xId)
		return func(id uint64) float32 {
			b, errB := bq.items.Get(id)
			if errA != nil || errB != nil {
				log.Warn().AnErr("errA", errA).AnErr("errB", errB).Uint64("idA", xId).Uint64("idB", id).Msg("point not found for distance calculation")
				return math.MaxFloat32
			}
			return distance.HammingDistance(a.BinaryVector, b.BinaryVector)
		}
	}
	// Fallback to original vector
	a, errA := bq.items.Get(xId)
	return func(id uint64) float32 {
		b, errB := bq.items.Get(id)
		if errA != nil || errB != nil {
			log.Warn().AnErr("errA", errA).AnErr("errB", errB).Uint64("idA", xId).Uint64("idB", id).Msg("point not found for distance calculation")
			return math.MaxFloat32
		}
		return bq.distFn(a.Vector, b.Vector)
	}
}

func (bq *binaryQuantizer) Flush() error {
	if err := bq.items.Flush(); err != nil {
		return err
	}
	if bq.threshold != nil {
		return bq.bucket.Put([]byte(binaryQuantizerThresholdKey), conversion.SingleFloat32ToBytes(*bq.threshold))
	}
	return nil
}

// ---------------------------

type binaryQuantizedPoint struct {
	Id           uint64
	Vector       []float32
	BinaryVector []uint64
	isDirty      bool
}

func (bqp *binaryQuantizedPoint) IdFromKey(key []byte) (uint64, bool) {
	return conversion.NodeIdFromKey(key, 'v')
}

func (bqp *binaryQuantizedPoint) CheckAndClearDirty() bool {
	// This case occurs usually after fitting the quantizer. That is we modify
	// the binary vectors and request that the points are rewritten.
	dirty := bqp.isDirty
	bqp.isDirty = false
	return dirty
}

func (bqp *binaryQuantizedPoint) ReadFrom(id uint64, bucket diskstore.Bucket) (point *binaryQuantizedPoint, err error) {
	point = &binaryQuantizedPoint{Id: id}
	// ---------------------------
	binaryVecBytes := bucket.Get(conversion.NodeKey(id, 'q'))
	if binaryVecBytes != nil {
		point.BinaryVector = conversion.BytesToEdgeList(binaryVecBytes)
		/* NOTE: We don't load the full vector if the quantised version exists.
		 * This is what saves memory. */
		return
	}
	// ---------------------------
	fullVecBytes := bucket.Get(conversion.NodeKey(id, 'v'))
	if fullVecBytes == nil {
		err = cache.ErrNotFound
		return
	}
	point.Vector = conversion.BytesToFloat32(fullVecBytes)
	// ---------------------------
	return
}

func (bqp *binaryQuantizedPoint) WriteTo(bucket diskstore.Bucket) error {
	if len(bqp.Vector) != 0 {
		if err := bucket.Put(conversion.NodeKey(bqp.Id, 'v'), conversion.Float32ToBytes(bqp.Vector)); err != nil {
			return err
		}
	}
	if len(bqp.BinaryVector) != 0 {
		if err := bucket.Put(conversion.NodeKey(bqp.Id, 'q'), conversion.EdgeListToBytes(bqp.BinaryVector)); err != nil {
			return err
		}
	}
	return nil
}

func (bqp *binaryQuantizedPoint) DeleteFrom(bucket diskstore.Bucket) error {
	if err := bucket.Delete(conversion.NodeKey(bqp.Id, 'v')); err != nil {
		return err
	}
	if err := bucket.Delete(conversion.NodeKey(bqp.Id, 'q')); err != nil {
		return err
	}
	return nil
}
