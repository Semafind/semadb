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
	items     *cache.ItemCache[uint64, *binaryQuantizedPoint]
	bucket    diskstore.Bucket
	distFn    distance.DistFunc
}

func newBinaryQuantizer(bucket diskstore.Bucket, distFn distance.DistFunc, params models.BinaryQuantizerParamaters) *binaryQuantizer {
	bq := &binaryQuantizer{
		items:     cache.NewItemCache[uint64, *binaryQuantizedPoint](bucket),
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

func (bq *binaryQuantizer) Get(id uint64) (VectorStorePoint, error) {
	return bq.items.Get(id)
}

func (bq *binaryQuantizer) GetMany(ids ...uint64) ([]VectorStorePoint, error) {
	points, err := bq.items.GetMany(ids...)
	if err != nil {
		return nil, err
	}
	ret := make([]VectorStorePoint, len(points))
	for i, p := range points {
		ret[i] = p
	}
	return ret, nil

}

func (bq *binaryQuantizer) SizeInMemory() int64 {
	return bq.items.SizeInMemory()
}

func (bq *binaryQuantizer) UpdateBucket(bucket diskstore.Bucket) {
	bq.items.UpdateBucket(bucket)
	bq.bucket = bucket
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

func (bq *binaryQuantizer) Set(id uint64, vector []float32) (VectorStorePoint, error) {
	point := &binaryQuantizedPoint{
		id:           id,
		Vector:       vector,
		BinaryVector: bq.encode(vector),
	}
	return point, bq.items.Put(id, point)
}

func (bq *binaryQuantizer) Delete(ids ...uint64) error {
	return bq.items.Delete(ids...)
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
	// It's okay to duplicate code inside the distance function here because it
	// avoids the if statement check for each distance calculation. Recall that
	// there are a lot of distance calculations in vector stores.
	if bq.threshold != nil {
		encodedX := bq.encode(x)
		return func(y VectorStorePoint) float32 {
			pointY, ok := y.(*binaryQuantizedPoint)
			if !ok {
				log.Warn().Uint64("id", y.Id()).Msg("point not found for distance calculation")
				return math.MaxFloat32
			}
			return distance.HammingDistance(encodedX, pointY.BinaryVector)
		}
	}
	/* Here we fall back to the original vector if the threshold is not set. */
	return func(y VectorStorePoint) float32 {
		pointY, ok := y.(*binaryQuantizedPoint)
		if !ok {
			log.Warn().Uint64("id", y.Id()).Msg("point not found for distance calculation")
			return math.MaxFloat32
		}
		return bq.distFn(x, pointY.Vector)
	}
}

func (bq *binaryQuantizer) DistanceFromPoint(x VectorStorePoint) PointIdDistFn {
	pointX, okX := x.(*binaryQuantizedPoint)
	if bq.threshold != nil {
		return func(y VectorStorePoint) float32 {
			pointB, okB := y.(*binaryQuantizedPoint)
			if !okX || !okB {
				log.Warn().Uint64("idX", x.Id()).Uint64("idY", y.Id()).Msg("point not found for distance calculation")
				return math.MaxFloat32
			}
			return distance.HammingDistance(pointX.BinaryVector, pointB.BinaryVector)
		}
	}
	// Fallback to original vector
	return func(y VectorStorePoint) float32 {
		pointB, okB := y.(*binaryQuantizedPoint)
		if !okX || !okB {
			log.Warn().Uint64("idX", x.Id()).Uint64("idY", y.Id()).Msg("point not found for distance calculation")
			return math.MaxFloat32
		}
		return bq.distFn(pointX.Vector, pointB.Vector)
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
	id           uint64
	Vector       []float32
	BinaryVector []uint64
	isDirty      bool
}

func (bqp *binaryQuantizedPoint) Id() uint64 {
	return bqp.id
}

func (bqp *binaryQuantizedPoint) IdFromKey(key []byte) (uint64, bool) {
	return conversion.NodeIdFromKey(key, 'v')
}

func (bqp *binaryQuantizedPoint) SizeInMemory() int64 {
	return int64(len(bqp.Vector)*4 + len(bqp.BinaryVector)*8)
}

func (bqp *binaryQuantizedPoint) CheckAndClearDirty() bool {
	// This case occurs usually after fitting the quantizer. That is we modify
	// the binary vectors and request that the points are rewritten.
	dirty := bqp.isDirty
	bqp.isDirty = false
	return dirty
}

func (bqp *binaryQuantizedPoint) ReadFrom(id uint64, bucket diskstore.Bucket) (point *binaryQuantizedPoint, err error) {
	point = &binaryQuantizedPoint{id: id}
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

func (bqp *binaryQuantizedPoint) WriteTo(id uint64, bucket diskstore.Bucket) error {
	if len(bqp.Vector) != 0 {
		if err := bucket.Put(conversion.NodeKey(id, 'v'), conversion.Float32ToBytes(bqp.Vector)); err != nil {
			return err
		}
	}
	if len(bqp.BinaryVector) != 0 {
		if err := bucket.Put(conversion.NodeKey(id, 'q'), conversion.EdgeListToBytes(bqp.BinaryVector)); err != nil {
			return err
		}
	}
	return nil
}

func (bqp *binaryQuantizedPoint) DeleteFrom(id uint64, bucket diskstore.Bucket) error {
	if err := bucket.Delete(conversion.NodeKey(id, 'v')); err != nil {
		return err
	}
	if err := bucket.Delete(conversion.NodeKey(id, 'q')); err != nil {
		return err
	}
	return nil
}
