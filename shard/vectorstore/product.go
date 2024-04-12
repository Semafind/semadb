package vectorstore

import (
	"fmt"
	"math"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/distance"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/utils"
)

const productQuantizerCentroidDistsKey = "_productQuantizerCentroidDists"
const productQuantizerFlatCentroidsKey = "_productQuantizerFlatCentroids"

/* Product Quantization is a technique to quantize high-dimensional vectors into
 * a low-memory usage representation. The original vector is divided up to m
 * subvectors and each subvector is quantized to k centroids. The final quantized
 * vector is the concatenation of the centroid ids of each subvector.  The
 * centroid ids are uint8 values.
 *
 * Credit and source: https://ieeexplore.ieee.org/document/5432202
 */
type productQuantizer struct {
	params            models.ProductQuantizerParameters
	distFn            distance.FloatDistFunc
	originalVectorLen int
	subVectorLen      int
	distFnName        string
	// ---------------------------
	items         *cache.ItemCache[uint64, *productQuantizedPoint]
	centroidDists []float32 // shape (num_subvectors * num_centroids * num_centroids)
	flatCentroids []float32 // shape (num_subvectors* num_centroids * subvector_len)
	// ---------------------------
	bucket diskstore.Bucket
}

func newProductQuantizer(bucket diskstore.Bucket, distFnName string, params models.ProductQuantizerParameters, vectorLen int) (*productQuantizer, error) {
	// Number of subvectors must divide the vector size perfectly
	if vectorLen%params.NumSubVectors != 0 {
		return nil, fmt.Errorf("vector length %d must be divisible by num subvectors %d", vectorLen, params.NumSubVectors)
	}
	// Check the distance function is compatiable
	if distFnName != models.DistanceEuclidean && distFnName != models.DistanceCosine && distFnName != models.DistanceDot {
		return nil, fmt.Errorf("distance function %s not supported for product quantisation", distFnName)
	}
	// Handle cosine distance
	if distFnName == models.DistanceCosine {
		/* Cosine distance can't be handled part wise. That is, product
		 * quantisation splits each vector into parts and sums distances of each
		 * part to centroids. Even if we compensate of cosine, the kmeans
		 * clustering uses euclidean distance and the normalisation property of
		 * subvectors are lost. We still have hope because for normalised vectors
		 * euclidean distance = 2*cosine distance, so it is proportional and
		 * yields similar results. */
		distFnName = models.DistanceEuclidean
	}
	// Check number of centroids, it cannot exceed 256 because of uint8 type
	if params.NumCentroids > 256 {
		return nil, fmt.Errorf("number of centroids %d cannot exceed 256", params.NumCentroids)
	}
	distFn, err := distance.GetFloatDistanceFn(distFnName)
	if err != nil {
		return nil, fmt.Errorf("could not get distance function %s: %w", distFnName, err)
	}
	// ---------------------------
	pq := &productQuantizer{
		params:            params,
		distFn:            distFn,
		distFnName:        distFnName,
		originalVectorLen: vectorLen,
		subVectorLen:      vectorLen / params.NumSubVectors,
		items:             cache.NewItemCache[uint64, *productQuantizedPoint](bucket),
		bucket:            bucket,
	}
	// Load centroid information from storage
	if buff := bucket.Get([]byte(productQuantizerCentroidDistsKey)); buff != nil {
		pq.centroidDists = conversion.BytesToFloat32(buff)
	}
	if buff := bucket.Get([]byte(productQuantizerFlatCentroidsKey)); buff != nil {
		pq.flatCentroids = conversion.BytesToFloat32(buff)
	}
	return pq, nil
}

func (pq productQuantizer) centroidDistIdx(subvector, centroidX, centroidY int) int {
	return subvector*pq.params.NumCentroids*pq.params.NumCentroids + centroidX*pq.params.NumCentroids + centroidY
}

func (pq productQuantizer) flatCentroidSlice(subvector, centroid int) (start, end int) {
	start = subvector*pq.params.NumCentroids*pq.subVectorLen + centroid*pq.subVectorLen
	end = start + pq.subVectorLen
	return
}

func (pq *productQuantizer) Exists(id uint64) bool {
	_, err := pq.items.Get(id)
	return err == nil
}

func (pq *productQuantizer) Get(id uint64) (VectorStorePoint, error) {
	return pq.items.Get(id)
}

func (pq *productQuantizer) GetMany(ids ...uint64) ([]VectorStorePoint, error) {
	points, err := pq.items.GetMany(ids...)
	if err != nil {
		return nil, err
	}
	ret := make([]VectorStorePoint, len(points))
	for i, p := range points {
		ret[i] = p
	}
	return ret, nil
}

func (pq *productQuantizer) ForEach(fn func(VectorStorePoint) error) error {
	return pq.items.ForEach(func(id uint64, point *productQuantizedPoint) error {
		return fn(point)
	})
}

func (pq *productQuantizer) SizeInMemory() int64 {
	return pq.items.SizeInMemory() + int64(len(pq.flatCentroids)*4) + int64(len(pq.centroidDists)*4)
}

func (pq *productQuantizer) UpdateBucket(bucket diskstore.Bucket) {
	pq.items.UpdateBucket(bucket)
	pq.bucket = bucket
}

func (pq productQuantizer) encode(vector []float32) []uint8 {
	if len(pq.flatCentroids) == 0 {
		return nil
	}
	/* We will now find the closest centroid for each subvector. */
	encoded := make([]uint8, pq.params.NumSubVectors)
	for i := 0; i < pq.params.NumSubVectors; i++ {
		// The subvector is the slice of the original vector
		subVector := vector[i*pq.subVectorLen : (i+1)*pq.subVectorLen]
		closestCentroidDistance := float32(math.MaxFloat32)
		closestCentroidId := 0
		for j := 0; j < pq.params.NumCentroids; j++ {
			sliceStart, sliceEnd := pq.flatCentroidSlice(i, j)
			centroid := pq.flatCentroids[sliceStart:sliceEnd]
			dist := pq.distFn(subVector, centroid)
			if dist < closestCentroidDistance {
				closestCentroidDistance = dist
				closestCentroidId = j
			}
		}
		encoded[i] = uint8(closestCentroidId)
	}
	return encoded
}

func (pq *productQuantizer) Set(id uint64, vector []float32) (VectorStorePoint, error) {
	point := &productQuantizedPoint{
		id:          id,
		Vector:      vector,
		CentroidIds: pq.encode(vector),
	}
	pq.items.Put(id, point)
	return point, nil
}

func (pq *productQuantizer) Delete(ids ...uint64) error {
	return pq.items.Delete(ids...)
}

func (pq *productQuantizer) Fit() error {
	// Have we already optimised?
	if len(pq.flatCentroids) != 0 {
		return nil
	}
	itemCount := pq.items.Count()
	if itemCount < pq.params.TriggerThreshold {
		return nil
	}
	// ---------------------------
	/* Run kmeans on the vectors to find the centroids. */
	allVectors := make([][]float32, 0, itemCount)
	allPoints := make([]*productQuantizedPoint, 0, itemCount)
	err := pq.items.ForEach(func(id uint64, point *productQuantizedPoint) error {
		allVectors = append(allVectors, point.Vector)
		allPoints = append(allPoints, point)
		point.CentroidIds = make([]uint8, pq.params.NumSubVectors)
		point.isDirty = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not collect vectors for kmeans: %w", err)
	}
	// ---------------------------
	pq.flatCentroids = make([]float32, pq.params.NumSubVectors*pq.params.NumCentroids*pq.subVectorLen)
	pq.centroidDists = make([]float32, pq.params.NumSubVectors*pq.params.NumCentroids*pq.params.NumCentroids)
	var wg sync.WaitGroup
	for i := 0; i < pq.params.NumSubVectors; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Perform kmeans on the subvectors
			kmeans := utils.KMeans{
				K:         pq.params.NumCentroids,
				MaxIter:   100,
				Offset:    i * pq.subVectorLen,
				VectorLen: pq.subVectorLen,
			}
			kmeans.Fit(allVectors)
			// Update encoded vectors, direct access from this go routine should
			// be safe because we only access our offset / subvector.
			for j := 0; j < len(allPoints); j++ {
				allPoints[j].CentroidIds[i] = kmeans.Labels[j]
			}
			// Update the flat centroids
			for j := 0; j < pq.params.NumCentroids; j++ {
				start, end := pq.flatCentroidSlice(i, j)
				copy(pq.flatCentroids[start:end], kmeans.Centroids[j])
			}
			// Update the centroid distances
			for j := 0; j < pq.params.NumCentroids; j++ {
				for k := 0; k < pq.params.NumCentroids; k++ {
					idx := pq.centroidDistIdx(i, j, k)
					pq.centroidDists[idx] = pq.distFn(kmeans.Centroids[j], kmeans.Centroids[k])
				}
			}
		}(i)
	}
	wg.Wait()
	// ---------------------------
	return nil
}

func (pq *productQuantizer) DistanceFromFloat(x []float32) PointIdDistFn {
	if len(pq.flatCentroids) == 0 {
		// We haven't fitted the quantizer yet
		return func(y VectorStorePoint) float32 {
			pointY, ok := y.(*productQuantizedPoint)
			if !ok {
				log.Warn().Uint64("id", y.Id()).Msg("point not found for pq distance calculation")
				return math.MaxFloat32
			}
			return pq.distFn(x, pointY.Vector)
		}
	}
	// ---------------------------
	/* We have encoded, so we will use the centroid distances. This is the
	 * asymmetric case where we have to calculate the distance between the query
	 * vector and the centroid. We do this by computing all centroid distances in
	 * advance and performing lookups after. */
	dists := make([]float32, pq.params.NumSubVectors*pq.params.NumCentroids)
	for i := 0; i < pq.params.NumSubVectors; i++ {
		subvector := x[i*pq.subVectorLen : (i+1)*pq.subVectorLen]
		for j := 0; j < pq.params.NumCentroids; j++ {
			start, end := pq.flatCentroidSlice(i, j)
			centroid := pq.flatCentroids[start:end]
			dists[i*pq.params.NumCentroids+j] = pq.distFn(subvector, centroid)
		}
	}
	// ---------------------------
	return func(y VectorStorePoint) float32 {
		pointY, ok := y.(*productQuantizedPoint)
		if !ok {
			log.Warn().Uint64("id", y.Id()).Msg("point not found for pq distance calculation")
			return math.MaxFloat32
		}
		var dist float32
		for i := 0; i < pq.params.NumSubVectors; i++ {
			dist += dists[i*pq.params.NumCentroids+int(pointY.CentroidIds[i])]
		}
		return dist
	}
}

func (pq *productQuantizer) DistanceFromPoint(x VectorStorePoint) PointIdDistFn {
	pointX, okX := x.(*productQuantizedPoint)
	if len(pq.flatCentroids) == 0 {
		// We haven't fitted the quantizer yet
		return func(y VectorStorePoint) float32 {
			pointY, okY := y.(*productQuantizedPoint)
			if !okX || !okY {
				log.Warn().Uint64("idX", x.Id()).Uint64("idY", y.Id()).Msg("point not found for distance calculation")
				return math.MaxFloat32
			}
			return pq.distFn(pointX.Vector, pointY.Vector)
		}
	}
	// We have encoded, so we will use the centroid distances
	return func(y VectorStorePoint) float32 {
		pointY, okY := y.(*productQuantizedPoint)
		if !okX || !okY {
			log.Warn().Uint64("idX", x.Id()).Uint64("idY", y.Id()).Msg("point not found for distance calculation")
			return math.MaxFloat32
		}
		var dist float32
		for i := 0; i < pq.params.NumSubVectors; i++ {
			dist += pq.centroidDists[pq.centroidDistIdx(i, int(pointX.CentroidIds[i]), int(pointY.CentroidIds[i]))]
		}
		return dist
	}
}

func (pq *productQuantizer) Flush() error {
	if err := pq.items.Flush(); err != nil {
		return err
	}
	if len(pq.flatCentroids) != 0 {
		if err := pq.bucket.Put([]byte(productQuantizerCentroidDistsKey), conversion.Float32ToBytes(pq.centroidDists)); err != nil {
			return err
		}
		if err := pq.bucket.Put([]byte(productQuantizerFlatCentroidsKey), conversion.Float32ToBytes(pq.flatCentroids)); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------

type productQuantizedPoint struct {
	id          uint64
	Vector      []float32
	CentroidIds []uint8
	isDirty     bool
}

func (p *productQuantizedPoint) Id() uint64 {
	return p.id
}

func (p *productQuantizedPoint) IdFromKey(key []byte) (uint64, bool) {
	return conversion.NodeIdFromKey(key, 'v')
}

func (p *productQuantizedPoint) SizeInMemory() int64 {
	return int64(8 + 4*len(p.Vector) + len(p.CentroidIds))
}

func (p *productQuantizedPoint) CheckAndClearDirty() bool {
	dirty := p.isDirty
	p.isDirty = false
	return dirty
}

func (p *productQuantizedPoint) ReadFrom(id uint64, bucket diskstore.Bucket) (point *productQuantizedPoint, err error) {
	point = &productQuantizedPoint{id: id}
	// ---------------------------
	centroidIdsBytes := bucket.Get(conversion.NodeKey(id, 'q'))
	if centroidIdsBytes != nil {
		// We make a copy here because the byte slice may be disposed after the
		// bucket transaction is closed.
		point.CentroidIds = make([]uint8, len(centroidIdsBytes))
		copy(point.CentroidIds, centroidIdsBytes)
		/* By returning here we save memory by not loading the full vector. */
		return
	}
	fullVecBytes := bucket.Get(conversion.NodeKey(id, 'v'))
	if fullVecBytes == nil {
		err = cache.ErrNotFound
		return
	}
	point.Vector = conversion.BytesToFloat32(fullVecBytes)
	// ---------------------------
	return
}

func (p *productQuantizedPoint) WriteTo(id uint64, bucket diskstore.Bucket) error {
	if len(p.Vector) != 0 {
		if err := bucket.Put(conversion.NodeKey(id, 'v'), conversion.Float32ToBytes(p.Vector)); err != nil {
			return err
		}
	}
	if len(p.CentroidIds) != 0 {
		if err := bucket.Put(conversion.NodeKey(id, 'q'), p.CentroidIds); err != nil {
			return err
		}
	}
	return nil
}

func (p *productQuantizedPoint) DeleteFrom(id uint64, bucket diskstore.Bucket) error {
	if err := bucket.Delete(conversion.NodeKey(id, 'v')); err != nil {
		return err
	}
	if err := bucket.Delete(conversion.NodeKey(id, 'q')); err != nil {
		return err
	}
	return nil
}

// ---------------------------
