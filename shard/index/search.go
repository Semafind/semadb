package index

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index/flat"
	"github.com/semafind/semadb/shard/index/inverted"
	"github.com/semafind/semadb/shard/index/text"
	"github.com/semafind/semadb/shard/index/vamana"
)

func (im indexManager) Search(
	ctx context.Context,
	q models.Query,
) (*roaring64.Bitmap, []models.SearchResult, error) {
	// ---------------------------
	// We will dispatch each query to the appropriate index in parallel
	// ---------------------------
	// Cover special property cases first
	switch q.Property {
	case "_and":
		return im.searchParallel(ctx, q.And, false)
	case "_or":
		return im.searchParallel(ctx, q.Or, true)
	}
	iparams, ok := im.indexSchema[q.Property]
	if !ok {
		return nil, nil, fmt.Errorf("property %s not found in index schema", q.Property)
	}
	itype := iparams.Type
	// ---------------------------
	// e.g. index/vamana/myvector
	bucketName := fmt.Sprintf("index/%s/%s", itype, q.Property)
	bucket, err := im.bm.Get(bucketName)
	if err != nil {
		return nil, nil, fmt.Errorf("could not read bucket %s: %w", bucketName, err)
	}
	cacheName := im.cacheRoot + "/" + bucketName
	// ---------------------------
	switch itype {
	case models.IndexTypeVectorVamana:
		if q.VectorVamana == nil {
			return nil, nil, fmt.Errorf("no vectorVamana query options for property %s", q.Property)
		}
		// ---------------------------
		// This has to be computed prior to the search so cannot be done in parallel
		var filter *roaring64.Bitmap
		if q.VectorVamana.Filter != nil {
			filter, _, err = im.Search(ctx, *q.VectorVamana.Filter)
			if err != nil {
				return nil, nil, fmt.Errorf("could not search filter: %w", err)
			}
		}
		// ---------------------------
		var vamanaSet *roaring64.Bitmap
		var vamanaRes []models.SearchResult
		newVamanaFn := func() (cache.Cachable, error) {
			return vamana.NewIndexVamana(cacheName, *iparams.VectorVamana, bucket)
		}
		err := im.cx.With(cacheName, true, newVamanaFn, func(cached cache.Cachable) error {
			vamanaIndex := cached.(*vamana.IndexVamana)
			vamanaIndex.UpdateBucket(bucket)
			resSet, res, err := vamanaIndex.Search(ctx, *q.VectorVamana, filter)
			if err != nil {
				return fmt.Errorf("could not perform vamana search %s: %w", bucketName, err)
			}
			vamanaRes = res
			vamanaSet = resSet
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("could not search %s: %w", bucketName, err)
		}
		// ---------------------------
		return vamanaSet, vamanaRes, nil
	case models.IndexTypeVectorFlat:
		if q.VectorFlat == nil {
			return nil, nil, fmt.Errorf("no vectorFlat query options for property %s", q.Property)
		}
		// ---------------------------
		var filter *roaring64.Bitmap
		if q.VectorFlat.Filter != nil {
			filter, _, err = im.Search(ctx, *q.VectorFlat.Filter)
			if err != nil {
				return nil, nil, fmt.Errorf("could not search filter: %w", err)
			}
		}
		// ---------------------------
		var flatSet *roaring64.Bitmap
		var flatRes []models.SearchResult
		newFlatFn := func() (cache.Cachable, error) {
			return flat.NewIndexFlat(*iparams.VectorFlat, bucket)
		}
		err := im.cx.With(cacheName, true, newFlatFn, func(cached cache.Cachable) error {
			flatIndex := cached.(flat.IndexFlat)
			flatIndex.UpdateBucket(bucket)
			resSet, res, err := flatIndex.Search(ctx, *q.VectorFlat, filter)
			if err != nil {
				return fmt.Errorf("could not perform flat search %s: %w", bucketName, err)
			}
			flatRes = res
			flatSet = resSet
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("could not search %s: %w", bucketName, err)
		}
		// ---------------------------
		return flatSet, flatRes, nil
	case models.IndexTypeText:
		if q.Text == nil {
			return nil, nil, fmt.Errorf("no text query options for property %s", q.Property)
		}
		var filter *roaring64.Bitmap
		if q.Text.Filter != nil {
			filter, _, err = im.Search(ctx, *q.Text.Filter)
			if err != nil {
				return nil, nil, fmt.Errorf("could not search filter: %w", err)
			}
		}
		textIndex, err := text.NewIndexText(bucket, *iparams.Text)
		if err != nil {
			return nil, nil, fmt.Errorf("could not create text index %s: %w", bucketName, err)
		}
		return textIndex.Search(*q.Text, filter)
	case models.IndexTypeString:
		if q.String == nil {
			return nil, nil, fmt.Errorf("no string query options for property %s", q.Property)
		}
		stringIndex := inverted.NewIndexInvertedString(bucket, *iparams.String)
		rSet, err := stringIndex.Search(*q.String)
		return rSet, nil, err
	case models.IndexTypeStringArray:
		if q.StringArray == nil {
			return nil, nil, fmt.Errorf("no stringArray query options for property %s", q.Property)
		}
		stringArrayIndex := inverted.NewIndexInvertedArrayString(bucket, *iparams.StringArray)
		rSet, err := stringArrayIndex.Search(*q.StringArray)
		return rSet, nil, err
	case models.IndexTypeInteger:
		if q.Integer == nil {
			return nil, nil, fmt.Errorf("no integer query options for property %s", q.Property)
		}
		integerIndex := inverted.NewIndexInverted[int64](bucket)
		rSet, err := integerIndex.Search(q.Integer.Value, q.Integer.EndValue, q.Integer.Operator)
		return rSet, nil, err
	case models.IndexTypeFloat:
		if q.Float == nil {
			return nil, nil, fmt.Errorf("no float query options for property %s", q.Property)
		}
		floatIndex := inverted.NewIndexInverted[float64](bucket)
		rSet, err := floatIndex.Search(q.Float.Value, q.Float.EndValue, q.Float.Operator)
		return rSet, nil, err
	default:
		return nil, nil, fmt.Errorf("search not supported for property %s of type %s", q.Property, itype)
	}
}

func (im indexManager) searchParallel(
	ctx context.Context,
	queries []models.Query,
	isDisjunction bool,
) (*roaring64.Bitmap, []models.SearchResult, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	// ---------------------------
	sets := make([]*roaring64.Bitmap, len(queries))
	results := make([][]models.SearchResult, len(queries))
	var wg sync.WaitGroup
	// ---------------------------
	// We will dispatch each query to the appropriate index in parallel
	for i, q := range queries {
		wg.Add(1)
		go func(i int, q models.Query) {
			defer wg.Done()
			set, res, err := im.Search(ctx, q)
			if err != nil {
				cancel(err)
				return
			}
			sets[i] = set
			results[i] = res
		}(i, q)
	}
	// ---------------------------
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return nil, nil, fmt.Errorf("parallel search failed: %w", err)
	}
	// ---------------------------
	if len(queries) == 1 {
		// Shortcut, no merging required
		return sets[0], results[0], nil
	}
	// ---------------------------
	var finalSet *roaring64.Bitmap
	if isDisjunction {
		finalSet = roaring64.FastOr(sets...)
	} else {
		finalSet = roaring64.FastAnd(sets...)
	}
	// ---------------------------
	/* Clean up results. The important thing to note is that we need to
	 * deduplicate and duplicate search results may have different hybrid scores.
	 * For example, two searches may find the same item but assign different
	 * hybrid scores. In this case we add them together. */
	finalSize := finalSet.GetCardinality()
	finalResults := make([]models.SearchResult, 0, finalSize)
	deduplicateMap := make(map[uint64]int, finalSize)
	// For every result, we check and add hybrid scores
	for _, res := range results {
		for _, r := range res {
			// In the disjunction case we know all the points are valid, so we
			// only check for !isDisjunction case
			if !isDisjunction && !finalSet.Contains(r.NodeId) {
				continue
			}
			idx, ok := deduplicateMap[r.NodeId]
			if !ok {
				deduplicateMap[r.NodeId] = len(finalResults)
				finalResults = append(finalResults, r)
			} else {
				// Add hybrid scores together
				finalResults[idx].HybridScore += r.HybridScore
				/* For now we merge the results as it covers the base, but in the
				 * future we might need to keep track of where these scores come
				 * from etc. For example if you did a hybrid search of more than
				 * 2 items, the distance and the score will be from the first
				 * search. */
				if finalResults[idx].Distance == nil && r.Distance != nil {
					finalResults[idx].Distance = r.Distance
				}
				if finalResults[idx].Score == nil && r.Score != nil {
					finalResults[idx].Score = r.Score
				}
			}
		}
	}
	// ---------------------------
	// Sort based on potential new hybrid scores
	slices.SortFunc(finalResults, func(a, b models.SearchResult) int {
		return cmp.Compare(b.HybridScore, a.HybridScore)
	})
	// ---------------------------
	return finalSet, finalResults, nil
}
