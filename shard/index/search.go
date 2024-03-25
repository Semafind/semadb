package index

import (
	"context"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
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
	// ---------------------------
	switch itype {
	case models.IndexTypeVectorVamana:
		if q.VectorVamana == nil {
			return nil, nil, fmt.Errorf("no vectorVamana query options")
		}
		// ---------------------------
		// TODO: Compute filter
		if q.VectorVamana.Filter != nil {
			return nil, nil, fmt.Errorf("filter not supported for property %s of type %s", q.Property, itype)
		}
		// ---------------------------
		cacheName := im.cacheRoot + "/" + bucketName
		var vamanaRes []models.SearchResult
		err := im.cx.WithReadOnly(cacheName, bucket, func(pc cache.SharedPointCache) error {
			vIndex, err := vamana.NewIndexVamana(cacheName, pc, *iparams.VectorVamana, im.maxNodeId)
			if err != nil {
				return fmt.Errorf("could not create vamana index: %w", err)
			}
			res, err := vIndex.Search(ctx, q.VectorVamana.Vector, q.VectorVamana.Limit)
			if err != nil {
				return fmt.Errorf("could not perform vamana search %s: %w", bucketName, err)
			}
			vamanaRes = res
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("could not search %s: %w", bucketName, err)
		}
		// ---------------------------
		weight := float32(1)
		if q.VectorVamana.Weight != 0 {
			weight = q.VectorVamana.Weight
		}
		set := roaring64.New()
		for i, r := range vamanaRes {
			// We multiply by -1 to make the distance a positive score
			set.Add(r.NodeId)
			score := (-1 * weight * *r.Distance)
			vamanaRes[i].FinalScore = &score
		}
		return set, vamanaRes, nil
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
		return nil, nil, fmt.Errorf("search failed: %w", err)
	}
	// ---------------------------
	if len(queries) == 1 {
		// Shortcut, no merging required
		return sets[0], results[0], nil
	}
	// ---------------------------
	var finalSet *roaring64.Bitmap
	finalResults := make([]models.SearchResult, 0)
	duplicateMap := make(map[uint64]struct{})
	if !isDisjunction {
		// We will take the intersection of all sets
		finalSet = roaring64.FastAnd(sets...)
		// This is now like post-filtering, we only keep those results that are in
		// the final set
		for _, res := range results {
			for _, r := range res {
				if finalSet.Contains(r.NodeId) {
					if _, ok := duplicateMap[r.NodeId]; !ok {
						finalResults = append(finalResults, r)
						duplicateMap[r.NodeId] = struct{}{}
					}
				}
			}
		}
		return finalSet, finalResults, nil
	}
	// We will take the union of all sets
	finalSet = roaring64.FastOr(sets...)
	// ---------------------------
	// N way merge sort descending on .FinalScore property
	for {
		var best models.SearchResult
		bestIndex := -1
		// We loop through to find the highest score from the results array
		for i, res := range results {
			if len(res) == 0 {
				continue
			}
			if *res[0].FinalScore > *best.FinalScore {
				best = res[0]
				bestIndex = i
			}
		}
		if bestIndex == -1 {
			break
		}
		if _, ok := duplicateMap[best.NodeId]; !ok {
			finalResults = append(finalResults, best)
			duplicateMap[best.NodeId] = struct{}{}
		}
		results[bestIndex] = results[bestIndex][1:]
	}
	// ---------------------------
	return finalSet, finalResults, nil
}
