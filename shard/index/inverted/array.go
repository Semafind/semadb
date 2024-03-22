package inverted

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/utils"
)

type IndexInvertedArray[T Invertable] struct {
	inner *indexInverted[T]
}

func NewIndexInvertedArray[T Invertable](bucket diskstore.Bucket) *IndexInvertedArray[T] {
	inv := NewIndexInverted[T](bucket)
	return &IndexInvertedArray[T]{inner: inv}
}

type IndexArrayChange[T Invertable] struct {
	Id           uint64
	PreviousData []T
	CurrentData  []T
}

func (inv *IndexInvertedArray[T]) InsertUpdateDelete(ctx context.Context, in <-chan IndexArrayChange[T]) <-chan error {
	// We are ignoring the error because the transformation never returns an error
	out, _ := utils.TransformWithContextMultiple(ctx, in, func(change IndexArrayChange[T]) ([]IndexChange[T], error) {
		// Any value present in current but not in previous is added
		// Any value present in previous but not in current is deleted
		currentSet := make(map[T]struct{})
		prevSet := make(map[T]struct{})
		for _, v := range change.PreviousData {
			prevSet[v] = struct{}{}
		}
		changes := make([]IndexChange[T], 0)
		for _, val := range change.CurrentData {
			// If the value is not in previous map, it's an addition
			if _, ok := prevSet[val]; !ok {
				changes = append(changes, IndexChange[T]{Id: change.Id, CurrentData: &val})
			}
			currentSet[val] = struct{}{}
		}
		// Detect deletions by iterating through previous map
		for val := range prevSet {
			// If the value is not in current map, it's a deletion
			if _, ok := currentSet[val]; !ok {
				changes = append(changes, IndexChange[T]{Id: change.Id, PreviousData: &val})
			}
		}

		return changes, nil
	})
	return inv.inner.InsertUpdateDelete(ctx, out)
}

func (inv *IndexInvertedArray[T]) Search(query []T, operator string) (*roaring64.Bitmap, error) {
	if len(query) == 0 {
		return nil, nil
	}
	// ---------------------------
	resList := make([]*roaring64.Bitmap, len(query))
	for i, q := range query {
		res, err := inv.inner.Search(q, q, models.OperatorEquals)
		if err != nil {
			return nil, err
		}
		resList[i] = res
	}
	// ---------------------------
	if len(resList) == 1 {
		return resList[0], nil
	}
	var finalSet *roaring64.Bitmap
	switch operator {
	case models.OperatorContainsAll:
		finalSet = roaring64.FastAnd(resList...)
	case models.OperatorContainsAny:
		finalSet = roaring64.FastOr(resList...)
	default:
		return nil, fmt.Errorf("unsupported operator %s", operator)
	}
	// ---------------------------
	return finalSet, nil
}
