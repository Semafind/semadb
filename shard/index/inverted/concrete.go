package inverted

import (
	"context"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
)

type IndexInvertedInteger struct {
	*indexInverted[int64]
}

func NewIndexInvertedInteger(bucket diskstore.Bucket) (*IndexInvertedInteger, error) {
	inv := newIndexInverted[int64](bucket)
	return &IndexInvertedInteger{inv}, nil
}

// ---------------------------

type IndexInvertedFloat struct {
	*indexInverted[float64]
}

func NewIndexInvertedFloat(bucket diskstore.Bucket) (*IndexInvertedFloat, error) {
	inv := newIndexInverted[float64](bucket)
	return &IndexInvertedFloat{inv}, nil
}

// ---------------------------

type IndexInvertedString struct {
	inner  *indexInverted[string]
	params models.IndexStringParameters
}

func NewIndexInvertedString(bucket diskstore.Bucket, params models.IndexStringParameters) (*IndexInvertedString, error) {
	inv := newIndexInverted[string](bucket)
	return &IndexInvertedString{inner: inv, params: params}, nil
}

// Transforms the data to lower case if the index is case insensitive
func (inv *IndexInvertedString) preProcessValue(term string) string {
	if !inv.params.CaseSensitive {
		return strings.ToLower(term)
	}
	return term
}

func (inv *IndexInvertedString) InsertUpdateDelete(ctx context.Context, in <-chan IndexChange[string]) error {
	// Process any transformers such as lowercasing before inserting
	out := in
	// Do we need to pre process?
	if !inv.params.CaseSensitive {
		out := make(chan IndexChange[string])
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case change, ok := <-in:
					if !ok {
						return
					}
					if change.CurrentData != nil {
						*change.CurrentData = inv.preProcessValue(*change.CurrentData)
					}
					if change.PreviousData != nil {
						*change.PreviousData = inv.preProcessValue(*change.PreviousData)
					}
					out <- change
				}
			}
		}()
	}
	return inv.inner.InsertUpdateDelete(ctx, out)
}

func (inv *IndexInvertedString) Search(options models.SearchStringOptions) (*roaring64.Bitmap, error) {
	query := inv.preProcessValue(options.Value)
	return inv.inner.Search(query, options.EndValue, options.Operator)
}

// ---------------------------
