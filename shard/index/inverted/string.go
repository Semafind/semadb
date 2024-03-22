package inverted

import (
	"context"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/utils"
)

// ---------------------------

type IndexInvertedString struct {
	inner  *indexInverted[string]
	params models.IndexStringParameters
}

func NewIndexInvertedString(bucket diskstore.Bucket, params models.IndexStringParameters) (*IndexInvertedString, error) {
	inv := NewIndexInverted[string](bucket)
	return &IndexInvertedString{inner: inv, params: params}, nil
}

// Transforms the data to lower case if the index is case insensitive
func (inv *IndexInvertedString) preProcessValue(term string) string {
	if !inv.params.CaseSensitive {
		return strings.ToLower(term)
	}
	return term
}

func (inv *IndexInvertedString) InsertUpdateDelete(ctx context.Context, in <-chan IndexChange[string]) <-chan error {
	// Process any transformers such as lowercasing before inserting
	out := in
	var transformErrC <-chan error
	// Do we need to pre process?
	if !inv.params.CaseSensitive {
		out, transformErrC = utils.TransformWithContext(ctx, in, func(change IndexChange[string]) (IndexChange[string], bool, error) {
			if change.CurrentData != nil {
				*change.CurrentData = inv.preProcessValue(*change.CurrentData)
			}
			if change.PreviousData != nil {
				*change.PreviousData = inv.preProcessValue(*change.PreviousData)
			}
			return change, false, nil
		})
	}
	insertErrC := inv.inner.InsertUpdateDelete(ctx, out)
	if transformErrC != nil {
		return utils.MergeErrorsWithContext(ctx, transformErrC, insertErrC)
	}
	return insertErrC
}

func (inv *IndexInvertedString) Search(options models.SearchStringOptions) (*roaring64.Bitmap, error) {
	query := inv.preProcessValue(options.Value)
	return inv.inner.Search(query, options.EndValue, options.Operator)
}

// ---------------------------
