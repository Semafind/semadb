package inverted

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
)

type invertable interface {
	uint64 | int64 | float64 | string
}

type setCacheItem struct {
	set     *roaring64.Bitmap
	isDirty bool
}

type indexInverted[T invertable] struct {
	setCache map[T]*setCacheItem
	bucket   diskstore.Bucket
	mu       sync.Mutex
}

func newIndexInverted[T invertable](b diskstore.Bucket) *indexInverted[T] {
	inv := &indexInverted[T]{
		setCache: make(map[T]*setCacheItem),
		bucket:   b,
	}
	return inv
}

func (inv *indexInverted[T]) getSetCacheItem(value T, setBytes []byte) (*setCacheItem, error) {
	item, ok := inv.setCache[value]
	if !ok {
		// Attempt to read from the bucket
		key, err := toByteSortable(value)
		if err != nil {
			return nil, fmt.Errorf("error converting key to byte sortable: %w", err)
		}
		if setBytes == nil {
			setBytes = inv.bucket.Get(key)
		}
		rSet := roaring64.New()
		if setBytes != nil {
			if _, err := rSet.ReadFrom(bytes.NewReader(setBytes)); err != nil {
				return nil, fmt.Errorf("error reading set from bytes: %w", err)
			}
		}
		item = &setCacheItem{
			set: rSet,
		}
		inv.setCache[value] = item

	}
	return item, nil
}

type IndexChange[T invertable] struct {
	Id           uint64
	PreviousData *T
	CurrentData  *T
}

func (inv *indexInverted[T]) InsertUpdateDelete(ctx context.Context, in <-chan IndexChange[T]) error {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case change, ok := <-in:
			if !ok {
				return inv.flush()
			}
			// ---------------------------
			// inv.preProcessChange(change)
			// ---------------------------
			switch {
			case change.PreviousData == nil && change.CurrentData == nil:
				// Blank change, nothing to do
				continue
			case change.PreviousData == nil && change.CurrentData != nil:
				// Insert
				set, err := inv.getSetCacheItem(*change.CurrentData, nil)
				if err != nil {
					return fmt.Errorf("error getting set cache item: %w", err)
				}
				set.isDirty = set.set.CheckedAdd(change.Id)
			case change.PreviousData != nil && change.CurrentData == nil:
				// Delete
				set, err := inv.getSetCacheItem(*change.PreviousData, nil)
				if err != nil {
					return fmt.Errorf("error getting set cache item: %w", err)
				}
				set.isDirty = set.set.CheckedRemove(change.Id)
			case *change.PreviousData != *change.CurrentData:
				// Update
				prevSet, err := inv.getSetCacheItem(*change.PreviousData, nil)
				if err != nil {
					return fmt.Errorf("error getting set cache item: %w", err)
				}
				prevSet.isDirty = prevSet.set.CheckedRemove(change.Id)
				currSet, err := inv.getSetCacheItem(*change.CurrentData, nil)
				if err != nil {
					return fmt.Errorf("error getting set cache item: %w", err)
				}
				currSet.isDirty = currSet.set.CheckedAdd(change.Id)
			case *change.PreviousData == *change.CurrentData:
				// This case needs to be last not to get null pointer exception
				continue
			}
		}
	}
}

func (inv *indexInverted[T]) flush() error {
	// ---------------------------
	for term, item := range inv.setCache {
		if !item.isDirty {
			continue
		}
		// ---------------------------
		setBytes, err := item.set.ToBytes()
		if err != nil {
			return fmt.Errorf("error converting term set to bytes: %w", err)
		}
		key, err := toByteSortable(term)
		if err != nil {
			return fmt.Errorf("error converting key to byte sortable: %w", err)
		}
		if err := inv.bucket.Put(key, setBytes); err != nil {
			return fmt.Errorf("error putting term set to bucket: %w", err)
		}
	}
	// ---------------------------
	return nil
}

func (inv *indexInverted[T]) Search(query T, endQuery T, operator string) (*roaring64.Bitmap, error) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	// ---------------------------
	queryKey, err := toByteSortable(query)
	if err != nil {
		return nil, fmt.Errorf("error converting value %v to search: %w", query, err)
	}
	sets := make([]*roaring64.Bitmap, 0, 1)
	// ---------------------------
	var start, end []byte
	var inclusive bool
	// ---------------------------
	switch operator {
	case models.OperatorEquals:
		item, err := inv.getSetCacheItem(query, nil)
		if err != nil {
			return nil, fmt.Errorf("error getting set cache item: %w", err)
		}
		sets = append(sets, item.set)
	case models.OperatorNotEquals:
		// This is actually a costly operation, we should let users know it
		// causes an index scan
		err := inv.bucket.ForEach(func(k, v []byte) error {
			if bytes.Equal(k, queryKey) {
				// We are looking for all the keys that are not equal
				return nil
			}
			var reverseKey T
			err := fromByteSortable(k, &reverseKey)
			if err != nil {
				return fmt.Errorf("error converting key to value: %w", err)
			}
			item, err := inv.getSetCacheItem(reverseKey, v)
			if err != nil {
				return fmt.Errorf("error getting set cache item: %w", err)
			}
			sets = append(sets, item.set)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("error iterating over bucket for inverted search: %w", err)
		}
	case models.OperatorStartsWith:
		err := inv.bucket.PrefixScan(queryKey, func(k, v []byte) error {
			var reverseKey T
			err := fromByteSortable(k, &reverseKey)
			if err != nil {
				return fmt.Errorf("error converting key to value: %w", err)
			}
			item, err := inv.getSetCacheItem(reverseKey, v)
			if err != nil {
				return fmt.Errorf("error getting set cache item: %w", err)
			}
			sets = append(sets, item.set)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("error prefix scanning over bucket for inverted search: %w", err)
		}
	case models.OperatorGreaterThan:
		start = queryKey
		inclusive = false
	case models.OperatorGreaterOrEq:
		start = queryKey
		inclusive = true
	case models.OperatorLessThan:
		end = queryKey
		inclusive = false
	case models.OperatorLessOrEq:
		end = queryKey
		inclusive = true
	case models.OperatorInRange:
		start = queryKey
		endk, err := toByteSortable(endQuery)
		if err != nil {
			return nil, fmt.Errorf("error converting value %v to search: %w", endQuery, err)
		}
		end = endk
		inclusive = true
	default:
		return nil, fmt.Errorf("unknown inverted search operator: %s", operator)
	}
	// ---------------------------
	if start != nil || end != nil {
		err := inv.bucket.RangeScan(start, end, inclusive, func(k, v []byte) error {
			var reverseKey T
			err := fromByteSortable(k, &reverseKey)
			if err != nil {
				return fmt.Errorf("error converting key to value: %w", err)
			}
			item, err := inv.getSetCacheItem(reverseKey, v)
			if err != nil {
				return fmt.Errorf("error getting set cache item: %w", err)
			}
			sets = append(sets, item.set)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("error range scanning over bucket for inverted search: %w", err)
		}
	}
	// ---------------------------
	if len(sets) == 0 {
		return nil, nil
	}
	if len(sets) == 1 {
		return sets[0].Clone(), nil
	}
	// ---------------------------
	return roaring64.FastAnd(sets...), nil
}