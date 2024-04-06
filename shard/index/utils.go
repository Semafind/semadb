package index

import (
	"bytes"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

func getPropertyFromBytes(dec *msgpack.Decoder, data []byte, property string) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	dec.Reset(bytes.NewReader(data))
	// ---------------------------
	queryResult, err := dec.Query(property)
	if err != nil {
		return nil, fmt.Errorf("could not query field: %w", err)
	}
	if len(queryResult) == 0 {
		// The field is not found
		return nil, nil
	}
	// ---------------------------
	return queryResult[0], nil
}

// ---------------------------
const (
	opInsert = "insert"
	opUpdate = "update"
	opDelete = "delete"
	opSkip   = "skip"
)

// Determines the operation to be performed on the index and extracts the previous
// and current property values.
func getOperation(dec *msgpack.Decoder, propertyName string, prevData, currentData []byte) (prevProp, currentProp any, op string, err error) {
	prevProp, err = getPropertyFromBytes(dec, prevData, propertyName)
	if err != nil {
		err = fmt.Errorf("could not get previous property %s: %w", propertyName, err)
		return
	}
	currentProp, err = getPropertyFromBytes(dec, currentData, propertyName)
	if err != nil {
		err = fmt.Errorf("could not get new property %s: %w", propertyName, err)
		return
	}
	switch {
	case prevProp == nil && currentProp != nil:
		// Insert
		op = opInsert
	case prevProp != nil && currentProp != nil:
		// Update
		op = opUpdate
	case prevProp != nil && currentProp == nil:
		// Delete
		op = opDelete
	case prevProp == nil && currentProp == nil:
		// Skip
		op = opSkip
	default:
		err = fmt.Errorf("unexpected previous and current values for %s: %v - %v", propertyName, prevProp, currentProp)
	}
	// Named return values are a language feature that allows us to declare what
	// the return values are named and then we can just return without specifying
	// the return values.
	return
}

// ---------------------------

func castDataToArray[T any](data any) ([]T, error) {
	// The problem is the query returns []any and we need to
	// convert it to the appropriate type, doing .([]float32) doesn't work
	var vector []T
	if data != nil {
		if anyArr, ok := data.([]any); !ok {
			return vector, fmt.Errorf("expected vector got %T", data)
		} else {
			vector = make([]T, len(anyArr))
			for i, v := range anyArr {
				vector[i], ok = v.(T)
				if !ok {
					return vector, fmt.Errorf("expected %T got %T", vector[i], v)
				}
			}
		}
	}
	return vector, nil
}
