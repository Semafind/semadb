package utils

import (
	"cmp"
	"reflect"
	"slices"
	"strings"

	"github.com/semafind/semadb/models"
)

// Attempts to compare two values of any type.
func CompareAny(a, b any) int {
	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)
	at := av.Kind()
	bt := bv.Kind()
	if at != bt {
		// Different types, compare type kinds directly so same types are grouped up.
		return cmp.Compare(at, bt)
	}
	// We are dealing with the same type
	switch at {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return cmp.Compare(av.Int(), bv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return cmp.Compare(av.Uint(), bv.Uint())
	case reflect.Float32, reflect.Float64:
		return cmp.Compare(av.Float(), bv.Float())
	case reflect.String:
		return cmp.Compare(av.String(), bv.String())
	}
	// We don't know how to compare this type, so we just say they are equal.
	return 0
}

// Accesses a nested property in a map of the form path "a.b.c".
func AccessNestedProperty(data map[string]any, path string) (any, bool) {
	var current any = data
	for _, p := range strings.Split(path, ".") {
		switch v := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = v[p]
			if !ok {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return current, true
}

// Attempts to sort search results by the given properties.
func SortSearchResults(results []models.SearchResult, sortOpts []models.SortOption) {
	/* Because we don't know the type of the values, this may be a costly
	 * operation to undertake. We should monitor how this performs. */
	slices.SortFunc(results, func(a, b models.SearchResult) int {
		for _, s := range sortOpts {
			// E.g. s = "age"
			av, aok := AccessNestedProperty(a.DecodedData, s.Property)
			bv, bok := AccessNestedProperty(b.DecodedData, s.Property)
			/* If the property is missing, we need to decide what to do
			 * here. We can either put it at the top or bottom. We put it
			 * at the bottom for now so that missing values are last. */
			if aok && !bok {
				// a has it, but b doesn't
				return -1
			}
			if !aok && bok {
				return 1
			}
			if !aok && !bok {
				continue
			}
			var res int
			if s.Descending {
				res = CompareAny(bv, av)
			} else {
				res = CompareAny(av, bv)
			}
			if res != 0 {
				return res
			}
		}
		return 0
	})
}
