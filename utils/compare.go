package utils

import (
	"cmp"
	"reflect"
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
