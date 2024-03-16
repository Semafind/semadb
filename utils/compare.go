package utils

import (
	"cmp"
	"reflect"
)

// Attempts to compare two values of any type.
func CompareAny(a, b any) int {
	at := reflect.TypeOf(a).Kind()
	bt := reflect.TypeOf(b).Kind()
	if at != bt {
		// Different types, compare type kinds directly so same types are grouped up.
		return cmp.Compare(at, bt)
	}
	// We are dealing with the same type
	switch at {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return cmp.Compare(a.(int64), b.(int64))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return cmp.Compare(a.(uint64), b.(uint64))
	case reflect.Float32, reflect.Float64:
		return cmp.Compare(a.(float64), b.(float64))
	case reflect.String:
		return cmp.Compare(a.(string), b.(string))
	}
	// We don't know how to compare this type, so we just say they are equal.
	return 0
}
