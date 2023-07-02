package kvstore

import "errors"

var ErrStaleData = errors.New("stale data")
var ErrExistingKey = errors.New("existing key")
var ErrKeyNotFound = errors.New("key not found")
