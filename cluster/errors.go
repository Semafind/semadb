package cluster

import "errors"

var ErrExists = errors.New("already exists")
var ErrConflict = errors.New("cluster conflict")
var ErrNoSuccess = errors.New("no success")
var ErrTimeout = errors.New("timeout")
var ErrPartialSuccess = errors.New("partial success")
var ErrNotFound = errors.New("not found")
