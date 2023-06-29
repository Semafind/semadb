package cluster

import "errors"

var ErrConflict = errors.New("cluster conflict")
var ErrNoSuccess = errors.New("no success")
var ErrTimeout = errors.New("timeout")
var ErrPartialSuccess = errors.New("partial success")
