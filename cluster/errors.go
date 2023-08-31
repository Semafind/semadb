package cluster

import "errors"

var ErrExists = errors.New("already exists")
var ErrTimeout = errors.New("timeout")
var ErrPartialSuccess = errors.New("partial success")
var ErrNotFound = errors.New("not found")
var ErrShardUnavailable = errors.New("shard unavailable")
