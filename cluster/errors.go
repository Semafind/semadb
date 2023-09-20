package cluster

import "errors"

var ErrExists = errors.New("already exists")
var ErrTimeout = errors.New("timeout")
var ErrNotFound = errors.New("not found")
var ErrShardUnavailable = errors.New("shard unavailable")
var ErrQuotaReached = errors.New("quota reached")
