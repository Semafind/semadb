package cluster

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

// ---------------------------

type PingRequest struct {
	RequestArgs
	Message string
}

type PingResponse struct {
	Message string
}

func (c *ClusterNode) Ping(args *PingRequest, reply *PingResponse) error {
	log.Debug().Interface("args", args).Str("host", c.MyHostname).Msg("Ping")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.Ping", args, reply)
	}
	reply.Message = fmt.Sprintf("Pong from semadb %v, message: %v", c.MyHostname, args.Message)
	return nil
}

// ---------------------------

type WriteKVRequest struct {
	RequestArgs
	Key   string
	Value []byte
}

type WriteKVResponse struct {
}

func (c *ClusterNode) WriteKV(args *WriteKVRequest, reply *WriteKVResponse) error {
	log.Debug().Str("key", string(args.Key)).Str("host", c.MyHostname).Msg("WriteKV")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.WriteKV", args, reply)
	}
	if err := c.kvstore.Insert([]byte(args.Key), args.Value); err != nil {
		return err
	}
	// TODO: Ideally these should be done in a single transaction to make
	// replication and writing atomic
	if err := c.addRepLogEntry(args.Key, args.Value); err != nil {
		return err
	}
	return nil
}
