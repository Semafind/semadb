package cluster

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/kvstore"
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

func (c *ClusterNode) RPCWrite(args *WriteKVRequest, reply *WriteKVResponse) error {
	// ---------------------------
	log.Debug().Str("key", args.Key).Str("host", c.MyHostname).Interface("route", args.RequestArgs).Msg("RPCWrite")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCWrite", args, reply)
	}
	// ---------------------------
	repLog, err := c.createRepLogEntry(args.Key, args.Value)
	if err != nil {
		return fmt.Errorf("could not create replog entry: %w", err)
	}
	if err := c.kvstore.WriteAsRepLog(repLog); err != nil {
		return err
	}
	return nil
}

// ---------------------------

type ScanKVRequest struct {
	RequestArgs
	Prefix string
}

type ScanKVResponse struct {
	Entries []kvstore.KVEntry
}

func (c *ClusterNode) RPCScan(args *ScanKVRequest, reply *ScanKVResponse) error {
	// ---------------------------
	log.Debug().Str("prefix", args.Prefix).Str("host", c.MyHostname).Interface("route", args.RequestArgs).Msg("RPCScan")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCScan", args, reply)
	}
	// ---------------------------
	entries, err := c.kvstore.ScanPrefix(args.Prefix)
	if err != nil {
		return err
	}
	reply.Entries = entries
	return nil
}
