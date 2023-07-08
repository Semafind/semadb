package cluster

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/kvstore"
	"github.com/semafind/semadb/models"
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

type RPCUpsertPointsRequest struct {
	RequestArgs
	ShardDir string
	Points   []models.Point
}

type RPCUpsertPointsResponse struct {
	ErrMap map[uuid.UUID]error
}

func (c *ClusterNode) RPCUpsertPoints(args *RPCUpsertPointsRequest, reply *RPCUpsertPointsResponse) error {
	log.Debug().Str("shardDir", args.ShardDir).Str("host", c.MyHostname).Interface("route", args.RequestArgs).Msg("RPCUpsertPoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCUpsertPoints", args, reply)
	}
	// ---------------------------
	shard, err := c.LoadShard(args.ShardDir)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	results, err := shard.UpsertPoints(args.Points)
	reply.ErrMap = results
	return err
}

// ---------------------------

type RPCSearchPointsRequest struct {
	RequestArgs
	ShardDir string
	Vector   []float32
	Limit    int
}

type RPCSearchPointsResponse struct {
	Points []models.Point
}

func (c *ClusterNode) RPCSearchPoints(args *RPCSearchPointsRequest, reply *RPCSearchPointsResponse) error {
	log.Debug().Str("shardDir", args.ShardDir).Str("host", c.MyHostname).Interface("route", args.RequestArgs).Msg("RPCSearchPoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCSearchPoints", args, reply)
	}
	// ---------------------------
	shard, err := c.LoadShard(args.ShardDir)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	points, err := shard.SearchPoints(args.Vector, args.Limit)
	reply.Points = points
	return err
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

// ---------------------------

type ReadKVRequest struct {
	RequestArgs
	Key string
}

type ReadKVResponse struct {
	Value []byte
}

func (c *ClusterNode) RPCRead(args *ReadKVRequest, reply *ReadKVResponse) error {
	// ---------------------------
	log.Debug().Str("key", args.Key).Str("host", c.MyHostname).Interface("route", args.RequestArgs).Msg("RPCRead")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCRead", args, reply)
	}
	// ---------------------------
	value, err := c.kvstore.Read(args.Key)
	if err != nil {
		return err
	}
	reply.Value = value
	return nil
}
