package cluster

import (
	"fmt"

	"github.com/google/uuid"
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
	c.logger.Debug().Interface("args", args).Msg("Ping")
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
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.RequestArgs).Msg("RPCUpsertPoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCUpsertPoints", args, reply)
	}
	// ---------------------------
	shard, err := c.LoadShard(args.ShardDir)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	return shard.UpsertPoints(args.Points)
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
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.RequestArgs).Msg("RPCSearchPoints")
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
