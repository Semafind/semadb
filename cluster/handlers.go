package cluster

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
)

// ---------------------------

// type rpcPingRequest struct {
// 	rpcRequestArgs
// 	Message string
// }

// type rpcPingResponse struct {
// 	Message string
// }

// func (c *ClusterNode) rpcPing(args *rpcPingRequest, reply *rpcPingResponse) error {
// 	c.logger.Debug().Interface("args", args).Msg("Ping")
// 	if args.Dest != c.MyHostname {
// 		return c.internalRoute("ClusterNode.Ping", args, reply)
// 	}
// 	reply.Message = fmt.Sprintf("Pong from semadb %v, message: %v", c.MyHostname, args.Message)
// 	return nil
// }

// ---------------------------

type rpcUpsertPointsRequest struct {
	rpcRequestArgs
	ShardDir string
	Points   []models.Point
}

type rpcUpsertPointsResponse struct {
	ErrMap map[uuid.UUID]error
}

func (c *ClusterNode) rpcUpsertPoints(args *rpcUpsertPointsRequest, reply *rpcUpsertPointsResponse) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.rpcRequestArgs).Msg("RPCUpsertPoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCUpsertPoints", args, reply)
	}
	// ---------------------------
	shard, err := c.LoadShard(args.ShardDir)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	return shard.InsertPoints(args.Points)
}

// ---------------------------

type rpcSearchPointsRequest struct {
	rpcRequestArgs
	ShardDir string
	Vector   []float32
	Limit    int
}

type rpcSearchPointsResponse struct {
	Points []models.Point
}

func (c *ClusterNode) rpcSearchPoints(args *rpcSearchPointsRequest, reply *rpcSearchPointsResponse) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.rpcRequestArgs).Msg("RPCSearchPoints")
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
