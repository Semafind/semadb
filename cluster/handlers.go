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

type rpcInsertPointsRequest struct {
	rpcRequestArgs
	ShardDir string
	Points   []models.Point
}

func (c *ClusterNode) RPCInsertPoints(args *rpcInsertPointsRequest, reply *struct{}) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.rpcRequestArgs).Msg("RPCInsertPoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCInsertPoints", args, reply)
	}
	// ---------------------------
	shard, err := c.LoadShard(args.ShardDir)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	return shard.InsertPoints(args.Points)
}

// ---------------------------

type rpcUpdatePointsRequest struct {
	rpcRequestArgs
	ShardDir string
	Points   []models.Point
}

type rpcUpdatePointsResponse struct {
	ErrMap map[uuid.UUID]error
}

func (c *ClusterNode) RPCUpdatePoints(args *rpcUpdatePointsRequest, reply *rpcUpdatePointsResponse) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.rpcRequestArgs).Msg("RPCUpdatePoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCUpdatePoints", args, reply)
	}
	// ---------------------------
	shard, err := c.LoadShard(args.ShardDir)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	errMap, err := shard.UpdatePoints(args.Points)
	reply.ErrMap = errMap
	return err
}

// ---------------------------

type rpcDeletePointsRequest struct {
	rpcRequestArgs
	ShardDir string
	Ids      []uuid.UUID
}

func (c *ClusterNode) RPCDeletePoints(args *rpcDeletePointsRequest, reply *struct{}) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.rpcRequestArgs).Msg("RPCDeletePoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCDeletePoints", args, reply)
	}
	// ---------------------------
	shard, err := c.LoadShard(args.ShardDir)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	deleteSet := make(map[uuid.UUID]struct{})
	for _, id := range args.Ids {
		deleteSet[id] = struct{}{}
	}
	return shard.DeletePoints(deleteSet)
}

// ---------------------------

type rpcGetPointCountRequest struct {
	rpcRequestArgs
	ShardDir string
}

type rpcGetPointCountResponse struct {
	Count int64
}

func (c *ClusterNode) RPCGetPointCount(args *rpcGetPointCountRequest, reply *rpcGetPointCountResponse) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.rpcRequestArgs).Msg("RPCGetPointCount")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCGetPointCount", args, reply)
	}
	// ---------------------------
	shard, err := c.LoadShard(args.ShardDir)
	if err != nil {
		return fmt.Errorf("could not load shard: %w", err)
	}
	count, err := shard.GetPointCount()
	reply.Count = count
	return err
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

func (c *ClusterNode) RPCSearchPoints(args *rpcSearchPointsRequest, reply *rpcSearchPointsResponse) error {
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
