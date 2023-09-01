package cluster

import (
	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard"
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

type RPCInsertPointsRequest struct {
	RPCRequestArgs
	ShardDir string
	Points   []models.Point
}

func (c *ClusterNode) RPCInsertPoints(args *RPCInsertPointsRequest, reply *struct{}) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.RPCRequestArgs).Msg("RPCInsertPoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCInsertPoints", args, reply)
	}
	// ---------------------------
	return c.DoWithShard(args.ShardDir, func(s *shard.Shard) error {
		return s.InsertPoints(args.Points)
	})
}

// ---------------------------

type RPCUpdatePointsRequest struct {
	RPCRequestArgs
	ShardDir string
	Points   []models.Point
}

type RPCUpdatePointsResponse struct {
	ErrMap map[uuid.UUID]error
}

func (c *ClusterNode) RPCUpdatePoints(args *RPCUpdatePointsRequest, reply *RPCUpdatePointsResponse) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.RPCRequestArgs).Msg("RPCUpdatePoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCUpdatePoints", args, reply)
	}
	// ---------------------------
	return c.DoWithShard(args.ShardDir, func(s *shard.Shard) error {
		errMap, err := s.UpdatePoints(args.Points)
		reply.ErrMap = errMap
		return err
	})
}

// ---------------------------

type RPCDeletePointsRequest struct {
	RPCRequestArgs
	ShardDir string
	Ids      []uuid.UUID
}

func (c *ClusterNode) RPCDeletePoints(args *RPCDeletePointsRequest, reply *struct{}) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.RPCRequestArgs).Msg("RPCDeletePoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCDeletePoints", args, reply)
	}
	// ---------------------------
	deleteSet := make(map[uuid.UUID]struct{})
	for _, id := range args.Ids {
		deleteSet[id] = struct{}{}
	}
	return c.DoWithShard(args.ShardDir, func(s *shard.Shard) error {
		return s.DeletePoints(deleteSet)
	})
}

// ---------------------------

type RPCGetShardInfoRequest struct {
	RPCRequestArgs
	ShardDir string
}

type RPCGetShardInfoResponse struct {
	PointCount int64
	Size       int64
}

func (c *ClusterNode) RPCGetShardInfo(args *RPCGetShardInfoRequest, reply *RPCGetShardInfoResponse) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.RPCRequestArgs).Msg("RPCGetShardInfo")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCGetShardInfo", args, reply)
	}
	// ---------------------------
	return c.DoWithShard(args.ShardDir, func(s *shard.Shard) error {
		si, err := s.Info()
		reply.PointCount = si.PointCount
		reply.Size = si.InUse
		return err
	})
}

// ---------------------------

type RPCSearchPointsRequest struct {
	RPCRequestArgs
	ShardDir string
	Vector   []float32
	Limit    int
}

type RPCSearchPointsResponse struct {
	Points []shard.SearchPoint
}

func (c *ClusterNode) RPCSearchPoints(args *RPCSearchPointsRequest, reply *RPCSearchPointsResponse) error {
	c.logger.Debug().Str("shardDir", args.ShardDir).Interface("route", args.RPCRequestArgs).Msg("RPCSearchPoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCSearchPoints", args, reply)
	}
	// ---------------------------
	return c.DoWithShard(args.ShardDir, func(s *shard.Shard) error {
		points, err := s.SearchPoints(args.Vector, args.Limit)
		reply.Points = points
		return err
	})
}

// ---------------------------
