package cluster

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard"
	"github.com/vmihailenco/msgpack/v5"
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

type RPCSetNodeKeyValueRequest struct {
	RPCRequestArgs
	Bucket    string
	KeyValues map[string][]byte
}

type RPCSetNodeKeyValueResponse struct {
	Count int
}

func (c *ClusterNode) RPCSetNodeKeyValue(args *RPCSetNodeKeyValueRequest, reply *RPCSetNodeKeyValueResponse) error {
	c.logger.Debug().Int("kvCount", len(args.KeyValues)).Str("Bucket", args.Bucket).Msg("RPCSetNodeKeyValue")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCSetNodeKeyValue", args, reply)
	}
	count := 0
	err := c.nodedb.Write(func(bm diskstore.BucketManager) error {
		b, err := bm.Get(args.Bucket)
		if err != nil {
			return fmt.Errorf("could not get write bucket %s: %w", args.Bucket, err)
		}
		for key, value := range args.KeyValues {
			if err := b.Put([]byte(key), value); err != nil {
				return fmt.Errorf("could not put key %s: %w", key, err)
			}
			count++
		}
		return nil
	})
	reply.Count = count
	return err
}

// ---------------------------

type RPCSendShardRequest struct {
	RPCRequestArgs
	UserId       string
	CollectionId string
	ShardId      string
	ChunkIndex   int
	ChunkData    []byte
}

type RPCSendShardResponse struct {
	BytesWritten int
	Checksum     uint64 // only used if EOF
}

func (c *ClusterNode) RPCSendShard(args *RPCSendShardRequest, reply *RPCSendShardResponse) error {
	c.logger.Debug().Str("userId", args.UserId).Str("collectionId", args.CollectionId).Str("shardId", args.ShardId).Int("chunkIndex", args.ChunkIndex).Int("chunkSize", len(args.ChunkData)).Msg("RPCSendShard")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCSendShard", args, reply)
	}
	// ---------------------------
	shardPath := filepath.Join(c.cfg.ShardManager.RootDir, USERCOLSDIR, args.UserId, args.CollectionId, args.ShardId, "sharddb.bbolt")
	if args.ChunkIndex == 0 {
		if err := os.MkdirAll(filepath.Dir(shardPath), 0755); err != nil {
			return fmt.Errorf("could not create shard directory: %w", err)
		}
	}
	// Append to shard file, create if it doesn't exist
	// Does this generate a lot of syscalls? If so, we can switch to buffered
	// writers but we need to keep track of the file descriptor across RPC
	// calls. Let's see if this is a problem first, we can optimize later.
	f, err := os.OpenFile(shardPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("could not open shard file: %w", err)
	}
	defer f.Close()
	n, err := f.Write(args.ChunkData)
	if err != nil {
		return fmt.Errorf("could not write shard file: %w", err)
	}
	reply.BytesWritten = n
	// ---------------------------
	// Compute final checksum
	if args.ChunkIndex > 0 && len(args.ChunkData) == 0 {
		f.Close()
		checksum, err := FileHash(shardPath)
		if err != nil {
			return fmt.Errorf("could not compute shard checksum: %w", err)
		}
		reply.Checksum = checksum
	}
	// ---------------------------
	return nil
}

// ---------------------------

type RPCCreateCollectionRequest struct {
	RPCRequestArgs
	Collection models.Collection
}

type RPCCreateCollectionResponse struct {
	AlreadyExists bool
	QuotaReached  bool
}

func (c *ClusterNode) RPCCreateCollection(args *RPCCreateCollectionRequest, reply *RPCCreateCollectionResponse) error {
	c.logger.Debug().Str("collectionId", args.Collection.Id).Msg("RPCCreateCollection")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCCreateCollection", args, reply)
	}
	// ---------------------------
	// Marshal collection
	colBytes, err := msgpack.Marshal(args.Collection)
	if err != nil {
		return fmt.Errorf("could not marshal collection: %w", err)
	}
	// ---------------------------
	err = c.nodedb.Write(func(bm diskstore.BucketManager) error {
		// ---------------------------
		b, err := bm.Get(USERCOLSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get write user collections bucket: %w", err)
		}
		// ---------------------------
		key := []byte(args.Collection.UserId + DBDELIMITER + args.Collection.Id)
		if b.Get(key) != nil {
			reply.AlreadyExists = true
			return nil
		}
		// ---------------------------
		// Check user quota
		prefix := []byte(args.Collection.UserId + DBDELIMITER)
		count := 0
		err = b.PrefixScan(prefix, func(k, _ []byte) error {
			count++
			return nil
		})
		if err != nil {
			return fmt.Errorf("could not scan user collections: %w", err)
		}
		if count >= args.Collection.UserPlan.MaxCollections {
			reply.QuotaReached = true
			return nil
		}
		// ---------------------------
		if err := b.Put(key, colBytes); err != nil {
			return fmt.Errorf("could not put collection: %w", err)
		}
		return nil
	})
	return err
}

// ---------------------------

type RPCDeleteCollectionRequest struct {
	RPCRequestArgs
	Collection models.Collection
}

type RPCDeleteCollectionResponse struct {
}

func (c *ClusterNode) RPCDeleteCollection(args *RPCDeleteCollectionRequest, reply *RPCDeleteCollectionResponse) error {
	c.logger.Debug().Str("userId", args.Collection.UserId).Str("collectionId", args.Collection.Id).Msg("RPCDeleteCollection")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCDeleteCollection", args, reply)
	}
	err := c.nodedb.Write(func(bm diskstore.BucketManager) error {
		// ---------------------------
		b, err := bm.Get(USERCOLSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get write user collections bucket: %w", err)
		}
		// ---------------------------
		if err := b.Delete([]byte(args.Collection.UserId + DBDELIMITER + args.Collection.Id)); err != nil {
			return fmt.Errorf("could not delete collection: %w", err)
		}
		return nil
	})
	return err
}

// ---------------------------

type RPCListCollectionsRequest struct {
	RPCRequestArgs
	UserId string
}

type RPCListCollectionsResponse struct {
	Collections []models.Collection
}

func (c *ClusterNode) RPCListCollections(args *RPCListCollectionsRequest, reply *RPCListCollectionsResponse) error {
	c.logger.Debug().Str("userId", args.UserId).Msg("RPCListCollections")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCListCollections", args, reply)
	}
	reply.Collections = make([]models.Collection, 0)
	err := c.nodedb.Read(func(bm diskstore.BucketManager) error {
		// ---------------------------
		b, err := bm.Get(USERCOLSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get read user collections bucket: %w", err)
		}
		// ---------------------------
		prefix := []byte(args.UserId + DBDELIMITER)
		err = b.PrefixScan(prefix, func(k, v []byte) error {
			var col models.Collection
			if err := msgpack.Unmarshal(v, &col); err != nil {
				return fmt.Errorf("could not unmarshal collection %s: %w", k, err)
			}
			reply.Collections = append(reply.Collections, col)
			return nil
		})
		return err
	})
	return err
}

// ---------------------------

type RPCGetCollectionRequest struct {
	RPCRequestArgs
	UserId       string
	CollectionId string
}

type RPCGetCollectionResponse struct {
	Collection models.Collection
	NotFound   bool
}

func (c *ClusterNode) RPCGetCollection(args *RPCGetCollectionRequest, reply *RPCGetCollectionResponse) error {
	c.logger.Debug().Str("userId", args.UserId).Str("collectionId", args.CollectionId).Msg("RPCGetCollection")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCGetCollection", args, reply)
	}
	err := c.nodedb.Read(func(bm diskstore.BucketManager) error {
		// ---------------------------
		b, err := bm.Get(USERCOLSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get read user collections bucket: %w", err)
		}
		// ---------------------------
		key := []byte(args.UserId + DBDELIMITER + args.CollectionId)
		value := b.Get(key)
		if value == nil {
			reply.NotFound = true
			return nil
		}
		// ---------------------------
		if err := msgpack.Unmarshal(value, &reply.Collection); err != nil {
			return fmt.Errorf("could not unmarshal collection %s: %w", key, err)
		}
		return nil
	})
	return err
}

// ---------------------------

type RPCCreateShardRequest struct {
	RPCRequestArgs
	UserId       string
	CollectionId string
}

type RPCCreateShardResponse struct {
	ShardId string
}

func (c *ClusterNode) RPCCreateShard(args *RPCCreateShardRequest, reply *RPCCreateShardResponse) error {
	c.logger.Debug().Str("userId", args.UserId).Str("collectionId", args.CollectionId).Msg("RPCCreateShard")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCCreateShard", args, reply)
	}
	err := c.nodedb.Write(func(bm diskstore.BucketManager) error {
		// ---------------------------
		b, err := bm.Get(USERCOLSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("could not get write user collections bucket: %w", err)
		}
		// ---------------------------
		key := []byte(args.UserId + DBDELIMITER + args.CollectionId)
		value := b.Get(key)
		if value == nil {
			return fmt.Errorf("collection %s not found", key)
		}
		var col models.Collection
		if err := msgpack.Unmarshal(value, &col); err != nil {
			return fmt.Errorf("could not unmarshal collection %s: %w", key, err)
		}
		// ---------------------------
		shardId := uuid.New().String()
		reply.ShardId = shardId
		col.ShardIds = append(col.ShardIds, shardId)
		// ---------------------------
		colBytes, err := msgpack.Marshal(col)
		if err != nil {
			return fmt.Errorf("could not marshal collection: %w", err)
		}
		// ---------------------------
		if err := b.Put(key, colBytes); err != nil {
			return fmt.Errorf("could not put collection: %w", err)
		}
		// ---------------------------
		return nil
	})
	return err
}

// ---------------------------

type RPCGetShardInfoRequest struct {
	RPCRequestArgs
	Collection models.Collection
	ShardId    string
}

type RPCGetShardInfoResponse struct {
	PointCount int64
	Size       int64
}

func (c *ClusterNode) RPCGetShardInfo(args *RPCGetShardInfoRequest, reply *RPCGetShardInfoResponse) error {
	c.logger.Debug().Str("userId", args.Collection.UserId).Str("collectionId", args.Collection.Id).Str("shardId", args.ShardId).Msg("RPCGetShardInfo")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCGetShardInfo", args, reply)
	}
	// ---------------------------
	return c.shardManager.DoWithShard(args.Collection, args.ShardId, func(s *shard.Shard) error {
		si, err := s.Info()
		reply.PointCount = int64(si.PointCount)
		reply.Size = si.Size
		return err
	})
}

// ---------------------------

type RPCDeleteCollectionShardsRequest struct {
	RPCRequestArgs
	Collection models.Collection
}

type RPCDeleteCollectionShardsResponse struct {
	DeletedShardIds []string
}

func (c *ClusterNode) RPCDeleteCollectionShards(args *RPCDeleteCollectionShardsRequest, reply *RPCDeleteCollectionShardsResponse) error {
	c.logger.Debug().Str("userId", args.Collection.UserId).Str("collectionId", args.Collection.Id).Msg("RPCDeleteCollectionShards")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCDeleteCollectionShards", args, reply)
	}
	deletedShardIds, err := c.shardManager.DeleteCollectionShards(args.Collection)
	reply.DeletedShardIds = deletedShardIds
	return err
}

// ---------------------------

type RPCInsertPointsRequest struct {
	RPCRequestArgs
	Collection models.Collection
	ShardId    string
	Points     []models.Point
}

// This response is not really used, but we need to return something otherwise
// enc/glob fails. That is, we can have reply be nil. We can use this response
// in the future.
type RPCInsertPointsResponse struct {
	Count int
}

func (c *ClusterNode) RPCInsertPoints(args *RPCInsertPointsRequest, reply *RPCInsertPointsResponse) error {
	c.logger.Debug().Str("userId", args.Collection.UserId).Str("collectionId", args.Collection.Id).Str("shardId", args.ShardId).Msg("RPCInsertPoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCInsertPoints", args, reply)
	}
	// ---------------------------
	return c.shardManager.DoWithShard(args.Collection, args.ShardId, func(s *shard.Shard) error {
		err := s.InsertPoints(args.Points)
		if err == nil {
			reply.Count = len(args.Points)
			c.metrics.pointInsertCount.Add(float64(len(args.Points)))
		}
		return err
	})
}

// ---------------------------

type RPCUpdatePointsRequest struct {
	RPCRequestArgs
	Collection models.Collection
	ShardId    string
	Points     []models.Point
}

type RPCUpdatePointsResponse struct {
	UpdatedIds []uuid.UUID
}

func (c *ClusterNode) RPCUpdatePoints(args *RPCUpdatePointsRequest, reply *RPCUpdatePointsResponse) error {
	c.logger.Debug().Str("userId", args.Collection.UserId).Str("collectionId", args.Collection.Id).Str("shardId", args.ShardId).Msg("RPCUpdatePoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCUpdatePoints", args, reply)
	}
	// ---------------------------
	return c.shardManager.DoWithShard(args.Collection, args.ShardId, func(s *shard.Shard) error {
		updatedIds, err := s.UpdatePoints(args.Points)
		reply.UpdatedIds = updatedIds
		if err == nil {
			c.metrics.pointUpdateCount.Add(float64(len(updatedIds)))
		}
		return err
	})
}

// ---------------------------

type RPCDeletePointsRequest struct {
	RPCRequestArgs
	Collection models.Collection
	ShardId    string
	Ids        []uuid.UUID
}

type RPCDeletePointsResponse struct {
	DeletedIds []uuid.UUID
}

func (c *ClusterNode) RPCDeletePoints(args *RPCDeletePointsRequest, reply *RPCDeletePointsResponse) error {
	c.logger.Debug().Str("userId", args.Collection.UserId).Str("collectionId", args.Collection.Id).Str("shardId", args.ShardId).Msg("RPCDeletePoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCDeletePoints", args, reply)
	}
	// ---------------------------
	deleteSet := make(map[uuid.UUID]struct{})
	for _, id := range args.Ids {
		deleteSet[id] = struct{}{}
	}
	return c.shardManager.DoWithShard(args.Collection, args.ShardId, func(s *shard.Shard) error {
		delIds, err := s.DeletePoints(deleteSet)
		reply.DeletedIds = delIds
		if err == nil {
			c.metrics.pointDeleteCount.Add(float64(len(delIds)))
		}
		return err
	})
}

// ---------------------------

type RPCSearchPointsRequest struct {
	RPCRequestArgs
	Collection    models.Collection
	ShardId       string
	SearchRequest models.SearchRequest
}

type RPCSearchPointsResponse struct {
	Points []models.SearchResult
}

func (c *ClusterNode) RPCSearchPoints(args *RPCSearchPointsRequest, reply *RPCSearchPointsResponse) error {
	c.logger.Debug().Str("userId", args.Collection.UserId).Str("collectionId", args.Collection.Id).Str("shardId", args.ShardId).Msg("RPCSearchPoints")
	if args.Dest != c.MyHostname {
		return c.internalRoute("ClusterNode.RPCSearchPoints", args, reply)
	}
	// ---------------------------
	return c.shardManager.DoWithShard(args.Collection, args.ShardId, func(s *shard.Shard) error {
		points, err := s.SearchPoints(args.SearchRequest)
		reply.Points = points
		if err == nil {
			c.metrics.pointSearchCount.Add(float64(len(points)))
		}
		return err
	})
}

// ---------------------------
