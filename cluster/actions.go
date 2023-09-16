package cluster

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard"
)

func (c *ClusterNode) CreateCollection(collection models.Collection) error {
	// ---------------------------
	// The collection information is stored in the user cluster node. We
	// construct the appropiate request and route it.
	rpcReq := RPCCreateCollectionRequest{
		RPCRequestArgs: RPCRequestArgs{
			Source: c.MyHostname,
			Dest:   RendezvousHash(collection.UserId, c.Servers, 1)[0],
		},
		Collection: collection,
	}
	rpcResp := RPCCreateCollectionResponse{}
	if err := c.RPCCreateCollection(&rpcReq, &rpcResp); err != nil {
		return fmt.Errorf("could not create collection: %w", err)
	}
	if rpcResp.AlreadyExists {
		return ErrExists
	}
	// ---------------------------
	return nil
}

func (c *ClusterNode) ListCollections(userId string) ([]models.Collection, error) {
	// ---------------------------
	rpcReq := RPCListCollectionsRequest{
		RPCRequestArgs: RPCRequestArgs{
			Source: c.MyHostname,
			Dest:   RendezvousHash(userId, c.Servers, 1)[0],
		},
		UserId: userId,
	}
	rpcResp := RPCListCollectionsResponse{}
	if err := c.RPCListCollections(&rpcReq, &rpcResp); err != nil {
		return nil, fmt.Errorf("could not list collections: %w", err)
	}
	// ---------------------------
	return rpcResp.Collections, nil
}

func (c *ClusterNode) GetCollection(userId string, collectionId string) (models.Collection, error) {
	// ---------------------------
	rpcReq := RPCGetCollectionRequest{
		RPCRequestArgs: RPCRequestArgs{
			Source: c.MyHostname,
			Dest:   RendezvousHash(userId, c.Servers, 1)[0],
		},
		UserId:       userId,
		CollectionId: collectionId,
	}
	rpcResp := RPCGetCollectionResponse{}
	if err := c.RPCGetCollection(&rpcReq, &rpcResp); err != nil {
		return models.Collection{}, fmt.Errorf("could not get collection: %w", err)
	}
	// ---------------------------
	if rpcResp.NotFound {
		return models.Collection{}, ErrNotFound
	}
	// ---------------------------
	return rpcResp.Collection, nil
}

func (c *ClusterNode) CreateShard(col models.Collection) (string, error) {
	// ---------------------------
	rpcRequest := RPCCreateShardRequest{
		RPCRequestArgs: RPCRequestArgs{
			Source: c.MyHostname,
			Dest:   RendezvousHash(col.UserId, c.Servers, 1)[0],
		},
		UserId:       col.UserId,
		CollectionId: col.Id,
	}
	rpcResponse := RPCCreateShardResponse{}
	if err := c.RPCCreateShard(&rpcRequest, &rpcResponse); err != nil {
		return "", fmt.Errorf("could not create shard: %w", err)
	}
	// ---------------------------
	return rpcResponse.ShardId, nil
}

type shardInfo struct {
	Id         string
	Size       int64
	PointCount int64
}

func (c *ClusterNode) GetShardsInfo(col models.Collection) ([]shardInfo, error) {
	// ---------------------------
	shards := make([]shardInfo, 0, len(col.ShardIds))
	for _, shardId := range col.ShardIds {
		// ---------------------------
		targetServer := RendezvousHash(shardId, c.Servers, 1)[0]
		getInfoRequest := RPCGetShardInfoRequest{
			RPCRequestArgs: RPCRequestArgs{
				Source: c.MyHostname,
				Dest:   targetServer,
			},
			Collection: col,
			ShardId:    shardId,
		}
		getInfoResponse := RPCGetShardInfoResponse{}
		if err := c.RPCGetShardInfo(&getInfoRequest, &getInfoResponse); err != nil {
			c.logger.Error().Err(err).Str("userId", col.UserId).Str("collectionId", col.Id).Str("shardId", shardId).Msg("could not get shard info")
			return nil, fmt.Errorf("could not get shard info: %w: %w", ErrShardUnavailable, err)
		}
		// ---------------------------
		si := shardInfo{
			Id:         shardId,
			Size:       getInfoResponse.Size,
			PointCount: getInfoResponse.PointCount,
		}
		shards = append(shards, si)
	}
	// ---------------------------
	return shards, nil
}

// ---------------------------

func (c *ClusterNode) DeleteCollection(col models.Collection) ([]string, error) {
	// ---------------------------
	// Delete the collection entry first
	deleteColReq := RPCDeleteCollectionRequest{
		RPCRequestArgs: RPCRequestArgs{
			Source: c.MyHostname,
			Dest:   RendezvousHash(col.UserId, c.Servers, 1)[0],
		},
		Collection: col,
	}
	if err := c.RPCDeleteCollection(&deleteColReq, &RPCDeleteCollectionResponse{}); err != nil {
		return nil, fmt.Errorf("could not delete collection: %w", err)
	}
	// ---------------------------
	// Delete all shards as a best effort service
	targetServers := make([]string, 0, len(col.ShardIds))
	for _, shardId := range col.ShardIds {
		targetServers = append(targetServers, RendezvousHash(shardId, c.Servers, 1)[0])
	}
	// ---------------------------
	// Contact all shard servers
	deletedShardIds := make([]string, 0, len(col.ShardIds))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, targetServer := range targetServers {
		wg.Add(1)
		// ---------------------------
		go func(tServer string) {
			deleteShardRequest := RPCDeleteCollectionShardsRequest{
				RPCRequestArgs: RPCRequestArgs{
					Source: c.MyHostname,
					Dest:   tServer,
				},
				Collection: col,
			}
			deleteShardResponse := RPCDeleteCollectionShardsResponse{}
			if err := c.RPCDeleteCollectionShards(&deleteShardRequest, &deleteShardResponse); err != nil {
				c.logger.Error().Err(err).Str("userId", col.UserId).Str("collectionId", col.Id).Msg("Could not delete collecion shards")
			} else {
				mu.Lock()
				deletedShardIds = append(deletedShardIds, deleteShardResponse.DeletedShardIds...)
				mu.Unlock()
			}
			wg.Done()
		}(targetServer)
		// ---------------------------
	}
	wg.Wait()
	// ---------------------------
	return deletedShardIds, nil
}

// ---------------------------

func (c *ClusterNode) InsertPoints(col models.Collection, points []models.Point) ([][2]int, error) {
	// ---------------------------
	// This is where shard distribution happens
	shards, err := c.GetShardsInfo(col)
	if err != nil {
		return nil, fmt.Errorf("could not get shards: %w", err)
	}
	// ---------------------------
	// Distribute points to shards
	shardAssignments, err := distributePoints(shards, points, c.cfg.MaxShardSize, c.cfg.MaxShardPointCount, func() (string, error) {
		return c.CreateShard(col)
	})
	if err != nil {
		return nil, fmt.Errorf("could not distribute points: %w", err)
	}
	// ---------------------------
	// Insert points
	failedRanges := make([][2]int, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for shardId, pointRange := range shardAssignments {
		wg.Add(1)
		go func(sId string, pRange [2]int) {
			// ---------------------------
			targetServer := RendezvousHash(sId, c.Servers, 1)[0]
			shardPoints := points[pRange[0]:pRange[1]]
			insertReq := RPCInsertPointsRequest{
				RPCRequestArgs: RPCRequestArgs{
					Source: c.MyHostname,
					Dest:   targetServer,
				},
				Collection: col,
				ShardId:    sId,
				Points:     shardPoints,
			}
			insertResp := RPCInsertPointsResponse{}
			if err := c.RPCInsertPoints(&insertReq, &insertResp); err != nil {
				c.logger.Error().Err(err).Str("userId", col.UserId).Str("collectionId", col.Id).Str("shardId", sId).Msg("could not insert points")
				mu.Lock()
				failedRanges = append(failedRanges, pRange)
				mu.Unlock()
			}
			wg.Done()
		}(shardId, pointRange)
	}
	// ---------------------------
	// Wait for all insertions to finish
	wg.Wait()
	// ---------------------------
	return failedRanges, nil
}

// These are the parameters for the linear approximation of the inverse of the
// CDF of the Poisson distribution for the number of shards to search and limit
// around 100 to 1000 points. It allows us to limit the shard search to reduce
// the number of points to discard. See the SearchPoints function for more
// information.
const poissonApproxA = 1.42
const poissonApproxB = 10.0

func (c *ClusterNode) SearchPoints(col models.Collection, query []float32, limit int) ([]shard.SearchPoint, error) {
	// ---------------------------
	// Search every shard in parallel. If a shard is unavailable, we will
	// simply ignore it for now to keep the search request alive.
	results := make([]shard.SearchPoint, 0, len(col.ShardIds)*10)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errCount atomic.Int32
	for _, shardId := range col.ShardIds {
		wg.Add(1)
		go func(sId string) {
			defer wg.Done()
			targetServer := RendezvousHash(sId, c.Servers, 1)[0]
			// ---------------------------
			// Here we calculate the target limit for each shard. We want to
			// reduce the number of points discarded. For example, 5 chards with
			// a limit of 100 would fetch 500 points and then discard 400 to
			// return the desired count back to the user. Instead, we start by
			// assuming each shard has equal number of points. This means on
			// average we would expect true desired search points to be equally
			// and randomly distributed across all shards. For 5 shards and
			// limit 100, we expect 20 points per shard. We use the poisson
			// distribution to find the upper bound on its CDF at 0.99
			// percentile. So we set the lambda to equal (limit / numShards) =
			// 20 and calculate the 0.99 percentile. In this case, it will
			// sample >20 points per shard to account for the randomness but
			// perhaps not 100 points reducing computation required. The inverse
			// of Poisson CDF doesn't have a closed form, so we use a linear
			// approximation for our expected operational ranges for lambda.
			targetLimit := int(float32(limit)*(1/float32(len(col.ShardIds)))*poissonApproxA + poissonApproxB)
			if targetLimit > c.cfg.MaxSearchLimit {
				targetLimit = c.cfg.MaxSearchLimit
			}
			if targetLimit > limit {
				targetLimit = limit
			}
			// We don't check for minimum since it will be at least poissonApproxB = 10
			// ---------------------------
			searchReq := RPCSearchPointsRequest{
				RPCRequestArgs: RPCRequestArgs{
					Source: c.MyHostname,
					Dest:   targetServer,
				},
				Collection: col,
				ShardId:    sId,
				Vector:     query,
				Limit:      targetLimit,
			}
			searchResp := RPCSearchPointsResponse{}
			if err := c.RPCSearchPoints(&searchReq, &searchResp); err != nil {
				c.logger.Error().Err(err).Str("userId", col.UserId).Str("collectionId", col.Id).Str("shardId", sId).Msg("could not search points")
				errCount.Add(1)
			} else {
				// Alternatively we can stream the results into a channel and
				// loop over. This is more straightforward for now.
				mu.Lock()
				results = append(results, searchResp.Points...)
				mu.Unlock()
			}
		}(shardId)
	}
	// ---------------------------
	// Merge results in a single slice. We could instead use a channel to stream
	// and merge results on the go but that adds more complexity which could be
	// future work.
	wg.Wait()
	if len(col.ShardIds) > 0 && errCount.Load() == int32(len(col.ShardIds)) {
		return nil, fmt.Errorf("could not search any shards")
	}
	slices.SortFunc(results, func(a, b shard.SearchPoint) int {
		return cmp.Compare(a.Distance, b.Distance)
	})
	// Take the top limit points
	if len(results) > limit {
		results = results[:limit]
	}
	// ---------------------------
	return results, nil
}

// ---------------------------

func (c *ClusterNode) UpdatePoints(col models.Collection, points []models.Point) ([]uuid.UUID, error) {
	// ---------------------------
	// The update request is similar to the search request except we need to
	// request every shard to participate. This is because we don't keep a table
	// of which points map to which shards and that the number of shards can
	// change dynamically making it difficult to keep up. A potential solution
	// is to keep a table or using a consistent hashing algorithm. Because at
	// the moment we fill shards in order without any rebalancing, its a fair
	// starting point to probe all shards for the update request since only 1
	// shard will have the point.
	results := make([]uuid.UUID, 0, len(points))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, shardId := range col.ShardIds {
		wg.Add(1)
		go func(sId string) {
			defer wg.Done()
			targetServer := RendezvousHash(sId, c.Servers, 1)[0]
			updateReq := RPCUpdatePointsRequest{
				RPCRequestArgs: RPCRequestArgs{
					Source: c.MyHostname,
					Dest:   targetServer,
				},
				Collection: col,
				ShardId:    sId,
				Points:     points,
			}
			updateResp := RPCUpdatePointsResponse{}
			if err := c.RPCUpdatePoints(&updateReq, &updateResp); err != nil {
				c.logger.Error().Err(err).Str("userId", col.UserId).Str("collectionId", col.Id).Str("shardId", sId).Msg("could not update points")
			} else {
				mu.Lock()
				results = append(results, updateResp.UpdatedIds...)
				mu.Unlock()
			}
		}(shardId)
	}
	// ---------------------------
	wg.Wait()
	// *** Return which points were NOT updated. ***
	// This is because we want to notify clients / user what failed but internally
	// we communicate what succeeded. Instead of every shard saying what failed,
	// we can just say more concisely what succeeded to reduce traffic size.
	slices.SortFunc(results, func(a, b uuid.UUID) int {
		return bytes.Compare(a[:], b[:])
	})
	successSize := len(results)
	for _, point := range points {
		_, found := slices.BinarySearchFunc(results, point.Id, func(a, b uuid.UUID) int {
			return bytes.Compare(a[:], b[:])
		})
		if !found {
			// Because results has capacity of len(points), we can just append
			// and then slice. This re-uses the allocated memory.
			results = append(results, point.Id)
		}
	}
	// ---------------------------
	return results[successSize:], nil
}

func (c *ClusterNode) DeletePoints(col models.Collection, pointIds []uuid.UUID) ([]uuid.UUID, error) {
	// ---------------------------
	// Deleting points is similar to updating points and we ask every shard to
	// participate. This is because we don't have a table of point ids to shard
	// ids, so we need every shard to check if it has the point.
	//
	// The shard operation returns which ids succeded and this function returns
	// which ones failed to notify the client upstream. This is because it is
	// more efficient to just let shards return what succeeded instead of a long
	// list of points that failed.
	// ---------------------------
	deletedIds := make([]uuid.UUID, 0, len(pointIds))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, shardId := range col.ShardIds {
		wg.Add(1)
		go func(sId string) {
			defer wg.Done()
			targetServer := RendezvousHash(sId, c.Servers, 1)[0]
			deleteReq := RPCDeletePointsRequest{
				RPCRequestArgs: RPCRequestArgs{
					Source: c.MyHostname,
					Dest:   targetServer,
				},
				Collection: col,
				ShardId:    sId,
				Ids:        pointIds,
			}
			deleteResp := RPCDeletePointsResponse{}
			if err := c.RPCDeletePoints(&deleteReq, &deleteResp); err != nil {
				c.logger.Error().Err(err).Str("userId", col.UserId).Str("collectionId", col.Id).Str("shardId", sId).Msg("could not delete points")
			} else {
				mu.Lock()
				deletedIds = append(deletedIds, deleteResp.DeletedIds...)
				mu.Unlock()
			}
		}(shardId)
	}
	// ---------------------------
	wg.Wait()
	// *** Return which points were NOT deleted. ***
	slices.SortFunc(deletedIds, func(a, b uuid.UUID) int {
		return bytes.Compare(a[:], b[:])
	})
	successSize := len(deletedIds)
	for _, pointId := range pointIds {
		_, found := slices.BinarySearchFunc(deletedIds, pointId, func(a, b uuid.UUID) int {
			return bytes.Compare(a[:], b[:])
		})
		if !found {
			deletedIds = append(deletedIds, pointId)
		}
	}
	// ---------------------------
	return deletedIds[successSize:], nil
}
