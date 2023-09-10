package cluster

import (
	"bytes"
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/semafind/semadb/config"
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
	colDir := filepath.Join(config.Cfg.RootDir, col.UserId, col.Id)
	// ---------------------------
	// Check if the directory exists?
	if _, err := os.Stat(colDir); err != nil {
		return "", ErrNotFound
	}
	// ---------------------------
	// Create shard directory
	shardId := uuid.New().String()
	shardDir := filepath.Join(colDir, shardId)
	if err := os.MkdirAll(shardDir, os.ModePerm); err != nil {
		return "", fmt.Errorf("could not create shard directory: %w", err)
	}
	c.logger.Debug().Str("shardDir", shardDir).Msg("CreateShard")
	// ---------------------------
	return shardDir, nil
}

type shardInfo struct {
	Id         string
	ShardDir   string
	Size       int64
	PointCount int64
}

func (c *ClusterNode) GetShards(col models.Collection, withSize bool) ([]shardInfo, error) {
	// ---------------------------
	colDir := filepath.Join(config.Cfg.RootDir, col.UserId, col.Id)
	shardDirs, err := os.ReadDir(colDir)
	if err != nil {
		return nil, fmt.Errorf("could not read collection directory: %w", err)
	}
	// ---------------------------
	shards := make([]shardInfo, 0, len(shardDirs))
	for _, shardDir := range shardDirs {
		if !shardDir.IsDir() {
			continue
		}
		// ---------------------------
		fullShardDir := filepath.Join(colDir, shardDir.Name())
		si := shardInfo{
			Id:       shardDir.Name(),
			ShardDir: fullShardDir,
		}
		// ---------------------------
		if withSize {
			targetServer := RendezvousHash(si.ShardDir, c.Servers, 1)[0]
			getInfoRequest := RPCGetShardInfoRequest{
				RPCRequestArgs: RPCRequestArgs{
					Source: c.MyHostname,
					Dest:   targetServer,
				},
				ShardDir: fullShardDir,
			}
			getInfoResponse := RPCGetShardInfoResponse{}
			if err := c.RPCGetShardInfo(&getInfoRequest, &getInfoResponse); err != nil {
				c.logger.Error().Err(err).Str("shardDir", shardDir.Name()).Msg("could not get shard info")
				return nil, fmt.Errorf("could not get shard info: %w: %w", ErrShardUnavailable, err)
			}
			// ---------------------------
			si.Size = getInfoResponse.Size
			si.PointCount = getInfoResponse.PointCount
		}
		shards = append(shards, si)
	}
	// ---------------------------
	return shards, nil
}

func (c *ClusterNode) InsertPoints(col models.Collection, points []models.Point) ([][2]int, error) {
	// ---------------------------
	// This is where shard distribution happens
	shards, err := c.GetShards(col, true)
	if err != nil {
		return nil, fmt.Errorf("could not get shards: %w", err)
	}
	// ---------------------------
	// Distribute points to shards
	shardAssignments, err := distributePoints(shards, points, config.Cfg.MaxShardSize, config.Cfg.MaxShardPointCount, func() (string, error) {
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
	for shardDir, pointRange := range shardAssignments {
		wg.Add(1)
		go func(sDir string, pointRange [2]int) {
			// ---------------------------
			targetServer := RendezvousHash(sDir, c.Servers, 1)[0]
			shardPoints := points[pointRange[0]:pointRange[1]]
			insertReq := RPCInsertPointsRequest{
				RPCRequestArgs: RPCRequestArgs{
					Source: c.MyHostname,
					Dest:   targetServer,
				},
				ShardDir: sDir,
				Points:   shardPoints,
			}
			insertResp := RPCInsertPointsResponse{}
			if err := c.RPCInsertPoints(&insertReq, &insertResp); err != nil {
				c.logger.Error().Err(err).Str("shardDir", sDir).Msg("could not insert points")
				mu.Lock()
				failedRanges = append(failedRanges, pointRange)
				mu.Unlock()
			}
			wg.Done()
		}(shardDir, pointRange)
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
	shards, err := c.GetShards(col, false)
	if err != nil {
		return nil, fmt.Errorf("could not get shards: %w", err)
	}
	// ---------------------------
	// Search every shard in parallel. If a shard is unavailable, we will
	// simply ignore it for now to keep the search request alive.
	results := make([]shard.SearchPoint, 0, len(shards)*10)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errCount atomic.Int32
	for _, shardInfo := range shards {
		wg.Add(1)
		go func(sDir string) {
			defer wg.Done()
			targetServer := RendezvousHash(sDir, c.Servers, 1)[0]
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
			targetLimit := int(float32(limit)*(1/float32(len(shards)))*poissonApproxA + poissonApproxB)
			if targetLimit > config.Cfg.MaxSearchLimit {
				targetLimit = config.Cfg.MaxSearchLimit
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
				ShardDir: sDir,
				Vector:   query,
				Limit:    targetLimit,
			}
			searchResp := RPCSearchPointsResponse{}
			if err := c.RPCSearchPoints(&searchReq, &searchResp); err != nil {
				c.logger.Error().Err(err).Str("shardDir", sDir).Msg("could not search points")
				errCount.Add(1)
			} else {
				// Alternatively we can stream the results into a channel and
				// loop over. This is more straightforward for now.
				mu.Lock()
				results = append(results, searchResp.Points...)
				mu.Unlock()
			}
		}(shardInfo.ShardDir)
	}
	// ---------------------------
	// Merge results in a single slice. We could instead use a channel to stream
	// and merge results on the go but that adds more complexity which could be
	// future work.
	wg.Wait()
	if len(shards) > 0 && errCount.Load() == int32(len(shards)) {
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
	shards, err := c.GetShards(col, false)
	if err != nil {
		return nil, fmt.Errorf("could not get shards: %w", err)
	}
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
	for _, shardInfo := range shards {
		wg.Add(1)
		go func(sDir string) {
			defer wg.Done()
			targetServer := RendezvousHash(sDir, c.Servers, 1)[0]
			updateReq := RPCUpdatePointsRequest{
				RPCRequestArgs: RPCRequestArgs{
					Source: c.MyHostname,
					Dest:   targetServer,
				},
				ShardDir: sDir,
				Points:   points,
			}
			updateResp := RPCUpdatePointsResponse{}
			if err := c.RPCUpdatePoints(&updateReq, &updateResp); err != nil {
				c.logger.Error().Err(err).Str("shardDir", sDir).Msg("could not update points")
			} else {
				mu.Lock()
				results = append(results, updateResp.UpdatedIds...)
				mu.Unlock()
			}
		}(shardInfo.ShardDir)
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
