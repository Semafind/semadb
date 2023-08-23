package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack"
)

func (c *ClusterNode) CreateCollection(collection models.Collection) error {
	// ---------------------------
	// Construct file path
	fpath := filepath.Join(config.Cfg.RootDir, collection.UserId, collection.Id, "collection.msgpack")
	// ---------------------------
	// Check if the file exists?
	if _, err := os.Stat(fpath); err == nil {
		return ErrExists
	}
	// ---------------------------
	// Create parent directories
	if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
		return fmt.Errorf("could not create collection directory: %w", err)
	}
	// ---------------------------
	// Marshal collection
	colBytes, err := msgpack.Marshal(collection)
	if err != nil {
		return fmt.Errorf("could not marshal collection: %w", err)
	}
	// ---------------------------
	// Write to file
	c.logger.Debug().Str("fpath", fpath).Msg("CreateCollection")
	if err := os.WriteFile(fpath, colBytes, os.ModePerm); err != nil {
		return fmt.Errorf("could not write collection file: %w", err)
	}
	// ---------------------------
	return nil
}

func (c *ClusterNode) ListCollections(userId string) ([]models.Collection, error) {
	// ---------------------------
	dirPath := filepath.Join(config.Cfg.RootDir, userId)
	colDirs, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("could not read user directory: %w", err)
	}
	// ---------------------------
	// Iterate through collection directories
	collections := make([]models.Collection, 0, 1)
	errCount := 0
	for _, colDir := range colDirs {
		if !colDir.IsDir() {
			continue
		}
		metaFile := filepath.Join(dirPath, colDir.Name(), "collection.msgpack")
		// ---------------------------
		// Read collection file
		colBytes, err := os.ReadFile(metaFile)
		if err != nil {
			c.logger.Error().Err(err).Str("metaFile", metaFile).Msg("could not read collection file")
			errCount++
			continue
		}
		var col models.Collection
		if err := msgpack.Unmarshal(colBytes, &col); err != nil {
			c.logger.Error().Err(err).Str("metaFile", metaFile).Msg("could not unmarshal collection file")
			errCount++
			continue
		}
		collections = append(collections, col)
	}
	// ---------------------------
	// Construct key and value
	if errCount > 0 && len(collections) == 0 {
		return nil, fmt.Errorf("could not read any collections")
	}
	// ---------------------------
	return collections, nil
}

func (c *ClusterNode) GetCollection(userId string, collectionId string) (models.Collection, error) {
	// ---------------------------
	fpath := filepath.Join(config.Cfg.RootDir, userId, collectionId, "collection.msgpack")
	// ---------------------------
	colBytes, err := os.ReadFile(fpath)
	if errors.Is(err, os.ErrNotExist) {
		return models.Collection{}, ErrNotFound
	} else if err != nil {
		return models.Collection{}, fmt.Errorf("could not read collection file: %w", err)
	}
	// ---------------------------
	var collection models.Collection
	if err := msgpack.Unmarshal(colBytes, &collection); err != nil {
		return models.Collection{}, fmt.Errorf("could not unmarshal collection file: %w", err)
	}
	// ---------------------------
	return collection, nil
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
	ShardDir string
	Size     int64
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
		// Get db size
		dbSize := int64(0)
		fullShardDir := filepath.Join(colDir, shardDir.Name())
		if withSize {
			dbFile := filepath.Join(fullShardDir, "db")
			dbStat, err := os.Stat(dbFile)
			if err != nil {
				c.logger.Error().Err(err).Str("dbFile", dbFile).Msg("could not stat db file")
				continue
			}
			c.logger.Debug().Str("dbFile", dbFile).Int64("dbSize", dbStat.Size()).Msg("GetShards")
			dbSize = dbStat.Size()
		}
		// ---------------------------
		si := shardInfo{
			ShardDir: fullShardDir,
			Size:     dbSize,
		}
		shards = append(shards, si)
	}
	// ---------------------------
	return shards, nil
}

func (c *ClusterNode) UpsertPoints(col models.Collection, points []models.Point) ([]error, error) {
	// ---------------------------
	// This is where shard distribution happens
	shards, err := c.GetShards(col, true)
	if err != nil {
		return nil, fmt.Errorf("could not get shards: %w", err)
	}
	// ---------------------------
	if len(shards) == 0 {
		newShard, err := c.CreateShard(col)
		if err != nil {
			return nil, fmt.Errorf("could not create shard: %w", err)
		}
		shards = append(shards, shardInfo{
			ShardDir: newShard,
			Size:     0,
		}) // empty shard
	}
	// ---------------------------
	// Distribute points to shards
	shardAssignments := make(map[string][2]int)
	lastShardIndex, lastPointIndex := 0, 0
	runningSize := shards[lastShardIndex].Size
	for i, point := range points {
		runningSize += int64(len(point.Metadata)+len(point.Vector)*4) + 8 // 8 bytes for id
		if runningSize > config.Cfg.MaxShardSize {
			// This shard can't take any more points, did it take any points?
			if i > lastPointIndex {
				// This shard took some points
				shardAssignments[shards[lastShardIndex].ShardDir] = [2]int{lastPointIndex, i}
				lastPointIndex = i
			}
			// Did we reach the last shard?
			if lastShardIndex == len(shards)-1 {
				// We need more shards to take the rest of the points
				newShard, err := c.CreateShard(col)
				if err != nil {
					return nil, fmt.Errorf("could not create shard: %w", err)
				}
				shards = append(shards, shardInfo{
					ShardDir: newShard,
					Size:     0,
				}) // empty shard
			}
			lastShardIndex++
			runningSize = shards[lastShardIndex].Size
		}
	}
	if lastPointIndex < len(points) {
		shardAssignments[shards[lastShardIndex].ShardDir] = [2]int{lastPointIndex, len(points)}
	}
	// ---------------------------
	// Insert points
	for shardId, pointRange := range shardAssignments {
		targetServer := RendezvousHash(shardId, c.Servers, 1)[0]
		shardPoints := points[pointRange[0]:pointRange[1]]
		insertReq := rpcInsertPointsRequest{
			rpcRequestArgs: rpcRequestArgs{
				Source: c.MyHostname,
				Dest:   targetServer,
			},
			ShardDir: shardId,
			Points:   shardPoints,
		}
		if err := c.RPCInsertPoints(&insertReq, nil); err != nil {
			c.logger.Error().Err(err).Str("shardId", shardId).Msg("could not insert points")
		}
	}
	// ---------------------------
	return nil, nil
}

func (c *ClusterNode) SearchPoints(col models.Collection, query []float32, limit int) ([]models.Point, error) {
	// ---------------------------
	shards, err := c.GetShards(col, false)
	if err != nil {
		return nil, fmt.Errorf("could not get shards: %w", err)
	}
	// ---------------------------
	if len(shards) == 0 {
		// Nothing to search
		return nil, nil
	}
	// ---------------------------
	// We have to search every shard
	if len(shards) > 1 {
		return nil, fmt.Errorf("searching multiple shards is not supported yet")
	}
	// ---------------------------
	targetServer := RendezvousHash(shards[0].ShardDir, c.Servers, 1)[0]
	searchReq := rpcSearchPointsRequest{
		rpcRequestArgs: rpcRequestArgs{
			Source: c.MyHostname,
			Dest:   targetServer,
		},
		ShardDir: shards[0].ShardDir,
		Vector:   query,
		Limit:    limit,
	}
	searchResp := rpcSearchPointsResponse{}
	if err := c.RPCSearchPoints(&searchReq, &searchResp); err != nil {
		return nil, fmt.Errorf("could not search points: %w", err)
	}
	return searchResp.Points, nil
}
