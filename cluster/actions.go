package cluster

import (
	"errors"
	"fmt"
	"math/rand"
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

func (c *ClusterNode) GetShards(col models.Collection) ([]string, error) {
	// ---------------------------
	colDir := filepath.Join(config.Cfg.RootDir, col.UserId, col.Id)
	shardDirs, err := os.ReadDir(colDir)
	if err != nil {
		return nil, fmt.Errorf("could not read collection directory: %w", err)
	}
	// ---------------------------
	shards := make([]string, 0, len(shardDirs))
	for _, shardDir := range shardDirs {
		if !shardDir.IsDir() {
			continue
		}
		shards = append(shards, filepath.Join(colDir, shardDir.Name()))
	}
	// ---------------------------
	return shards, nil
}

func (c *ClusterNode) UpsertPoints(col models.Collection, points []models.Point) ([]error, error) {
	// ---------------------------
	// This is where shard distribution happens
	shards, err := c.GetShards(col)
	if err != nil {
		return nil, fmt.Errorf("could not get shards: %w", err)
	}
	// ---------------------------
	if len(shards) == 0 {
		newShard, err := c.CreateShard(col)
		if err != nil {
			return nil, fmt.Errorf("could not create shard: %w", err)
		}
		shards = append(shards, newShard)
	}
	// ---------------------------
	// Distribute points to shards
	shardPoints := make(map[string][]models.Point)
	for _, point := range points {
		shardId := shards[rand.Intn(len(shards))]
		shardPoints[shardId] = append(shardPoints[shardId], point)
	}
	// ---------------------------
	// Insert points
	results := make(map[string]error)
	// ---------------------------
	for shardId, shardPoints := range shardPoints {
		targetServer := RendezvousHash(shardId, c.Servers, 1)[0]
		upsertReq := RPCUpsertPointsRequest{
			RequestArgs: RequestArgs{
				Source: c.MyHostname,
				Dest:   targetServer,
			},
			ShardDir: shardId,
			Points:   shardPoints,
		}
		upsertResp := RPCUpsertPointsResponse{}
		if err := c.RPCUpsertPoints(&upsertReq, &upsertResp); err != nil {
			results[shardId] = err
		}
	}
	// ---------------------------
	for shardId, err := range results {
		if err != nil {
			c.logger.Error().Err(err).Str("shardId", shardId).Msg("could not upsert points")
		}
	}
	// ---------------------------
	return nil, nil
}

func (c *ClusterNode) SearchPoints(col models.Collection, query []float32, limit int) ([]models.Point, error) {
	// ---------------------------
	shards, err := c.GetShards(col)
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
	targetServer := RendezvousHash(shards[0], c.Servers, 1)[0]
	searchReq := RPCSearchPointsRequest{
		RequestArgs: RequestArgs{
			Source: c.MyHostname,
			Dest:   targetServer,
		},
		ShardDir: shards[0],
		Vector:   query,
		Limit:    limit,
	}
	searchResp := RPCSearchPointsResponse{}
	if err := c.RPCSearchPoints(&searchReq, &searchResp); err != nil {
		return nil, fmt.Errorf("could not search points: %w", err)
	}
	return searchResp.Points, nil
}
