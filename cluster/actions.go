package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/kvstore"
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
	log.Debug().Str("fpath", fpath).Msg("CreateCollection")
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
			log.Error().Err(err).Str("metaFile", metaFile).Msg("could not read collection file")
			errCount++
			continue
		}
		var col models.Collection
		if err := msgpack.Unmarshal(colBytes, &col); err != nil {
			log.Error().Err(err).Str("metaFile", metaFile).Msg("could not unmarshal collection file")
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

func (c *ClusterNode) InsertPoints(col models.Collection, points []models.Point) []error {
	// ---------------------------
	// Construct key and value
	// e.g. U/ USERID / C/ COLLECTIONID / P/ POINTID
	pointPrefix := kvstore.USER_PREFIX + col.UserId + kvstore.DELIMITER + kvstore.COLLECTION_PREFIX + col.Id + kvstore.DELIMITER + kvstore.POINT_PREFIX
	// ---------------------------
	results := make([]error, len(points))
	// ---------------------------
	// Insert points
	for i, point := range points {
		fullKey := pointPrefix + point.Id
		targetServers, err := c.KeyPlacement(fullKey, &col)
		if err != nil {
			results[i] = fmt.Errorf("could not place point: %w", err)
			continue
		}
		// ---------------------------
		pointValue, err := msgpack.Marshal(point)
		if err != nil {
			results[i] = fmt.Errorf("could not marshal point: %w", err)
			continue
		}
		err = c.ClusterWrite(fullKey, pointValue, targetServers)
		if err != nil {
			results[i] = fmt.Errorf("could not write point: %w", err)
		}
	}
	// ---------------------------
	return results
}
