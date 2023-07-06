package cluster

import (
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
	// Construct key and value
	prefix := newUserCollectionKeyPrefix(userId)
	entries, err := c.kvstore.ScanPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("could not scan collections: %w", err)
	}
	// ---------------------------
	// Unmarshal values and deduplicate
	collections := make([]models.Collection, 0, len(entries))
	for _, entry := range entries {
		var collection models.Collection
		err := msgpack.Unmarshal(entry.Value, &collection)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal collection: %w", err)
		}
	}
	// ---------------------------
	return collections, nil
}

func (c *ClusterNode) GetCollection(userId string, collectionId string) (models.Collection, error) {
	// ---------------------------
	// Construct key and value
	cKey := newCollectionKey(userId, collectionId)
	value, err := c.kvstore.Read(cKey)
	if err != nil {
		return models.Collection{}, fmt.Errorf("could not read collection: %w", err)
	}
	// ---------------------------
	// Unmarshal
	var collection models.Collection
	if err := msgpack.Unmarshal(value, &collection); err != nil {
		return models.Collection{}, fmt.Errorf("could not unmarshal collection: %w", err)
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
