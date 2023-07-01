package cluster

import (
	"fmt"

	"github.com/semafind/semadb/kvstore"
	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack"
)

type CreateCollectionRequest struct {
	UserId     string
	Collection models.Collection
}

func (c *ClusterNode) CreateCollection(req CreateCollectionRequest) error {
	// ---------------------------
	// Construct key and value
	// e.g. U/ USERID / C/ COLLECTIONID
	fullKey := kvstore.USER_PREFIX + req.UserId + kvstore.DELIMITER + kvstore.COLLECTION_PREFIX + req.Collection.Id
	collectionValue, err := msgpack.Marshal(req.Collection)
	if err != nil {
		return fmt.Errorf("could not marshal collection: %w", err)
	}
	return c.ClusterWrite(fullKey, collectionValue)
}

func (c *ClusterNode) ListCollections(userId string) ([]models.Collection, error) {
	// ---------------------------
	// Construct key and value
	// e.g. U/ USERID / C
	fullKey := kvstore.USER_PREFIX + userId + kvstore.DELIMITER + kvstore.COLLECTION_PREFIX
	entries, err := c.ClusterScan(fullKey)
	if err != nil {
		return nil, fmt.Errorf("could not scan collections: %w", err)
	}
	// ---------------------------
	// Unmarshal values and deduplicate
	collections := make([]models.Collection, 0, len(entries))
	versionMap := make(map[string]int64)
	for _, entry := range entries {
		var collection models.Collection
		err := msgpack.Unmarshal(entry.Value, &collection)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal collection: %w", err)
		}
		if collection.Version > versionMap[entry.Key] {
			collections = append(collections, collection)
			versionMap[entry.Key] = collection.Version
		}
	}
	// ---------------------------
	return collections, nil
}
