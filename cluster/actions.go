package cluster

import (
	"fmt"

	"github.com/semafind/semadb/kvstore"
	"github.com/semafind/semadb/models"
	"github.com/vmihailenco/msgpack"
)

func (c *ClusterNode) CreateCollection(userId string, collection models.Collection) error {
	// ---------------------------
	// Construct key and value
	// e.g. U/ USERID / C/ COLLECTIONID
	fullKey := kvstore.USER_PREFIX + userId + kvstore.DELIMITER + kvstore.COLLECTION_PREFIX + collection.Id
	collectionValue, err := msgpack.Marshal(collection)
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

func (c *ClusterNode) GetCollection(userId string, collectionId string) (models.Collection, error) {
	// ---------------------------
	// Construct key and value
	// e.g. U/ USERID / C/ COLLECTIONID
	fullKey := kvstore.USER_PREFIX + userId + kvstore.DELIMITER + kvstore.COLLECTION_PREFIX + collectionId
	values, err := c.ClusterGet(fullKey, 0)
	if err != nil {
		return models.Collection{}, fmt.Errorf("could not get collection: %w", err)
	}
	// ---------------------------
	// Unmarshal and deduplicate values
	latestVersion := int64(0)
	var collection models.Collection
	for _, value := range values {
		var tempCollection models.Collection
		err := msgpack.Unmarshal(value, &tempCollection)
		if err != nil {
			return models.Collection{}, fmt.Errorf("could not unmarshal collection: %w", err)
		}
		if tempCollection.Version > latestVersion {
			collection = tempCollection
			latestVersion = tempCollection.Version
		}
	}
	// ---------------------------
	return collection, nil
}
