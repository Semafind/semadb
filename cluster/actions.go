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
