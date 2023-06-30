package cluster

import (
	"fmt"
	"sync"

	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/kvstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/rpcapi"
	"github.com/vmihailenco/msgpack"
)

type Cluster struct {
	Servers        []string
	mu             sync.RWMutex
	kvstore        *kvstore.KVStore
	rpcApi         *rpcapi.RPCAPI
	onWriteChannel chan kvstore.OnWriteEvent
}

func New(kvs *kvstore.KVStore, rpcApi *rpcapi.RPCAPI) (*Cluster, error) {
	onWriteChannel := make(chan kvstore.OnWriteEvent, 100)
	kvs.RegisterOnWriteObserver(onWriteChannel)
	cluster := &Cluster{Servers: config.Cfg.Servers, kvstore: kvs, rpcApi: rpcApi, onWriteChannel: onWriteChannel}
	go cluster.kvOnWrite()
	go cluster.startRepLogService()
	return cluster, nil
}

func (c *Cluster) Close() error {
	close(c.onWriteChannel)
	return nil
}

// ---------------------------

type CreateCollectionRequest struct {
	UserId     string
	Collection models.Collection
}

func (c *Cluster) CreateCollection(req CreateCollectionRequest) error {
	// ---------------------------
	// Construct key and value
	// e.g. U/ USERID / C/ COLLECTIONID
	fullKey := kvstore.USER_PREFIX + req.UserId + kvstore.DELIMITER + kvstore.COLLECTION_PREFIX + req.Collection.Id
	collectionValue, err := msgpack.Marshal(req.Collection)
	if err != nil {
		return fmt.Errorf("could not marshal collection: %w", err)
	}
	return c.Write(fullKey, collectionValue)
}
