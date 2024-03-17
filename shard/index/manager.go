package index

import (
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
)

type indexManager struct {
	bm          diskstore.BucketManager
	cx          *cache.Transaction
	cacheRoot   string
	indexSchema models.IndexSchema
	maxNodeId   uint64
}

func NewIndexManager(
	bm diskstore.BucketManager,
	cx *cache.Transaction,
	cacheRoot string,
	indexSchema models.IndexSchema,
	maxNodeId uint64,
) indexManager {
	return indexManager{
		bm:          bm,
		cx:          cx,
		cacheRoot:   cacheRoot,
		indexSchema: indexSchema,
		maxNodeId:   maxNodeId,
	}
}
