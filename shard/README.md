# Shard

Each shard is a self-contained point store and index. It has all the core operations you expect such as inserting and searching points. The critical journey of a user request follows the path along the files:

- **shard.go** initialises and loads a shard. It also contains the public API of inserting, updating and deleting points. Part of the algorithm is implemented in these functions with the rest in:
- **search.go** covers two core functions of the similarity search algorithm. At the moment this is based on the [Vamana algorithm](https://proceedings.neurips.cc/paper_files/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf) adjusted for CRUD operations.
- **pointcache.go** acts as a buffer between decoded values and the actual key value store.
- **point.go** interacts with the key value store, converting and storing points as key value bytes.

## Critical paths

- **Similarity graph**: When new points are inserted, updated or deleted the similarity graph, i.e. the neighbours of the points in the graph have to be updated. This is dictated by the similarity search algorithm such as Vamana used here. It is a tricky process to make get right and trickier to handle concurrency properly. *At the moment, the algorithm is implemented in a single threaded fashion and incoming write requests are serialised to avoid disconnected graphs.*

## Storage

Since we don't dabble in relational data, a local embedded key-value store is sufficient and often more efficient. Each point has a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) is stored along 3 keys:

- **Point vector**: with key `{id}_v` stores the actual vector, aka embedding.
- **Point metadata**: with key `{id}_m` stores the encoded point metadata. We use [MessagePack](https://msgpack.org/index.html) for efficiency and speed.
- **Point edges**: with key `{id}_e` for the similarity graph edges. It is a list of edge ids to other points.

During regular operation, a point cache contains the actively used points for the algorithm. When it is flushed, it sets the keys in the key value store.

The current key-value store is [bbolt](https://github.com/etcd-io/bbolt) for its simplicity. Alternatives such as [badger](https://github.com/dgraph-io/badger) were also considered and initially used but eventually a clearer API and with similar performance on our workload from bbolt, it was preferred instead.

Finally, a custom flat offset based file was considered, similar to the original Vamana paper. But this was deemed too tricky for CRUD operations. It works well for indexing once and searching many times but with CRUD, the file would have empty regions for deleted data and a map of empty slots. In that case, it slowly starts to resemble a B+ tree which is why a key-value store was chosen.

## Design choices

Choosing the right similarity search algorithm was the most important aspect. The main contender was [HSNW](https://arxiv.org/abs/1603.09320) that almost every other vector database uses. The reason we chose the Vamana algorithm was its simpler flat (non-hierachical) graph representation and potential to benefit from disk based storage. Although HSNW seems to come on top in benchmarks, the recall performance of our current algorithm especially with sharding seems to be sufficient enough.
