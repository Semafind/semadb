# Shard

Each shard is a self-contained point store and index. It has all the core operations you expect such as inserting and searching points. The critical journey of a user request follows the path along the files:

- **shard.go** initialises and loads a shard. It also contains the public API of inserting, searching, updating, and deleting points. Part of the algorithm is implemented in these functions with the rest in:
- **search.go** covers two core functions of the similarity search algorithm. At the moment this is based on the [Vamana algorithm](https://proceedings.neurips.cc/paper_files/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf) adjusted for CRUD operations.
- **cache** helps re-use existing points in memory. It acts as a buffer between the disk and active operations.
    - **manager.go** allocates, ensures safe concurrent access to shared caches (in memory points).
    - **pointcache.go** is a linker between a shared cache and the disk. If things exist in memory, it'll serve from there, keeping track of flags such as whether a point is edited / added. When done, the user can flush to disk.
    - **point.go** interacts with the key value store (disk), converting and storing points as key value bytes on behalf of the point cache.

## Similarity Index Graph

CRUD is handled by the corresponding `InsertPoints`, `UpdatePoints`, `SearchPoints` and `DeletePoints` functions in **shard.go**. You can follow through the functions and comments to see how the operations are carried out. They are the exposed functions used by the system so no other wrappers or tricks are used.

When new points are inserted, updated or deleted the similarity search graph / index, i.e. the neighbours of the points in the graph have to be updated. This is dictated by the similarity search algorithm such as Vamana used here. It is a tricky process to get right and trickier to handle concurrency properly. You can use Go's race detector `go test -race ./shard` to run the test cases with race detection. Currently, any operation besides insertion is single threaded. However, it is possible, for example, that there are multiple search requests coming in and would be handled concurrently. That is, the search action uses a single thread but here could be multiple thread searching.

There could be a big performance difference in how the shard is used. If tested end-to-end as part of the database than there is a lot of encoding, decoding, RPC calls, loading, parsing etc going on. The total response time the user sees is usually higher because of that. To get the maximum performance out, one would need to keep everything in memory and query the shard directly such as using `internal/shardpy`.

## Cache

The shard cache is a shared in memory storage that speaks the language of points (add, delete etc). The idea here is to make the most of linear requests. For example, we search for something and load a part of the graph from disk. A subsequent search could benefit from the existing work the previous request has done. Albeit there are some things to look out for:

- **All write requests are linear.** This is already a constraint on bbolt, so the cache restricts write access to a single go routine (thread) at a time.
- If an operation fails, the cache gets *scrapped* in case it is in an inconsistent state. This is handled by the `cache/manager.go`
- Most importantly, there is a critical usage of `mutex.TryRLock` inside `manager.go`. If a read request comes in and the current shared cache is busy, it continues with a new blank workspace cache to keep serving requests from the last consistent state as read from the disk by bbolt. The points that are read during the search will be discarded but this means the search requests continue at the cost of extra memory as opposed to wait for a write to finish.
- With the aforementioned read-write lock, multiple readers can benefit from the cache if there are no writers at the same time. As soon as a write comes in reads revert to using a temporary cache (a map that is garbage collected).
- The cache is shared across requests for the same shard, not across shards or points. Each shard gets an opportunity to store some information. So it is possible that there are two large shards and their requests interleave. The first shard A uses memory, then shard B uses more and the manager evicts shard A cache, but shard A request comes in and so on. There is never enough RAM is the take home lesson.

The story goes like:

1. Is there a shared cache for this shard? If not, create a new one and continue as a writer or reader with a read-write lock on the shared cache.
2. There is a shared cache:
    1. I would like read from it only. Can I get a lock (`TryRLock`)? If not use a temporary cache.
    2. I would like write as well as read. I need a write lock on the shared cache.
    3. I have a lock (read or write), is this shared cache scrapped? If so, continue with a temporary one.
3. When done, the manager checks:
    1. If operation has failed, scrap the shared cache and remove from the manager. 
    2. Check the new size and prune shared caches based on maximum desired size.

## Storage

Since we don't dabble in relational data, a local embedded key-value store is sufficient and often more efficient. Each point has a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) is stored:

- `n<node_id>v` is the main vector for the node / point in the graph.
- `n<node_id>e` edges
- `n<node_id>m` stores the encoded point metadata. We use [MessagePack](https://msgpack.org/index.html) for efficiency and speed.
- `n<node_id>i` point UUID
- `p<point_uuid>i` node id

*What is a node vs a point?* A point is a unit of data that is stored in the shard as the user sees. They have unique UUIDs. A node wraps a point to be indexed in a graph structure. A node has a unique node id and edges to other nodes.

In addition to the points, some extra information is also stored:

- `startId` is the node id of the starting point for the search. The start node is part of the graph but not an actual data point. It just serves to kick start the search.
- `pointCount` is the current running point count. Mainly used when getting information about the shard so we don't have to scan the keys to figure how many points there are.
- `freeNodeIds` are node ids of old delete nodes and are up for grabs. It helps us reuse node ids to keep them within a reasonable bound and leverage things like bitsets during search.
- `nextFreeNodeId` the next node id that is free to assign if the above list is empty.
- `shardVersion` just an integer number indicating the current storage layout.

During regular operation, a point cache contains the actively used points for the algorithm. When it is flushed, it sets the keys in the key value store. The point cache is definitely a *point* of interest.

The current key-value store is [bbolt](https://github.com/etcd-io/bbolt) for its simplicity. Alternatives such as [badger](https://github.com/dgraph-io/badger) were also considered and initially used but eventually with a clearer API and a similar performance on our workload, bbolt was preferred instead.

Finally, a custom flat offset based file was considered, similar to the original Vamana paper. But this was deemed too tricky for CRUD operations. It works well for indexing once and searching many times but with CRUD, the file would have empty regions for deleted data and a map of empty slots. In that case, it slowly starts to resemble a B+ tree which is why a key-value store was chosen.

## Design choices

Choosing the right similarity search algorithm was the most important aspect. The main contender was [HSNW](https://arxiv.org/abs/1603.09320) that almost every other vector database uses. The reason we chose the Vamana algorithm was its simpler flat (non-hierarchical) graph representation and potential to benefit from disk based storage. Although HSNW seems to come on top in benchmarks, the recall performance of our current algorithm especially with sharding seems to be sufficient enough.

Another guiding principle is code readability and cleanliness. Instead of optimising every aspect of the Shard with Formula 1 level cost cutting, we tried to implement what is easier to understand.