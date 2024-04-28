# Shard

Each shard is a self-contained point store and index. It has all the core operations you expect such as inserting and searching points. The critical journey of a user request follows the path along the files:

- [shard.go](shard.go) initialises and loads a shard. It also contains the public API of inserting, searching, updating, and deleting points.
- [index](index/README.md) then handles any indexing operations that are required. This is a separate package that is used by the shard to manage indexes.

## Multi-index

CRUD is handled by the corresponding `InsertPoints`, `UpdatePoints`, `SearchPoints` and `DeletePoints` functions in [shard.go](shard.go). You can follow through the functions and comments to see how the operations are carried out. They are the exposed functions used by the system so no other wrappers or tricks are used.

When new points are inserted, updated or deleted the indexes are updated accordingly. It is a tricky process to get right and trickier to handle concurrency properly. You can use Go's race detector `go test -race ./shard` to run the test cases with race detection. Currently, within a single request different indices may operator concurrently, i.e. it is up to the index to distribute workload. However, it is possible, for example, that there are multiple requests coming in and would be handled concurrently by any upstream layer (e.g. multiple http requests trigger multiple shard CRUD calls).

There could be a big performance difference in how the shard is used. If tested end-to-end as part of the database than there is a lot of encoding, decoding, RPC calls, loading, parsing etc going on. The total response time the user sees is usually higher because of that. To get the maximum performance out, one would need to keep everything in memory and query the shard directly.

## Cache

The shard utilises an in-memory map-like cache to store items such as decoded vectors and similarity graphs. It is up to the indexes to decide whether it is worth using one. See the [cache](cache/README.md) package for more information.

## Storage

Since we don't dabble in relational data, a local embedded key-value store is sufficient and often more efficient. Each point has a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) is stored in the `points` bucket:

- `n<node_id>d` stores the encoded point data. We use [MessagePack](https://msgpack.org/index.html) for efficiency and speed.
- `n<node_id>i` point UUID
- `p<point_uuid>i` node id

*What is a node vs a point?* A point is a unit of data that is stored in the shard as the user sees. They have unique UUIDs. A node wraps a point to be indexed internall. It is historically called a node because originally SemaDB only performed graph based similarity indexes. A node has a unique node id.

*Where are the vectors, text index etc?* These are managed and stored by the indexes following the internal node id in their own buckets, e.g. `index/float/price`. The indexes are responsible for managing their own data and the shard only stores the node id and the point UUID. There are effort to ensure that what is stored by the indexes do not clash.

In addition to the points, some extra information is also stored in the `internal` bucket:

- `pointCount` is the current running point count. Mainly used when getting information about the shard so we don't have to scan the keys to figure how many points there are.
- `freeNodeIds` are node ids of old delete nodes and are up for grabs. It helps us reuse node ids to keep them within a reasonable bound and leverage things like bitsets during search.
- `nextFreeNodeId` the next node id that is free to assign if the above list is empty.

## Design choices

One of the main components of SemaDB is the approximate nearest neighbour vector search. So, choosing the right similarity search algorithm was critical. The main contender was [HSNW](https://arxiv.org/abs/1603.09320) that almost every other vector database uses. The reason we chose the Vamana algorithm was its simpler flat (non-hierarchical) graph representation and potential to benefit from disk based storage. Although HSNW seems to come on top in benchmarks, the recall performance of our current algorithm especially with sharding seems to be sufficient enough.

Another guiding principle is code readability and cleanliness. Instead of optimising every aspect of the Shard with Formula 1 level cost cutting, we tried to implement what is easier to understand.