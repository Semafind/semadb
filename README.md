# semadb

No fuss vector database.

# Architecture

The journey of a user request follows the following critical path:

1. The server starts running from `main.go` which initialises the key components below.
2. The `httpapi/handlers.go` picks up the request and attempts to validate it. If valid it passes it onto the cluster node.
3. The `cluster/clusternode.go` represents a single server / container running semadb. It has a set of exported functions that mirrors the http requests under `cluster/actions.go`.
4. The cluster node then attempts to route the request to the appropiate cluster node if the final destination of the request is not already at the correct node. The `rpc` files are responsible for this. The routing is done using rendezvous hashing mainly using the shard directory path.
5. The `cluster/rpchandlers.go` process incoming requests such as searching for points for a specific shard a cluster node might be responsible for.
6. The `cluster/shardmgr.go` manages the shards a cluster node currently holds. Most embedded databases do not allow multi-process access, i.e. they hold a lock on the file. Hence, the shard manager loads, unloads and ensure the database files are managed as best as they could across the cluster.
7. The request is passed to the `shard` package which has mostly matching handlers such as inserting and searching for points. The main algorithm is implemented in `shard` and `search` files.
8. While the request is being processed by the shard, a `pointcache` stores currently used decoded point values for quicker access.
9. The `point` finally interacts with the database file, setting and getting key value pairs.
