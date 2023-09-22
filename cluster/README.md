# Cluster Node

A single cluster node represents a server in the SemaDB deployment. The `ClusterNode` struct is a self-contained entity and encapsulates the RPC communication to other servers. The client to data journey is covered by the following critical files:

- **clusternode.go**: sets up a cluster node instance handle all init and closing functions as well as some background functions like the RPC server and backup functionality.
- **actions.go**: is the public facing API of the cluster node. It mostly mirrors what you would expect a user to be able to do such as creating a collection and inserting points. It also contains some extra functions such as getting shard info.
- **rpchandlers.go**: is the internal facing API. Each cluster node may communicate with other cluster nodes over the RPC network to complete a task depending on the *rendezvous hashing* used. Internal routing between nodes is handled in each function transparently to make it clear what is going on.
- **shardmgr.go**: opens and provides instances of shards to complete the jobs. It ensures that a shard cannot be unloaded whilst being used etc. It also currently managed automatic backups of shards.

## Critical paths

- **Actions to RPC**: almost all actions make rpc calls. Who calls what, where and how is really important to the overall system. At the moment [rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) is used in actions according to request parameters such as user id or shard id.
- **Shard loading and unloading**: the shard manager juggles open shards and attempts to unload unused ones. During operation, incoming requests and the clean up goroutine will attempt to access the same shard. Care must be taken to avoid concurrency bugs and potential corrupting of the database file for example by closing the shard whilst inserting points.

## Design choices

The original design of the cluster node was to also include the HTTP API. Since we already run an RPC server, the http server would sit next to the all the necessary functionality already offered by the cluster node. But this overloaded the cluster node with very similar but subtly different sets of functionality. For example, incoming HTTP requests need user auth etc whereas we assume internal RPC calls are safe. Despite almost mirroring the http calls in the public facing actions, decoupling http requests from cluster node helped better structure the code and test it.

Another concern was the similarity of actions and the rpc handlers. Each action computes the necessary rpc calls. The reason they are separate is because some actions such as searching, makes multiple rpc requests. Having a clear boundary between what is computed on the node vs across rpc helped with debugging.
