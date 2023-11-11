# Contributing

Thank you for considering to contribute! This repo is hopefully structured in an easy-to-understand manner and you can get started quickly.

Please:

1. Create an issue to track the contribution.
2. Follow the [fork and pull request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects) approach.
3. Add documentation, ideally tests and `go test ./...`.
4. Be respectful of others :rocket:

> Thanks again!

## Architecture Design

SemaDB mainly resolves around these key components that correspond to the folders in the repository. In order from client request to lowest level handler:

- **HTTP API**: handles incoming client requests and is responsible for checking the payload, checking user information such as id and plan. It then forwards the request to the cluster node it is running on.
- **Cluster Node**: almost mirrors the http api to complete client requests. It manages remote procedure calls (RPC) to other servers and distributes data etc.
- **Shard**: is a self-contained point store and index. Eventually the cluster node juggles around shards to complete requests.

### Journey of a request

The journey of a user request follows the following critical path:

1. The server starts running from `main.go` which initialises the key components below.
2. The `httpapi/handlers.go` picks up the request and attempts to validate it. If valid it passes it onto the cluster node.
3. The `cluster/clusternode.go` represents a single server / container running semadb. It has a set of exported functions that mirrors the http requests under `cluster/actions.go`.
4. The cluster node then attempts to route the request to the appropriate cluster node if the final destination of the request is not already at the correct node. The `rpc` files are responsible for this. The routing is done using rendezvous hashing mainly using the shard directory path.
5. The `cluster/rpchandlers.go` process incoming requests such as searching for points for a specific shard a cluster node might be responsible for.
6. The `cluster/shardmgr.go` manages the shards a cluster node currently holds. Most embedded databases do not allow multi-process access, i.e. they hold a lock on the file. Hence, the shard manager loads, unloads and ensure the database files are managed as best as they could across the cluster.
7. The request is passed to the `shard` package which has mostly matching handlers such as inserting and searching for points. The main algorithm is implemented in `shard` and `search` files.
8. While the request is being processed by the shard, a `pointcache` stores currently used decoded point values for quicker access.
9. The `point` finally interacts with the database file, setting and getting key value pairs.

## Repo structure

The structure is mostly by the main components that make up SemaDB:

- `main.go` is the entry point that performs setup and gets the components going.
- **cluster** deals server to server communication and organisation of shards.
- **config** has the library to load configuration and sample configs for single server and local cluster.
- **distance** contains vector distance functions.
- **httpapi** exposes the level RESTful API. It mainly validates requests and passes them onto the cluster layer.
- **models** has common type definitions used across components such as `Point`.
- **shard** is a self-contained vector storage and indexing model. The core algorithm for indexing and searching similar vectors happens here.

## Manual testing

If you run the `run-single-server` task in VS Code, it will run a single instance of main with the correct configuration. Then you can make HTTP requests. Refer to Getting Started section on how to use the VS Code extension and `internal/samples.http` file.

There is also a `run-cluster` task which will spawn 3 instances of SemaDB configured as a cluster. This is useful for seeing remote calls in action.

### Loading random vectors

Sometimes to test bulk performance, we need to flood the system with data. For the moment, you can use the `internal/loadrand.go`:

```bash
go run ./internal/loadrand
```

which attempts to insert random vectors into the desired collection by making POST requests.

## Automated testing

There are some unit and integration tests across the repository. You can see and run them using

```bash
go test ./...
```

and also using VS Code testing integration.

The testing idea here is to cover critical behaviour as opposed to handle every use case. Currently, most of the test cases are for isolated components such as shard indexing and distance functions.

## Roadmap

- [ ] Version releases
- [ ] Multiple root directories to partition data across several disks
- [ ] Automatic horizontal scaling