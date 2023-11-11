# SemaDB

No fuss vector database.

SemaDB is a low cost, disk based, non-relational vector database. It is designed to offer a clear and easy-to-use API. It offers the following features:

- **Simple REST API**: fully JSON based, restful API for interacting with the database.
- **Efficient vector search**: that is based on the [Vamana algorithm](https://proceedings.neurips.cc/paper_files/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf).
- **Cluster mode**: where the data is distributed to multiple servers and search is offloaded to all participating machines.

For more information individual components that make SemaDB refer to directory READMEs.

## Getting Started

SemaDB reads all the configuration from a yaml file, there are some examples contained in the `config` folder. You can run a single server using:

```bash
SEMADB_CONFIG=./config/singleServer.yaml go run ./
```

If you are using VS Code as your editor, then there are already pre-made tasks that do the same thing but also launch a cluster locally too in debug mode.

After you have a server running, you can use the `internal/samples.http` file to see some example requests that can be made to the server. To make the most of it, install the [REST Client extension](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) which will allow you to make requests directly in the editor and show the results.

### Docker

You can locally build and run the docker image using:

```bash
docker build -t semadb ./
docker run -it --rm -v ./config:/config -e SEMADB_CONFIG=/config/singleServer.yaml -p 8081:8081 semadb
```

Please note that when using docker, the hostname and whitelisting of IPs may need to be adjusted depending on the network configuration of docker. Leaving hostname as a blank string and setting whitelisting to `'*'` opens up SemaDB to every connection as done in the `singleServer.yaml` configuration.

## Limitations

Automatic horizontal scaling: the number of servers in the SemaDB is currently fixed because it has no mechanism of moving the data around after server count change. When the number of servers change, the rendezvous hashing used will move 1/n amount of data to the new server. This is tricky to perform safely while the database is operating due to race conditions across servers. Some pitfalls are: a server lagging behind in configuration sending data to old servers, while data transfer is happening user requests must be handled, any mis-routed data must eventually arrive at the correct server, the system must recover from a split-brain scenario if the network is partitioned. Many distributed databases incorporate additional machinery that adds significant complexity to handles these such as versioned keys, vector clocks etc. One option in the future could be an offline scaling tool. At the moment, pre-deployment the workload must be anticipated and only vertical scaling (increasing compute resources per server) is possible.

No high availability besides searching. Collection and point operations require all involved (servers that have been distributed data) to participate. In the search path, failures can be tolerated because it is a Stochastic search and occasional drops in performance due to unavailable shards can be acceptable. We offload maintaining a healthy system across physical server failures to a container orchestration tool such as Kubernetes. We assume that the configured state of SemaDB will be actively maintained and as a result do not contain any peer discovery or consensus algorithms in the design. This design choice again simplifies the architecture of SemaDB and aids with rapid development. Original designs included consensus mechanisms such as [Raft](https://raft.github.io/) and a fully self-contained distributed system with peer discovery, but this was deemed overkill.
