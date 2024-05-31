<p align="center">
  <img src="https://raw.githubusercontent.com/Semafind/semadb/main/docs/static/logowithtext.svg" alt="SemaDB" style="height: 200px;"/>
</p>

<p align="center">
  <b>No fuss multi-index hybrid vector database / search engine</b>
</p>

[![Build Docker](https://github.com/Semafind/semadb/actions/workflows/build-docker.yaml/badge.svg)](https://github.com/Semafind/semadb/actions/workflows/build-docker.yaml)
[![Build Test](https://github.com/Semafind/semadb/actions/workflows/build-test.yaml/badge.svg)](https://github.com/Semafind/semadb/actions/workflows/build-test.yaml)
[![Build Docs](https://github.com/Semafind/semadb/actions/workflows/build-docs.yaml/badge.svg)](https://github.com/Semafind/semadb/actions/workflows/build-docs.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/semafind/semadb)](https://goreportcard.com/report/github.com/semafind/semadb)
![GitHub Issues or Pull Requests](https://img.shields.io/github/issues/Semafind/semadb)
![GitHub License](https://img.shields.io/github/license/Semafind/semadb)


SemaDB is a multi-index, multi-vector, document-based vector database / search engine. It is designed to offer a clear and easy-to-use JSON RESTful API. It original components of SemaDB were built for a knowledge-management project at [Semafind](https://www.semafind.com/) before it was developed into a standalone project. The goal is to provide a simple, modern, and efficient search engine that can be used in a variety of applications.

> Looking for a hosted solution? [SemaDB Cloud Beta](https://www.semafind.com/semadb) is available on [RapidAPI](https://rapidapi.com/semafind-semadb/api/semadb).

## Features ⚡

- **Vector search**: leverage the power of vector search to find similar items and build AI applications. SemaDB uses the graph-based [Vamana algorithm](https://proceedings.neurips.cc/paper_files/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf) to perform efficient approximate nearest neighbour search.
- **Keyword / text search**: search for documents based on keywords or phrases, categories, tags etc.
- **Geo indices**: search for documents based on their location either via latitude and longitude or [geo hashes](https://en.wikipedia.org/wiki/Geohash).
- **Multi-vector search**: search across multiple vectors at the same time for a single document each with own index.
- **Quantized vector search**: use quantizers to change internal vector representations to reduce memory usage.
- **Hybrid search**: combine vector and keyword search to find the most relevant documents in a single search request. Use weights to adjust the importance of each search type.
- **Filter search**: filter search results based on other queries or metadata.
- **Hybrid, filter, multi-vector, multi-index search**: combine all the above search types in a single query. For example, "find me the nearest restaurants (geo index) that are open now (inverted index), have a rating of 4 or more (integer index), and serve a dish similar to this image (vector search) and have a similar description to this text (vector search)".
- **Simple REST API**: fully JSON based, restful API for interacting with the database. No need to learn a new query language, install custom clients or libraries.
- **Real-time**: changes are visible immediately and search results are returned in milliseconds, even for large datasets.
- **Single binary**: the entire database is contained in a single binary. No need to install additional dependencies or services.
- **Multiple deployment modes**: standalone, container, or cloud. SemaDB can be deployed in a variety of ways to suit your needs.
- **Prometheus metrics**: monitor the health of SemaDB with metrics such as number of searched points, latency of search, number of requests etc.
- **Cluster mode**: where the data is distributed to multiple servers and search is offloaded to all participating machines.
- **Automatic-sharding**: data is automatically sharded across multiple servers based on a hashing algorithm.
- **Multi-tenancy**: multiple users with different plans can use the same SemaDB instance. Each user can have their own collections and indices.
- **Legible source code**: the goal is to allow anyone pick a random file and hopefully understand what is going on. Some files have more comments than code.


## Getting Started

To get started from source, please follow the instructions to install [Go](https://go.dev/doc/install). That is the only dependency required to run SemaDB. We try to keep SemaDB as self-contained as possible and up-to-date with the latest Go releases.

SemaDB reads all the configuration from a yaml file, there are some examples contained in the `config` folder. You can run a single server using:

```bash
SEMADB_CONFIG=./config/singleServer.yaml go run ./
```

If you are using VS Code as your editor, then there are already pre-made [tasks](https://code.visualstudio.com/docs/editor/tasks) that do the same thing but also launch a cluster locally too in debug mode.

After you have a server running, you can use the [samples](httpapi/v2/samples.http) file to see some example requests that can be made to the server. To make the most of it, install the [REST Client extension](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) which will allow you to make requests directly in the editor and show the results.

### Docker & Podman

You can run the latest version of SemaDB using the following repository container image:

```bash
docker run -it --rm -v ./config:/config -e SEMADB_CONFIG=/config/singleServer.yaml -p 8081:8081 ghcr.io/semafind/semadb:main
# If using podman
podman run -it --rm -v ./config:/config:Z -e SEMADB_CONFIG=/config/singleServer.yaml -p 8081:8081 ghcr.io/semafind/semadb:main
```

which will run the main branch. There are also tagged versions for specific releases. See the [container registry](https://github.com/Semafind/semadb/pkgs/container/semadb) of the repository stable and production ready versions.

You can locally build and run the container image using:

```bash
docker build -t semadb ./
docker run -it --rm -v ./config:/config -e SEMADB_CONFIG=/config/singleServer.yaml -p 8081:8081 semadb
# If using podman
podman build -t semadb ./
# The :Z argument relabels to access: see https://github.com/containers/podman/issues/3683
podman run -it --rm -v ./config:/config:Z -e SEMADB_CONFIG=/config/singleServer.yaml -p 8081:8081 semadb
```

Please note that when using docker, the hostname and whitelisting of IPs may need to be adjusted depending on the network configuration of docker. Leaving hostname as a blank string and setting whitelisting to `'*'` opens up SemaDB to every connection as done in the `singleServer.yaml` configuration.

## Contributing

Contributions are welcome! Please read the [contributing guide](CONTRIBUTING.md) file for more information. The contributing guide also contains information about the architecture of SemaDB and how to get started with development.


## Search Algorithm 🔍

SemaDB's core vector search algorithm is based on the following excellent research papers:

- Jayaram Subramanya, Suhas, et al. "Diskann: Fast accurate billion-point nearest neighbor search on a single node." Advances in Neural Information Processing Systems 32 (2019) [link](https://proceedings.neurips.cc/paper_files/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf).
- Singh, Aditi, et al. "FreshDiskANN: A Fast and Accurate Graph-Based ANN Index for Streaming Similarity Search." arXiv preprint arXiv:2105.09613 (2021) [link](https://arxiv.org/abs/2105.09613).
- Gollapudi, Siddharth, et al. "Filtered-DiskANN: Graph Algorithms for Approximate Nearest Neighbor Search with Filters." Proceedings of the ACM Web Conference 2023. 2023 [link](https://harsha-simhadri.org/pubs/Filtered-DiskANN23.pdf).

Other indices such as string, or text follows an [inverted index](https://en.wikipedia.org/wiki/Inverted_index) approach. The inverted index is a data structure that stores a mapping from content, such as words or numbers, to its locations in a database file, or in a document or a set of documents. The purpose of an inverted index is to allow fast full-text searches, string prefix lookups, integer range search etc.

### Performance

SemaDB with default configuration values on a `Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz` commodity workstation with 16GB RAM achieves good recall across standard benchmarks, similar to the reported results:

|                             | v1     |        | v2     |        | v2-PQ  |       | v2-BQ  |        |
|-----------------------------|--------|--------|--------|--------|--------|-------|--------|--------|
|                     Dataset | Recall | QPS    | Recall | QPS    | Recall | QPS   | Recall | QPS    |
| glove-100-angular           |  0.924 | 973.6  | 0.853  | 773.9  | 0.526  | 628.6 |        |        |
| dbpedia-openai-100k-angular |        |        | 0.990  | 519.9  | 0.920  | 240.8 | 0.766  | 978.6  |
| glove-25-angular            |  0.999 | 1130.3 | 0.992  | 914.4  | 0.989  | 805.8 |        |        |
| mnist-784-euclidean         |  0.999 | 1898.6 | 0.999  | 1267.4 | 0.928  | 571.6 | 0.667  | 2369.7 |
| nytimes-256-angular         |  0.903 | 1020.6 | 0.891  | 786.7  | 0.438  | 983.6 |        |        |
| sift-128-euclidean          |  0.999 | 1537.7 | 0.991  | 1272.9 | 0.696  | 967.4 |        |        |

The results are obtained using [ann-benchmarks](https://github.com/erikbern/ann-benchmarks). The queries per second (QPS) uses full in memory cache with a single thread similar to other methods but is not a good indication of overall performance. The full pipeline would be slower because of the end-to-end journey of a request has the overhead of HTTP handling, encoding, decoding of query, parsing, validation, cluster routing, remote procedure calls, loading data from disk etc. This further depends on the hardware, especially SSD vs hard disk. However, the raw performance of the search algorithm within a single Shard would, in theory, be similar to that reported in the research papers.

Version 1 (v1) is the original pure vector search implementation of SemaDB. Version 2 (v2) is the multi-index, hybrid, keyword search etc implementation which has a much higher overhead of decoding, dispatching data into indices and using quantizers. Version 2 with Product Quantization (v2-PQ) and Binary Quantization (v2-BQ) use the respective quantization methods to reduce memory usage. We expect the recall to be lower because the quantization methods are lossy and the search is approximate.

## Limitations 🪧

**Cold disk starts** can be really slow. At the bottom of the chain sits the disk where all the data is stored. There are two caches in play: the in-memory cache and the operating system file cache. The OS cache is not in our control and gets populated as files are read or written to. When a request is made, the index graph gets traversed and points are loaded from the disk to operating system cache and decoded into in-memory set of points. The search operation often performs random reads from disk as it traverses the similarity graph; hence, during a cold start it can take a long time (1 second, 10 seconds or more) depending on the hardware. Solid-state disks (SSDs) are strongly recommended for this reason as they serve random reads better. For single application deployments, this is not a major concern because we expect a portion of the data / index to be cached either in-memory or by the operating system during operation. An alternative is to use a custom graph oriented storage layout on disk so blocks / pages are better aligned with neighbours of nodes in the similarity graph.

**Automatic horizontal scaling**: the number of servers in the SemaDB is currently fixed because it has no mechanism of moving the data around after server count change. When the number of servers change, the rendezvous hashing used will move 1/n amount of data to the new server. This is tricky to perform safely while the database is operating due to race conditions across servers. Some pitfalls are: a server lagging behind in configuration sending data to old servers, while data transfer is happening user requests must be handled, any mis-routed data must eventually arrive at the correct server, the system must recover from a split-brain scenario if the network is partitioned. Many distributed databases incorporate additional machinery that adds significant complexity to handles these such as versioned keys, vector clocks etc. One option in the future could be an offline scaling tool. At the moment, pre-deployment the workload must be anticipated and only vertical scaling (increasing compute resources per server) is possible.

**No write high availability**: SemaDB is optimised for search heavy workloads. Collection and point write operations require all involved (servers that have been distributed data) to participate. In the search path, failures can be tolerated because it is a Stochastic search and occasional drops in performance due to unavailable shards can be acceptable. We offload maintaining a healthy system across physical server failures to a container orchestration tool such as Kubernetes. We assume that the configured state of SemaDB will be actively maintained and as a result do not contain any peer discovery or consensus algorithms in the design. This design choice again simplifies the architecture of SemaDB and aids with rapid development. Original designs included consensus mechanisms such as [Raft](https://raft.github.io/) and a fully self-contained distributed system with peer discovery, but this was deemed overkill.

## Related Projects

There are many open-source vector search and search engine projects out there. It may be helpful to compare SemaDB with some of them to see if one fits your use case better:

- [faiss](https://github.com/facebookresearch/faiss) - A library for efficient similarity search and clustering of dense vectors.
- [milvus](https://github.com/milvus-io/milvus) - An open-source similarity search engine for embedding vectors.
- [weaviate](https://github.com/weaviate/weaviate) - Weaviate is a cloud-native, open source vector database.
- [qdrant](https://github.com/qdrant/qdrant) - A vector search engine written in Rust.
- [chroma](https://github.com/chroma-core/chroma) - The AI-native open-source embedding database