# Cluster

The most important part is the `ClusterNode` struct which represents a self-contained entity within a cluster. It offers means of communicating with other nodes in the cluster, replicating data etc.

It purposefully does not incorporate the user facing HTTP API to decouple user facing actions from internal communication.