# Internal

This folder contains internal tools used for development and testing. It is not part of the public API and should not be used in production.

## Contents

- [dumpGraph](dumpGraph/dumpGraph.go) is a script to dump [vamana index](../shard/index/vamana/) graphs into a file. It may be useful for debugging or visualising the graph index.
- [generateJSONSchema](generateJSONSchema/generateJSONSchema.go) is work in progress script to read Go definitions and create JSON schemas for the [Open API specs](../httpapi/v2/openapi.yaml). The current specification is done manually to add more descriptions and documentation.
- [loadhdf5](loadhdf5/loadhdf5.go) loads pre-built datasets directly into a shard avoiding any http handlers. It assumes the HDF5 data is in a certain format and is not a generic loader.
- [loadrand](loadrand/loadrand.go) generates and loads random vectors into a collection via the HTTP API. It is useful for stress testing the ingestion and indexing pipeline.
- [migratev1-v2](migratev1-v2/migratev1-v2.go) migrates the database files from version 1 to 2. Since only a single process can open a database file, the server must not be running.
- [shardpy](shardpy/shardpy.go) is a wrapper to provide a memory-only shard for testing purposes. It is useful for running real-world datasets that originate from Python scripts. It may act as the basis of a future Python binding library.
