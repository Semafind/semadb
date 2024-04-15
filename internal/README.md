# Internal

This folder contains internal tools used for development and testing. It is not part of the public API and should not be used in production.

## Contents

- [dumpGraph](dumpGraph/dumpGraph.go) is a script to dump [vamana index](../shard/index/vamana/) graphs into a file. It may be useful for debugging or visualising the graph index.
- [generateJSONSchema](generateJSONSchema/generateJSONSchema.go) is work in progress script to read Go definitions and create JSON schemas for the [Open API specs](../httpapi/v2/openapi.yaml). The current specification is done manually to add more descriptions and documentation.
- [shardpy](shardpy/shardpy.go) is a wrapper to provide a memory-only shard for testing purposes. It is useful for running real-world datasets that originate from Python scripts.