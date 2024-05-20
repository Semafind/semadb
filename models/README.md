# Models

This directory contains the models, i.e. the type definitions that are shared across the application. These are usually `type struct` definitions that are used to pass data between different packages. The models are usually simple and don't contain any logic.

> We expect models package to self-contained and not import any SemaDB packages to avoid circular dependencies.

The reason for having a separate package is to have a single source of truth for shared structures. This doesn't mean we put every definition here, in fact most of the time we try to place them in the package where they are used or exposed. However, some models such as `Collection` are used across multiple packages at different stages. For example, at HTTP API level, at RPC calls, at shard operations etc. In such cases, we try to refactor them into this package.