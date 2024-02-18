# Disk Storage

This package aims to provide a thin abstraction over the storage layer to unify access accross the application. We are not necessarily interested in creating a full adapter pattern approach in case we swap out our storage layer, it is more to stop us digging further into the specifics of an implementation and simplify common operations. Some of the goals are:

- Keep the exposed APIs simpler, for example details of transactions etc can be simplified. This helps make the upstream operations more robust and concise.
- Restrict access to operations such as splitting read-only operations from read-write stuff at compiler level. We use different types to enforce at compile time. For example, you can't write to a disk inside a read operation.
- Have a common way of handling disk files such as opening bbolt files using the same options.

The main disk storage is handled by [bbolt](https://github.com/etcd-io/bbolt) and the wrapping interfaces follow it's API closely. There is also an in-memory storage based on maps.

## Difference between cache

The in-memory implementation may look like a pre-cursor to a cache but the actual shard level cache stores decoded point data. These are key-value byte pairs whereas the point cache in the shard package decodes them. The caching mechanism is for the diskstore for file based bbolt is handled by the operating system. The OS caches pages read from and written to the file, so we get that for free and actually have no control over it.