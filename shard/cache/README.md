# Cache

Some indexes, definitely the vector ones, use a shared cache to keep decoded vectors and similarity graphs in memory. This is facilitated by this package. There are two main cache types:

- **Item Cache**: This is a simple in-memory **buffer** for decoded items from disk. Recall that the disk only stores `[]byte` and the cache decodes them into the actual item. This is a simple map with a mutex lock. We have this because during a large insertion operation for example, we don't want to repeatedly encode and decode items from disk. The cache is shared across all requests.
- **Shared Cache**: These are persistent caches that are shared across requests for the same shard. They are used to store entire vector indexes and similarity graphs. The manager is responsible for creating, updating, and deleting shared caches. It also prunes the shared caches based on a maximum desired size.

## Shared Cache

This is a glorious map that stores *things* in memory. It is more similar to a temporary workspace for a shard. For example, when we create vector indices, we do a lot of computational work to create the index. If the user is sending subsequent requests, it is beneficial to keep the index in memory for a while, especially if we have space. But, we can evict it and the request may require read from disk, the ultimate source of truth. So this shared cache business is a best-effort service to speed things up. Notably it covers the following:

- **All write requests are linear.** This is already a constraint on bbolt, so the cache restricts write access to a single go routine (thread) at a time.
- If an operation fails, the cache gets *scrapped* in case it is in an inconsistent state.
- Most importantly, there is a critical usage of `mutex.TryRLock` inside [manager.go](cache/manager.go). If a read request comes in and the current shared cache is busy, it continues with a new blank workspace cache to keep serving requests from the last consistent state as read from the disk by bbolt. The actual items that may be read during the request will be discarded since this is a temporary cache but this means the requests continue at the cost of extra memory as opposed to wait for a write to finish.
- With the aforementioned read-write lock, multiple readers can benefit from the cache if there are no writers at the same time. As soon as a write comes in reads revert to using a temporary cache.
- The cache is shared across requests for the same shard, not across shards or points. Each shard gets an opportunity to store some information. So it is possible that there are two large shards and their requests interleave. The first shard A uses memory, then shard B uses more and the manager evicts shard A cache, but shard A request comes in and so on. There is never enough RAM is the take home lesson.

*So what is in a cache?* Anything, often it is the entire index but this package doesn't assume anything beyond that it has a size and a way to create it. For example, the vector index is stored in a cache. When a request comes in, we ask: Is there a shared cached version of this index? The story goes like:

1. Is there a shared cache for this shard? If not, create a new one and continue as a writer or reader with a read-write lock on the shared cache.
2. There is a shared cache:
    1. I would like read from it only. Can I get a lock (`TryRLock`)? If not use a temporary cache.
    2. I would like write as well as read. I need a write lock on the shared cache.
    3. I have a lock (read or write), is this shared cache scrapped? If so, continue with a temporary one.
3. When done, the manager checks:
    1. If operation has failed, scrap the shared cache and remove from the manager. 
    2. Check the new size and prune shared caches based on maximum desired size.

> For write operations, the shared cache also a transaction lock. This is to ensure that a single operation acting on multiple items is consistent in case one fails. For example, when we interact with two vector indexes and an operation on one fails, we need to discard the other as well.