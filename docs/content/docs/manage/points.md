# Points

Managing points in SemaDB follows an easy to understand RESTful API. You can insert, search, and delete points in a collection using the API. This section will guide you through the process of managing points in SemaDB.

> [SemaDB Cloud Beta](https://rapidapi.com/semafind-semadb/api/semadb) offers live interactive examples you can use without having to setup a local instance.

You need to have an existing [collection]({{< ref "/docs/concepts/collection" >}}) to insert points. If you don't have a collection, you can create one using the [collections API]({{< ref "collections" >}}). The [getting started guide]({{< ref "/docs/getting-started" >}}) has a full example of making requests to the API too.

For more details on the endpoints and parameters, please refer to the [API reference](/api-reference.html).

## Insert

POST: `/collections/{id}/points`

The inserted  points can have any fields so long as the total size of the encoded point does not exceed the limit set on SemaDB. This limit is just avoid blowing up the server if someone accidentally sends a large matrix of numbers instead of vectors etc. The post request should have the following format:

```json
{
  "points": [
    {
      "myvector": [
        4.2,
        2.4
      ]
    },
    {
      "myvector": [
        1.2,
        3.4
      ],
      "foo": "bar",
      "externalId": 42
    },
    {
      "_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "myvector": [
        3.4,
        2.4
      ],
      "externalId": 43
    }
  ]
}
```

When it comes to the point `_id`, it is optional. If you don't provide it, SemaDB will generate a unique ID for the point. If you provide it, SemaDB will use the provided ID. **The provided `_id` must be unique**, SemaDB doesn't check for duplicates across collection shards but may detect duplicates if there is a single shard.

If the collection has multiple shards, some may fail to insert the distributed points. In this case, SemaDB still commits the points to the shards that succeeded to avoid repeated work on subsequent requests. The response will contain the points that have failed:

```json
{
  "message": "partial success",
  "failedRanges": [
    {
      "shardId": "fff3a226-b9f8-4375-8dbd-1a240e000705",
      "start": 0,
      "end": 2,
      "error": "point already exists"
    }
  ]
}
```

saying that the first two points failed to insert because at least one of them exists. In this case you have to retry the insert by addressing the failed points error message.

## Update

PUT: `/collections/{id}/points`

You can update points in a collection by making a **PUT request** to the `/collections/{id}/points` endpoint. The only difference compared to insert is **the `_id` field must be set**:

```json
{
  "points": [
    {
      "_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "myvector": [
        4.2,
        2.4
      ],
      "wizard": "harry"
    }
  ]
}
```

The response is similar to the insert operation and you are encouraged to check the result to see if parts of the update failed.

## Delete

DELETE: `/collections/{id}/points`

To delete points, just send the `_id` of the points you want to delete:

```json
{
  "ids": [
    "3fa85f64-5717-4562-b3fc-2c963f66afa6"
  ]
}
```

The response will indicate if the delete was successful or not:

```json
{
  "message": "partial success",
  "failedPoints": [
    {
      "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "error": "not found"
    }
  ]
}
```