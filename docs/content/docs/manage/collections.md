# Collections

All data in SemaDB are organised into a [collection]({{< ref "/docs/concepts/collection" >}}) that are similar to collections in other noSQL databases. A collection is a group of documents that are stored together. Each document in a collection is a JSON object that can have any number of fields. The fields can be of different types such as strings, numbers, arrays, or even nested objects.

For more details on the endpoints and parameters, please refer to the [API reference](/api-reference.html).

> [SemaDB Cloud Beta](https://rapidapi.com/semafind-semadb/api/semadb) offers live interactive examples you can use without having to setup a local instance.

## Create

POST: `/collections`

To create a collection, you need to provide a name for the collection and optionally an [index schema]({{< ref "/docs/concepts/indexing" >}}). The index schema defines the fields that are to be indexed for *searching* only. We can create a collection by **making a POST request** to the `/collections` endpoint, see the [getting started]({{< ref "/docs/getting-started" >}}) for a full request example with appropriate headers.

```json
{
    "id": "products",
    "indexSchema": {
        "descriptionEmbedding": {
            "type": "vectorVamana",
            "vectorVamana": {
                "vectorSize": 384,
                "distanceMetric": "euclidean",
                "searchSize": 75,
                "degreeBound": 64,
                "alpha": 1.2
            }
        }
    }
}
```

In the above example, we are creating a collection named `products` with an index schema that indexes the `descriptionEmbedding` field. The `descriptionEmbedding` field is a vector of size 384 that uses the `euclidean` distance metric for similarity calculations.

## List

GET: `/collections`

You can view all the collections in the database by making a **GET request** to the `/collections` endpoint. The response will contain a list of all the collections in the database.

```json
{
  "collections": [
    {
      "id": "mycollection"
    },
    {
      "id": "anothercollection"
    }
  ]
}
```

## Get

GET: `/collections/{id}`

You can view some details about the collection by making a **GET request** to the `/collections/{id}` endpoint. The response will contain the collection name, the number of documents in the collection, and the index schema if it exists.

```json
{
  "id": "mycollection",
  "indexSchema": {
    "myvector": {
      "type": "vectorVamana",
      "vectorVamana": {
        "vectorSize": 2,
        "distanceMetric": "euclidean",
        "searchSize": 75,
        "degreeBound": 64,
        "alpha": 1.2
      }
    }
  },
  "shards": [
    {
      "id": "fff3a226-b9f8-4375-8dbd-1a240e000705",
      "pointCount": 42
    }
  ]
}
```

## Delete

DELETE: `/collections/{id}`

To delete a collection, you can make a **DELETE request** to the `/collections/{id}` endpoint. This will delete the collection and all the documents in it. Be careful as this operation is irreversible. SemaDB attempst to delete *all* the data in the collection. We say attempts because if a shard server is unreachable, it may have to be deleted in the future.