---
title: ⚡ Getting Started
weight: 10
---

# Getting Started

This page demonstrates how to get start with semantic search using SemaDB. We will create a collection, use [Sentence Transformers](https://www.sbert.net/) to create vector representations of the sentences we would like to search, insert them into the collection and use SemaDB's search functionality.

SemaDB doesn't use a custom client library, instead it has a JSON or [MessagePack](https://msgpack.org/) Rest API that can be accessed using any programming language. In this example we will use Python to interact with the API using the [requests library](https://requests.readthedocs.io/en/latest/).

> You can try [SemaDB Cloud Beta](https://rapidapi.com/semafind-semadb/api/semadb) for free without having to install anything and follow this guide. Otherwise you'll need a running instance of SemaDB. You can follow the instructions on the [main readme](/).

```bash
# Install the required libraries
pip install -U sentence-transformers requests
```

Then in our script we setup the necessary headers:

```python
import requests

base_url = "https://semadb.p.rapidapi.com"
# Or use an appropriate base_url if you are using a self-hosted instance, e.g.
# base_url = "http://localhost:8081/v2"

headers = {
	"content-type": "application/json",
	"X-RapidAPI-Key": "<SEMADB_API_KEY>",
	"X-RapidAPI-Host": "semadb.p.rapidapi.com"
    # Or if self-hosting
    # "X-User-Id": "<USER_ID>",
    # "X-User-Plan": "BASIC"
}
```

That is all the setup we need to start using SemaDB. Now we can start creating a collection, inserting data and searching.

## Create a Collection

A collection is a group of points (documents) that live in the same search space. You can read more about [collections in the concept page]({{< ref "concepts/collection" >}}).

```python
payload = {
	"id": "mycollection",
    "indexSchema": {
        "vector": {
            "type": "vectorVamana",
            "vectorVamana": {
                "vectorSize": 384, # Sentence transformers give embeddings of size 384
                "distanceMetric": "cosine",
                "searchSize": 75, # How exhaustive the search should be?
                "degreeBound": 64, # How dense the graph should be?
                "alpha": 1.2, # How much longer edges should be preferred?
            }
        }
    }
}

response = requests.post(base_url + "/collections", json=payload, headers=headers)

print(response.json())
```

Here we are asking to create a collection with the Vamana index, which is one of the index type supported by SemaDB. You can read more about [index types in the concept page]({{< ref "concepts/indexing" >}}).

## Insert Data

There are two steps to insert vector points into the collection. First we need the actual embeddings and SemaDB is model agnostic, so you can use any model you like. Here we are using Sentence Transformers to get the embeddings.

```python
import numpy as np
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('all-MiniLM-L6-v2')

sentences = [
    "This flowing floral maxi dress is perfect for a summer day.",
    "These ripped denim jeans are a must-have for any fashion-forward wardrobe.",
    "A classic black turtleneck sweater is a versatile piece.",
]

embeddings = model.encode(sentences)
# We normalise the embeddings because for cosine distance SemaDB expects
# normalised vectors. We can skip this step for example if we use euclidean
# distance.
embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

print(embeddings.shape)
```

which should print `(3, 384)` as we have 3 sentences and the embeddings are of size 384. We can now insert the data into the collection.

```python
# Insert the points into the collection for searching
points = []
for i in range(embeddings.shape[0]):
    # Here the "vector" field is the one that is indexed as per the indexSchema
    # definition. But "myfield" is completely optional and can be any field you
    # want to store.
    points.append({'vector': embeddings[i].tolist(), "myfield": i})
payload = { "points": points }

response = requests.post(base_url+"/collections/mycollection/points", json=payload, headers=headers)

print(response.json())
```

which should print something like:

```json
{"message": "success", "failedRanges": []}
```

In this basic example we only have 1 field (`vector`) that is indexed. But you can have any mixture of indexed and non-indexed fields. Refer to the [indexing page concept page]({{< ref "concepts/indexing" >}}) for more information.

### MessagePack Encoding

JSON is very flexible and easy-to-use; however, when inserting large amounts of data it can be slow. [MessagePack](https://msgpack.org/) is a binary format that is more compact and faster to encode and decode. It is supported by SemaDB and *recommended* for inserting points in bulk.

```python
import msgpack # pip install msgpack

payload = { "points": points } # Same as before
headers["content-type"] = "application/msgpack" # Change the content type to msgpack
response = requests.post(base_url+"/collections/mycollection/points", data=msgpack.dumps(payload), headers=headers)

# Responses are always in JSON
print(response.json())
```

This will give the same result as before, but will be considerably faster for large amounts of data.

> Make sure to change the content type back to JSON for other requests in this tutorial.

## Vector Search

To perform vector search, we need a query vector. This is often obtained using the same model that was used to create the embeddings. Here we use the same model to get the query vector.

```python
# Create a search string and embed it using the same method
search_sentence = "What can I wear on a hot day?"
search_vector = model.encode(search_sentence)
# Normalise the search vector for cosine distance, can be ignored if using
# euclidean distance
search_vector = search_vector / np.linalg.norm(search_vector)
print(search_vector.shape)
```

which should print `(384,)` as the search vector is of size 384. Now we can perform the search.

```python
# Ask SemaDB to perform vector search
import pprint
payload = {
    "query": {
        "property": "vector",
        "vectorVamana": {
            "vector": search_vector.tolist(),
            "operator": "near",
            "searchSize": 75,
            "limit": 3
        }
    },
    # Restrict what is returned
    "select": ["sentence"],
    "limit": 3
}
response = requests.post(base_url+"/collections/mycollection/points/search", json=payload, headers=headers)
pprint.pprint(response.json())
```

which should print something like:

```json
{"points": [{"_distance": 0.6202282,
             "_hybridScore": -0.6202282,
             "_id": "93687544-6aaa-437e-a3bc-2983b1104aea",
             "sentence": "This flowing floral maxi dress is perfect for a "
                         "summer day."},
            {"_distance": 0.64442706,
             "_hybridScore": -0.64442706,
             "_id": "3d81846f-fa4f-4ab9-abd6-7c45a944f87a",
             "sentence": "A classic black turtleneck sweater is a versatile "
                         "piece."},
            {"_distance": 0.6641861,
             "_hybridScore": -0.6641861,
             "_id": "667a3d27-f270-4c10-9352-77dfff328160",
             "sentence": "These ripped denim jeans are a must-have for any "
                         "fashion-forward wardrobe."}]}
```

The `_distance` field is the cosine distance between the query vector and the result vector. The `_hybridScore` is the score that is used to rank the results. Because there is only one search criteria in this example, the `_distance` and `_hybridScore` are the same. The score is negated of the distance to allow higher scores to be better.

There are different and complex search queries that can be performed using SemaDB. Make sure to check them out, but this single vector search is often the building block of many AI applications.

## Delete Collection

To clean up after we are done, we can delete the collection easily with:

```python
# Clean up by deleting the collection
response = requests.delete(base_url+"/collections/mycollection", headers=headers)

print(response.json())
```

This command will delete the collection and all the points in it.

# What's Next?

We've seen a very basic example of how to use SemaDB for single vector search. There are many more features and search queries that we can handle to build an application. Make sure to check out rest of the documentation to learn more.
