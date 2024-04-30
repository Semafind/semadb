---
weight: 30
---

# Vector Search

Vector search is a powerful way to search for points in a collection based on their vector fields. This is especially useful when you have embeddings or other vector representations of your data. It is commonly used in retrieval augmented generation, recommender systems and similarity search.

SemaDB currently covers [two vector index types]({{< ref "/docs/concepts/indexing" >}}) each with similar but slightly different search parameters.

## Flat Index

The flat index is a simple index that stores the vectors in a flat array. This is the most basic form of vector search and is useful when you have a small number of vectors. The search parameters for the flat index are:

```json
{
    "query": {
        "property": "productEmbedding",
        "vectorFlat": {
            "vector": [1, 2],
            "operator": "near",
            "limit": 10 
        }
    },
    "limit": 10
}
```

*Why are there two limits?* The limit in the query object is the number of vectors to search for. The limit in the overall request is the number of points to return. For this simple query, it seems redundant but for more complex queries, it can be useful to have different limits. For example, we might be interested in the nearest 10 vectors and combine it with another 10 points from text search but return the top 5. More on that sort of action is discussed in [hybrid search]({{< ref "hybrid" >}}).

## Vector Vamana

The Vamana index is the recommend vector index for most use cases. It grows and searches efficiently. It requires only 1 extra parameter:

```json
{
    "query": {
        "property": "productEmbedding",
        "vectorVamana": {
            "vector": [1, 2],
            "operator": "near",
            "searchSize": 75,
            "limit": 10 
        }
    },
    "limit": 10
}
```

The `searchSize` here refers to the number of nodes in the graph to expand before deciding the search is over. That is, if we expanded 75 nodes and couldn't find anything closer then the current set, we stop the search. Lower values will be less accurate but faster. We recommend starting with 75 which is a good upper bound for most applications. This search request corresponds to the [greedy search algorithm from the DiskANN paper](https://proceedings.neurips.cc/paper_files/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf).