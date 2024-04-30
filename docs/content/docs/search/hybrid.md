---
weight: 50
---

# Hybrid Search

Hybrid search is a powerful way to combine different search methods to get the best results. This is commonly used to combine [vector]({{< ref "vector" >}}) and [text]({{< ref "text" >}}) search results to get the best of both worlds.

Here are some very useful examples of hybrid search to overcome the limitations of vector search methods:

- **terminology**: Most domains have specific terms that are not present in the training data. By combining text search with vector search, you can find points that contain the specific terms.
- **abbreviations**: Similar to terminology, abbreviations are often not present in the training data. If a user searches for B2B, the neural network might not have seen this before. So combining text search with vector search can help a lot.
- **outliers**: Sometimes the vector search might not find the best results because the documents are all similar to each other. Text search can pick out differentiating keywords to provide more context to an AI agent.

## Composite Query

To create a hybrid query, we need to combine two or more *ranking search results*. Currently, in SemaDB these are vector or text search results. By ranking, we mean that the search results are ranked accordingly to their relevance to the query, i.e. distance for vector search and score for text search.

We create a hybrid query using the `_or`  or `_and` special query type. By doing so, we might get overlapping results from the two search methods. The results are then combined and ranked using a hybrid score:

```json
{
    "query": {
        "property": "_or",
        "_or": [
            {
                "property": "productEmbedding",
                "vectorVamana": {
                    "vector": [1, 2],
                    "operator": "near",
                    "searchSize": 75,
                    "limit": 10,
                    "weight": 0.2
                }
            },
            {
                "property": "description",
                "text": {
                    "value": "summer floral",
                    "operator": "containsAny",
                    "limit": 10,
                    "weight": 0.5
                }
            },
            {
                "property": "title",
                "text": {
                    "value": "maxi dress",
                    "operator": "containsAll",
                    "limit": 10,
                    "weight": 0.3
                }
            },
        ]
    },
    "select": ["title", "price"],
    "limit": 10
}
```

## Hybrid Score

In the above query, we combine the results of a vector search on `productEmbedding` with text searches on `description` and `title`. The `weight` parameter is used to adjust the importance of each search method and defines the initial hybrid score value as:

- `hybridScore = weight * score` for score based indices such as text search.
- `hybridScore = weight * distance * -1` for distance based indices such as vector search. The distance is negated to ensure lower distances yield higher scores.

The weight is optional and will be set to 1 if not provided. The above query might yield a result containing a point:

```json
{
  "points": [
    {
      "_distance": 8,
      "_hybridScore": -1.6802747,
      "_id": "9ce6678a-8cb8-4d7d-a367-a9e0a1bbec2e",
      "_score": -0.10034334,
      "title": "Maxi Dress",
      "price": 49.99
    }
    // ...
  ]
}
```

where the `hybridScore` is the sum of the weighted scores of the individual search methods if they yield **overlapping** documents. That is, multiple search methods return the same documents. If a document appears in a single search result, then its hybrid score is carried over. The `distance` and `score` fields are the raw values from the vector and text search results, respectively.

*What happens if there are multiple score or distance results?* In this case, the last search that yields the distance or score will be returned in the final result. In the above case, both `title` and `description` are text searches, so the score from `title` will be set as `_score` but this does not affect the `hybridScore` calculation.
