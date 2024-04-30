---
weight: 40
---

# Text Search

The full-text search is a powerful way to search for points in a collection based on a query string. This is especially useful when you have textual data like documents, articles, or product descriptions. It is commonly used in search engines, recommendation systems, and information retrieval.

> Before vector similarity search became popular, text search was and is the go-to method for searching for documents. It is still widely used today and is often combined with vector search for better results.

The text search query involves:

```json
{
    "query": {
        "property": "description",
        "text": {
            "value": "summer dress",
            "operator": "containsAll",
            "limit": 10
        }
    },
    "limit": 10
}
```

The `value` string is processed by the same index analyser to create tokens. In this case it could be something like `["summer", "dress"]`. The `operator` specifies how the tokens should be matched. The available operators are:

- **containsAll**: All the tokens must be present in the document. Useful for searching for documents that contain all the words in the query.
- **containsAny**: At least one of the tokens must be present in the document.

Similar to the [vector search]({{< ref "vector" >}}), the text search contains its own limit parameter. This is separate from the overall limit to allow complex searches without generating large intermediate result sets. For example, you might want to search for documents that contain all the words in the query and then sort them by a field and return the top 5.


## _score

Text search results come with an additional `_score` field. This field is the [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) ranking of the document. The higher the score, the better the match. So the results may look like:

```json
{
  "points": [
    {
      "_hybridScore": -0.10034334,
      "_id": "9ce6678a-8cb8-4d7d-a367-a9e0a1bbec2e",
      "_score": -0.10034334,
      "description": "This summer inspired dress is perfect for a hot day.",
    }
  ]
}
```