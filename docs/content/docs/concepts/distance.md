---
title: Distance
weight: 4
---

# Distance Metrics

Every vector index in SemaDB requires a distance metric to be specified. The distance metric is used to calculate the **similarity** between two vectors and is a function that takes two vectors as input and returns a scalar value. The smaller the value, the closer the two vectors are.

The following distance metrics are supported in SemaDB:

## Floating Point

- `euclidean`: The **squared** Euclidean distance between two vectors. This is the default recommended distance metric for most use cases. It is squared to avoid the expensive square root operation and does not affect the ranking of the nearest neighbors since the square root is a monotonic function.
- `cosine`: The cosine distance between two vectors defined as 1 - cosine similarity. This is a popular distance metric for text and image similarity search. With cosine distance, **vectors must be normalised** before indexing. SemaDB doesn't normalise vectors by default since most models already output normalised vectors.
- `dot`: The negated dot product between two vectors, i.e. -dot(v1, v2). It is negated so smaller values are closer. Bear in mind, this is not a proper distance metric since it doesn't satisfy the triangle inequality and has negative values.
- `haversine`: The [Haversine distance](https://en.wikipedia.org/wiki/Haversine_formula) between two vectors. This is the distance between two points on Earth. It is used for geospatial data and the **vectors must be in the form of [latitude, longitude]** pairs in degrees. The distance is returned in meters.

> For normalised vectors, the squared euclidean distance is proportional to the cosine distance, i.e. euclidean^2 = 2(1-cosine(x,y)). So, using squared euclidean distance is a good default choice.

The distance metric to use besides `haversine` depends on the use case. If you are unsure, `euclidean` is a good default choice because it is a proper metric and doesn't require normalisation.

## Binary

- `hamming`: The [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) between two binary vectors. It is the number of bits that differ between two binary vectors. This is useful for binary embeddings. Using this distance metric automatically enables [binary quantisation]({{< ref "quantization" >}}).
- `jaccard`: The [Jaccard distance](https://en.wikipedia.org/wiki/Jaccard_index) between two binary vectors. It is the number of bits that differ between two binary vectors divided by the number of bits that are set in either vector. This is again useful for binary embeddings that have set like semantics. Using this distance metric automatically enables [binary quantisation]({{< ref "quantization" >}}).

## Custom Metric?

SemaDB is open source and you can easily add your own distance metric. If you have a custom distance metric that you would like to use, please open an issue on the [GitHub repository](https://github.com/Semafind/semadb)!