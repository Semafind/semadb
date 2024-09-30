---
title: ❓ FAQ
weight: 70
---

# Frequently Asked Questions

## What is SemaDB?

SemaDB is a search engine that allows you to search for items in a large dataset. It originated from vectors similarity search and grew to more complex query types. It is designed to be fast, accurate and easy to use.

## What is vector similarity search?

Vector similarity search is a technique to find similar items in a dataset based on their vector representation. It is used in a wide range of applications such as recommendation systems, image search, text search and more. SemaDB is designed to make it easy to search for similar vectors in a large dataset using a variety of distance metrics.

## Can I use SemaDB without vectors?

Yes, absolutely. SemaDB supports different types of data such as text, strings, integers and floats. You can use SemaDB to search for items based on these data types. For example, you can search for products based on their description, category, price and more. Please refer to [indexing guide]({{< ref "concepts/indexing" >}}).

## How does SemaDB work?

SemaDB is open-source and one of the goals is to ensure the codebase is easy to understand. The [contributing guidelines](https://github.com/Semafind/semadb/blob/main/CONTRIBUTING.md) dives into more detail about the architecture and how to contribute.

## Why doesn't SemaDB normalise vectors for cosine distance?

Some models such as [OpenAI Embedding](https://platform.openai.com/docs/guides/embeddings/which-distance-function-should-i-use) already output normalised vectors. Adding normalisation to the search process would be redundant and slow down the search process. If you have non-normalised vectors, you can normalise them before inserting them into SemaDB.

## I have binary vectors, can I use SemaDB?

Yes, SemaDB supports binary vectors. You can use binary vectors with the `hamming` or `jaccard` distance metric. Using these distance metrics automatically enables [binary quantisation]({{< ref "quantization" >}}). When inserting or searching, you need to ensure that the vectors are still in floating point format, e.g. `[0.0, 1.0, 0.0, 1.0]`.