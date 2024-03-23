# Full Text Index

This index covers the full text search of documents. It works on `string` properties of documents. The main components are:

1. Text analysis, this is at the moment kindly covered by [bleve](https://github.com/blevesearch/bleve) so we don't have to reinvent the wheel. It takes a string and returns tokens.
2. Term sets store the mapping `term -> docIds` as a [Roaring Bitmap](https://github.com/RoaringBitmap/roaring).
3. Document items store the mapping `docId -> term frequencies`.

The index is locked during any operation, search or write, because the roaring sets are not thread safe. This lock is used to protect the caches, set and document. The set and document caches speed up the process, especially during a write, by avoiding serialisation operations repeated. For example, if there are shared terms across the write documents, the term set doesn't need to be refected, deserialised, updated and serialised again.

The flush operation writes all the information and is performed after a write operation.

## Scoring

We currently use [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) scoring to rank the results of the documents. It is calculated as described online:

```go
// ---------------------------
// TF-IDF scoring
// https://en.wikipedia.org/wiki/Tf%E2%80%93idf
score := float32(0)
// E.g. queryTerms = ["gandalf", "wizard"]
for term := range queryTerms {
    freq := 0
    // How many times the term occurs in the document? Is gandalf
    // mentioned a lot?
    if termItem, ok := docItem.Terms[term]; ok {
        freq = termItem.Frequency
    }
    // Calculate the tf-idf score
    // term frequency tf = how often does the term occur in the document
    // with respect to the length of the document
    tf := float32(freq) / float32(docItem.Length)
    termSetItem, _ := index.getSetCacheItem(term)
    // inverse document frequency idf = how rare is the term in the
    // index
    idf := math.Log10(float64(index.numDocs) / float64(termSetItem.set.GetCardinality()+1))
    score += tf * float32(idf)
}
// ---------------------------
```