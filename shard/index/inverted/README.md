# Inverted Index

The inverted index is a map of terms to a set of documents containing those terms. It is effectively an inverse look up table. An inverted index is useful to filter or search for items with specific values quickly. For example, all the documents that are tagged "critical" would mean, look up the term "critical" and fetch it's set of document ids.

The document set is stored as a [Roaring bitmap](https://github.com/RoaringBitmap/roaring). So the storage layout in the bucket is `term -> roaring set`.

Otherwise, nothing magical happens. When a search query comes in, we look up its or a range of document sets and merge them using logical OR.

## Byte sortability

To perform range, greater than, less than etc operations, the term key in bytes needs to be sortable. For strings this is not a problem, but for signed integers this means some sort of [big endian](https://en.wikipedia.org/wiki/Endianness) encoding with a special care to sign bits. This is currently handled by the `sortable.go` file.

If a new type is to be added, it must be taken with that its byte encoding `[]byte` format follows this property.