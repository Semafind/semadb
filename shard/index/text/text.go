/*
Package text provides a text index. The text index is used to index and
search text fields in documents. The scoring is based on the term frequency and
inverse document frequency of the terms in the index, aka tf-idf.

We use the jargon document to refer to a single piece of text (string).

To not reinvent the wheel, the text index uses the bleve text analysis to
convert a document into a list of terms. The terms are then stored in the index
along with the document id and the term frequency. The term frequency is the
number of times the term occurs in the document.

The term to document index is handled by roaring bitmaps. The term to document index
is a map of terms to a set of document ids.

Storage in bucket:
NUMDOCUMENTSKEY: The number of documents in the index. This is used to calculate tf-idf score.
t<TERM>s: roaring set of document ids where the term occurs.
d<DOCID>: document cache item containing the terms and their frequencies in the document.
*/
package text

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/bleve/v2/analysis"

	// The init function in the package registers the analysers so we have to
	// import them to get the side effect of the registration.
	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/standard"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/utils"
	"github.com/vmihailenco/msgpack/v5"
)

// Used to store the number of documents in the index which is then used to
// calculate the IDF for the terms in the index.
const numDocumentsKey = "_numDocuments"

// ---------------------------

// For example, the term "hello" would be stored as "thellos".
func termKey(term string) []byte {
	return []byte("t" + term + "s")
}

// For example, the document with id 123 would be stored as "d<binary123>".
func documentKey(id uint64) []byte {
	key := [9]byte{}
	key[0] = 'd'
	binary.LittleEndian.PutUint64(key[1:], id)
	return key[:]
}

// ---------------------------
// Stores the term and the start and end position in the document. Usually
// obtained after analysing / tokenising the document.
type Token struct {
	Term  string
	Start int
	End   int
}

// Common interface for analysers. The bleveAnalyser implements this interface.
// It is here to allow for different analysers in the future.
type analyser interface {
	Analyse(string) ([]Token, error)
}

// ---------------------------
/* The bleve analysis package uses a global registry and init functions to
 * register them. So we need a cache to get the analyser. */
var analyserCache = registry.NewCache()

type bleeveAnalyser struct {
	analyser analysis.Analyzer
}

func newBeleeveAnalyser(name string) (*bleeveAnalyser, error) {
	analyser, err := analyserCache.AnalyzerNamed(name)
	if err != nil {
		return nil, fmt.Errorf("error getting analyser: %w", err)
	}
	return &bleeveAnalyser{analyser: analyser}, nil
}

func (a *bleeveAnalyser) Analyse(text string) ([]Token, error) {
	tokenStream := a.analyser.Analyze([]byte(text))
	tokens := make([]Token, len(tokenStream))
	for i, token := range tokenStream {
		tokens[i] = Token{
			Term:  string(token.Term),
			Start: token.Start,
			End:   token.End,
		}
	}
	return tokens, nil
}

// ---------------------------

// setCacheItem is used to store the roaring set and a flag to indicate if the
// set has been modified.
type setCacheItem struct {
	set     *roaring64.Bitmap
	isDirty bool
}

type Term struct {
	Frequency int `msgpack:"frequency"`
}

type docCacheItem struct {
	Terms  map[string]Term `msgpack:"terms"`
	Length int             `msgpack:"length"`
}

// indexText is the main struct for the text index. The mutex is used to protect
// both caches.
type indexText struct {
	analyser analyser
	setCache map[string]*setCacheItem
	docCache map[uint64]*docCacheItem
	numDocs  uint64
	bucket   diskstore.Bucket
	mu       sync.Mutex
}

// NewIndexText creates a new text index. The analyser parameter is the name of
// the analyser to use. The analyser is used to convert a document into a list
// of tokens.
func NewIndexText(b diskstore.Bucket, params models.IndexTextParameters) (*indexText, error) {
	analyser, err := newBeleeveAnalyser(params.Analyser)
	if err != nil {
		return nil, fmt.Errorf("error getting analyser: %w", err)
	}
	it := &indexText{
		analyser: analyser,
		setCache: make(map[string]*setCacheItem),
		docCache: make(map[uint64]*docCacheItem),
		bucket:   b,
	}
	// ---------------------------
	it.numDocs = it.initSize()
	// ---------------------------
	return it, nil
}

func (index *indexText) initSize() uint64 {
	v := index.bucket.Get([]byte(numDocumentsKey))
	if v == nil {
		return 0
	}
	vv := conversion.BytesToUint64(v)
	return vv
}

func (index *indexText) getSetCacheItem(term string) (*setCacheItem, error) {
	item, ok := index.setCache[term]
	if !ok {
		// Attempt to read from the bucket
		v := index.bucket.Get(termKey(term))
		rSet := roaring64.New()
		if v != nil {
			if _, err := rSet.ReadFrom(bytes.NewReader(v)); err != nil {
				return nil, fmt.Errorf("error reading set from bytes: %w", err)
			}
		}
		item = &setCacheItem{
			set: rSet,
		}
		index.setCache[term] = item
	}
	return item, nil
}

func (index *indexText) getDocCacheItem(id uint64) (*docCacheItem, error) {
	item, ok := index.docCache[id]
	if !ok {
		v := index.bucket.Get(documentKey(id))
		if v == nil {
			return nil, nil
		}
		item = &docCacheItem{}
		if err := msgpack.Unmarshal(v, item); err != nil {
			return nil, fmt.Errorf("error unmarshalling doc cache item: %w", err)
		}
		index.docCache[id] = item
	}
	return item, nil
}

type Document struct {
	Id   uint64
	Text string
}

type analysedDocument struct {
	Id          uint64
	Frequencies map[string]int
	Length      int
}

func (index *indexText) InsertUpdateDelete(ctx context.Context, in <-chan Document) <-chan error {
	analysedDocs, errC := index.parallelAnalyse(ctx, in)
	// ---------------------------
	/* We are currently leaving the update of sets and documents single threaded
	 * to avoid excessive locking initially. One reason is that roaring bitmaps
	 * are not thread-safe so we would have to lock around them. We should
	 * monitor the performance of a single thread iterating over all the changes
	 * before we consider parallelising it. */
	writeErrC := make(chan error, 1)
	// This is the aforementioned single thread
	go func() {
		defer close(writeErrC)
		index.mu.Lock()
		defer index.mu.Unlock()
		sinkErrC := utils.SinkWithContext(ctx, analysedDocs, index.processAnalysedDoc)
		if err := <-sinkErrC; err != nil {
			writeErrC <- fmt.Errorf("error processing analysed docs: %w", err)
			return
		}
		writeErrC <- index.flush()
	}()
	return utils.MergeErrorsWithContext(ctx, errC, writeErrC)
}

// Updates the index with the analysed document. The document is either inserted,
// updated or deleted based on the length of the document.
func (index *indexText) processAnalysedDoc(ad analysedDocument) error {
	docItem, err := index.getDocCacheItem(ad.Id)
	if err != nil {
		return fmt.Errorf("error getting doc cache item: %w", err)
	}
	switch {
	// ---------------------------
	case docItem == nil && ad.Length > 0:
		// Insert
		terms := make(map[string]Term)
		for term, frequency := range ad.Frequencies {
			terms[term] = Term{
				Frequency: frequency,
			}
			setItem, err := index.getSetCacheItem(term)
			if err != nil {
				return fmt.Errorf("error getting set cache item: %w", err)
			}
			setItem.isDirty = setItem.set.CheckedAdd(ad.Id)
		}
		index.docCache[ad.Id] = &docCacheItem{
			Terms:  terms,
			Length: ad.Length,
		}
		index.numDocs++
	// ---------------------------
	case docItem != nil && ad.Length == 0:
		// Delete
		for term := range docItem.Terms {
			setItem, err := index.getSetCacheItem(term)
			if err != nil {
				return fmt.Errorf("error getting set cache item: %w", err)
			}
			setItem.isDirty = setItem.set.CheckedRemove(ad.Id)
		}
		index.docCache[ad.Id] = nil
		index.numDocs--
	// ---------------------------
	case docItem != nil && ad.Length > 0:
		// Update
		// We need to remove the old terms from the set that are not in the new
		// document and add the new terms to the set that are not in the old
		for term := range docItem.Terms {
			if _, ok := ad.Frequencies[term]; ok {
				continue
			}
			setItem, err := index.getSetCacheItem(term)
			if err != nil {
				return fmt.Errorf("error getting set cache item: %w", err)
			}
			setItem.isDirty = setItem.set.CheckedRemove(ad.Id)
		}
		terms := make(map[string]Term)
		for term, freq := range ad.Frequencies {
			terms[term] = Term{
				Frequency: freq,
			}
			if _, ok := docItem.Terms[term]; ok {
				continue
			}
			setItem, err := index.getSetCacheItem(term)
			if err != nil {
				return fmt.Errorf("error getting set cache item: %w", err)
			}
			setItem.isDirty = setItem.set.CheckedAdd(ad.Id)
		}
		docItem.Terms = terms
		docItem.Length = ad.Length
	// ---------------------------
	default:
		return fmt.Errorf("unexpected state: docItem: %v, ad.Frequencies: %v", docItem, ad.Frequencies)
	}
	return nil
}

func (index *indexText) parallelAnalyse(ctx context.Context, in <-chan Document) (<-chan analysedDocument, <-chan error) {
	numWorkers := runtime.NumCPU() - 1
	outs := make([]<-chan analysedDocument, numWorkers)
	errCs := make([]<-chan error, numWorkers)
	for i := 0; i < numWorkers; i++ {
		out, errC := utils.TransformWithContext(ctx, in, func(doc Document) (ad analysedDocument, skip bool, err error) {
			// Perform analysis
			tokens, err := index.analyser.Analyse(doc.Text)
			if err != nil {
				return
			}
			// Calculate term frequencies from tokens
			freq := make(map[string]int)
			for _, t := range tokens {
				freq[t.Term]++
			}
			ad.Id = doc.Id
			ad.Frequencies = freq
			ad.Length = len(tokens)
			return
		})
		outs[i] = out
		errCs[i] = errC
	}
	return utils.MergeWithContext(ctx, outs...), utils.MergeErrorsWithContext(ctx, errCs...)
}

// Flush writes the index changes to the bucket. It should be called after write operation.
func (index *indexText) flush() error {
	// ---------------------------
	numDocs := index.numDocs
	if err := index.bucket.Put([]byte(numDocumentsKey), conversion.Uint64ToBytes(numDocs)); err != nil {
		return fmt.Errorf("error putting num documents to bucket: %w", err)
	}
	// ---------------------------
	for term, item := range index.setCache {
		if !item.isDirty {
			continue
		}
		// ---------------------------
		if item.set.IsEmpty() {
			if err := index.bucket.Delete(termKey(term)); err != nil {
				return fmt.Errorf("error deleting term set from bucket: %w", err)
			}
			continue
		}
		// ---------------------------
		setBytes, err := item.set.ToBytes()
		if err != nil {
			return fmt.Errorf("error converting term set to bytes: %w", err)
		}
		if err := index.bucket.Put(termKey(term), setBytes); err != nil {
			return fmt.Errorf("error putting term set to bucket: %w", err)
		}
	}
	// ---------------------------
	for id, item := range index.docCache {
		if item == nil || item.Length == 0 {
			if err := index.bucket.Delete(documentKey(id)); err != nil {
				return fmt.Errorf("error deleting doc cache item from bucket: %w", err)
			}
			continue
		}
		val, err := msgpack.Marshal(item)
		// ---------------------------
		if err != nil {
			return fmt.Errorf("error marshalling doc cache item: %w", err)
		}
		if err := index.bucket.Put(documentKey(id), val); err != nil {
			return fmt.Errorf("error putting doc cache item to bucket: %w", err)
		}
	}
	// ---------------------------
	return nil
}

func (index *indexText) Search(options models.SearchTextOptions) ([]models.SearchResult, error) {
	index.mu.Lock()
	defer index.mu.Unlock()
	// ---------------------------
	// Analyse query
	tokens, err := index.analyser.Analyse(options.Value)
	if err != nil {
		return nil, fmt.Errorf("error analysing text: %w", err)
	}
	queryTerms := make(map[string]struct{})
	for _, token := range tokens {
		queryTerms[token.Term] = struct{}{}
	}
	// ---------------------------
	sets := make([]*roaring64.Bitmap, 0, len(queryTerms))
	for term := range queryTerms {
		item, err := index.getSetCacheItem(term)
		if err != nil {
			return nil, fmt.Errorf("error getting set cache item: %w", err)
		}
		sets = append(sets, item.set)
	}
	var finalSet *roaring64.Bitmap
	if options.Operator == models.OperatorContainsAll {
		finalSet = roaring64.FastAnd(sets...)
	} else {
		finalSet = roaring64.FastOr(sets...)
	}
	// ---------------------------
	// Get the documents and rank them using tf-idf
	results := make([]models.SearchResult, 0, finalSet.GetCardinality())
	it := finalSet.Iterator()
	for it.HasNext() {
		docId := it.Next()
		docItem, err := index.getDocCacheItem(docId)
		if err != nil {
			return nil, fmt.Errorf("error getting doc cache item: %w", err)
		}
		if docItem == nil {
			return nil, fmt.Errorf("doc cache item not found for id: %d", docId)
		}
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
		weightedScore := score
		if options.Weight != nil {
			weightedScore *= *options.Weight
		}
		sr := models.SearchResult{
			NodeId:     docId,
			Score:      &score,
			FinalScore: &weightedScore,
		}
		results = append(results, sr)
	}
	// ---------------------------
	slices.SortFunc(results, func(a, b models.SearchResult) int {
		return cmp.Compare(*b.Score, *a.Score)
	})
	// ---------------------------
	if len(results) > options.Limit {
		results = results[:options.Limit]
	}
	// ---------------------------
	return results, nil
}
