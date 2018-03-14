package weasel

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/peterbourgon/diskv"
	"github.com/pkg/errors"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"io/ioutil"
	"log"
	"reflect"
	"unicode"
	"path"
	"os"
	"encoding/binary"
	"github.com/hashicorp/golang-lru"
	"sync"
)

var (
	//flatTransform  = func(s string) []string { return []string{} }
	blockTransform = func(blockSize int) func(string) []string {
		return func(s string) []string {
			var (
				sliceSize = len(s) / blockSize
				pathSlice = make([]string, sliceSize)
			)
			for i := 0; i < sliceSize; i++ {
				from, to := i*blockSize, (i*blockSize)+blockSize
				pathSlice[i] = s[from:to]
			}
			return pathSlice
		}
	}
)

// TermID is a representation for the ID of a term.
type TermID uint32

// DocumentID is a representation for the ID of a document.
type DocumentID uint32

type DocumentIDs []DocumentID

type postingItem struct {
	DocumentID
	TermID
	Term  string
	Field string
}

// IndexedDocument is a document that gets returned if the index is asked for a document
type IndexedDocument struct {
	ID            string
	TermFrequency map[string]map[TermID]uint32
	Positions     map[string]map[TermID][]int32
	Source        map[string]interface{}
}

// TermStatistics contains the information about a term.
type TermStatistics struct {
	ID                 TermID
	TotalTermFrequency uint32
	DocumentFrequency  uint32
}

// InvertedIndex stores statistics and indexes the documents. Since the index should grow at about a logarithmic rate,
// it shouldn't matter too much that strings are being used as keys to term statistics.
//
// It is probably possible to simply use a hash of the term to get a term id which might improve memory usage.
//
// Unfortunately, since there are so many maps being used, it is nigh impossible that indexing can be parallelised with
// this implementation of an inverted index.
type InvertedIndex struct {
	// Number of documents in the index.
	// We can store just over 4 billion documents with a uint32.
	NumDocs uint32

	// Name of the index.
	Name string

	// Ensure each field is the correct type.
	// field->(type)
	FieldMapping map[string]Indexable

	// Mapping to determine if the source of the field is indexed.
	ContainsSource map[string]bool

	// Term statistics
	// termID->TermStatistics
	TermStatistics map[string]TermStatistics

	// Mapping of internal documentIDs to the string IDs.
	DocumentMapping map[DocumentID]string

	// Analysers applied to certain fields.
	// map[field]->analyser
	Analysers map[string][]Analyser

	// Posting is a map of term ID to document ID.
	// field->termID->docID
	// map[string]map[TermID][]DocumentID
	posting Posting

	postingChan  chan postingItem
	postingCache *lru.Cache

	// Persistent disk.
	disk *diskv.Diskv
}

// AddAnalyser sets analysers for a field in the index.
func AddAnalyser(field string, analyser ...Analyser) func(index *InvertedIndex) {
	return func(index *InvertedIndex) {
		index.Analysers[field] = analyser
	}
}

func NoSource(fields ...string) func(index *InvertedIndex) {
	return func(index *InvertedIndex) {
		for _, field := range fields {
			index.ContainsSource[field] = false
		}
	}
}

// NewInvertedIndex is the constructor for the InvertedIndex. It takes a document mapping.
func NewInvertedIndex(name string, mapping map[string]Indexable, options ...func(index *InvertedIndex)) InvertedIndex {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		os.Mkdir(name, 0777)
	}

	// Simplest transform function: put all the data files into the base dir.
	i := &InvertedIndex{
		Name:            name,
		FieldMapping:    mapping,
		TermStatistics:  make(map[string]TermStatistics),
		ContainsSource:  make(map[string]bool),
		DocumentMapping: make(map[DocumentID]string),
		Analysers:       make(map[string][]Analyser),
		disk: diskv.New(diskv.Options{
			BasePath: path.Join(name, "index"),
			//TempDir:      path.Join(name, "index_tmp"),
			Transform:    blockTransform(3),
			CacheSizeMax: 4096 * 1024,
			Compression:  diskv.NewGzipCompression(),
		}),
	}

	// By default store the sources of all fields.
	var fields []string
	for field := range mapping {
		i.ContainsSource[field] = true
		fields = append(fields, field)
	}

	p, err := NewPosting(name, fields...)
	if err != nil {
		panic(err)
	}
	i.posting = p

	// Apply optional functions to the index.
	for _, option := range options {
		option(i)
	}

	return *i
}

// isMn is a predicate for checking for non-spacing marks.
func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: non-spacing marks
}

func (t TermID) toBytes(field string) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(t))
	return append([]byte(field), b...)
}

func bytesToString(data []byte) (s string) {
	for _, d := range data {
		s += fmt.Sprintf("%d", d)
	}
	return
}

func (ids DocumentIDs) toBytes() []byte {
	var b []byte
	for _, id := range ids {
		d := make([]byte, 4)
		binary.LittleEndian.PutUint32(d, uint32(id))
		b = append(b, d...)
	}
	return b
}

func documentIDsFromBytes(data []byte) []DocumentID {
	d := make([]DocumentID, len(data)/4)
	for i, j := 0, 0; i < len(data); i += 4 {
		d[j] = DocumentID(binary.LittleEndian.Uint32(data[i:i+4]))
		j++
	}
	return d
}

// toBytes converts a document to a byte representation.
func (index IndexedDocument) toBytes() ([]byte, error) {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(index)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// tokenise normalises and splits fields into a slice of strings
func tokenise(value interface{}) []string {
	switch v := value.(type) {
	case string:
		{
			var tokens []string
			var token string
			for _, char := range v {
				if unicode.IsSpace(char) || unicode.IsPunct(char) || unicode.IsControl(char) {
					tokens = append(tokens, token)
					token = ""
				} else if unicode.IsGraphic(char) {
					token += string(char)
				}
			}
			return tokens
		}
	case int, int64, float32, float64:
	}
	return []string{}
}

func (index InvertedIndex) DocumentIDs(field, term string) (ids []DocumentID) {
	var termID TermID
	if t, ok := index.TermStatistics[term]; ok {
		termID = t.ID
	} else {
		return
	}

	v, err := index.posting.Read(field, termID)
	if err != nil {
		return
	}

	if v == nil {
		return
	}

	return documentIDsFromBytes(v)
}

func (index *InvertedIndex) BulkIndex(docs []Document) error {
	for _, doc := range docs {
		err := index.Index(doc)
		if err != nil {
			return err
		}
	}
	return nil
}

var mu = sync.Mutex{}

// Index takes a document and adds it to the inverted index. It also stores the document source, plus statistics
func (index *InvertedIndex) Index(d Document) error {
	// make sure the mapping and the source have the same number of keys
	if len(index.FieldMapping) != len(d.Source()) {
		return errors.New("field count mismatch")
	}

	termFrequency := make(map[string]map[TermID]uint32)
	positions := make(map[string]map[TermID][]int32)

	t := transform.Chain(norm.NFD, runes.Remove(runes.Predicate(isMn)), norm.NFC)

	index.NumDocs++
	docID := index.NumDocs

	// check the types of the mapping match that of the source
	for field, source := range d.source {

		positions[field] = make(map[TermID][]int32)
		termFrequency[field] = make(map[TermID]uint32)

		// type-check the document to the mapping in the index
		var (
			ok    bool
			dType Indexable
		)
		if dType, ok = index.FieldMapping[field]; !ok {
			return fmt.Errorf("field %v is not defined in index mapping", field)
		}

		vType := Indexable(reflect.TypeOf(source).Kind())

		if !(vType == dType) {
			return fmt.Errorf("incorrect type for %v. Expecting %v, got %v", field, dType, vType)
		}

		if vType == IndexableString {
			source, _, _ = transform.String(t, source.(string))
		}

		// Add new terms to the term mapping
		terms := tokenise(source)
		for i, term := range terms {

			if len(term) == 0 {
				continue
			}

			// Apply analysers to the field
			for _, analyser := range index.Analysers[field] {
				analysed, err := analyser.Analyse(term)
				if err != nil {
					return err
				}
				term = analysed
			}

			if len(term) == 0 {
				continue
			}

			// Create a position of the term in the posting if one does not exist
			if stats, ok := index.TermStatistics[term]; !ok {
				stats.ID = TermID(uint32(len(index.TermStatistics)))
				index.TermStatistics[term] = stats
			}

			index.DocumentMapping[DocumentID(docID)] = d.ID
			termID := index.TermStatistics[term].ID

			if _, ok := positions[field][termID]; !ok {
				go func() {
					var storedDocIDs DocumentIDs
					v, err := index.posting.Read(field, termID)
					if err != nil {
						storedDocIDs = []DocumentID{}
					} else if v == nil {
						storedDocIDs = []DocumentID{}
					} else {
						storedDocIDs = documentIDsFromBytes(v)
					}
					docIDs := append(storedDocIDs, DocumentID(docID))
					//fmt.Println(termID, term, field, docIDs)

					err = index.posting.Write(field, termID, docIDs.toBytes())
					if err != nil {
						panic(err)
					}
				}()
			}

			// Recalculate term frequency
			if _, ok := index.TermStatistics[term]; ok {
				tmp := index.TermStatistics[term]
				tmp.TotalTermFrequency++
				if _, ok := positions[field][termID]; !ok {
					tmp.DocumentFrequency++
				}
				index.TermStatistics[term] = tmp
			} else {
				// Create a new term statistics object if one hasn't been created yet.
				index.TermStatistics[term] = TermStatistics{
					TotalTermFrequency: 1,
					DocumentFrequency:  1,
				}
			}
			termFrequency[field][termID]++
			positions[field][termID] = append(positions[term][termID], int32(i))
		}

		if index.ContainsSource[field] == false {
			delete(d.source, field)
		}
	}

	indexedDocument := IndexedDocument{Source: d.source, ID: d.ID, TermFrequency: termFrequency, Positions: positions}

	go func() {
		b, err := indexedDocument.toBytes()
		if err != nil {
			panic(err)
		}
		err = index.disk.Write(indexedDocument.ID, b)
		if err != nil {
			panic(err)
		}
	}()

	return nil
}

// Get is a function that returns the source of a single document in the index.
func (index *InvertedIndex) Get(docID string) (IndexedDocument, error) {
	source, err := index.disk.Read(docID)
	if err != nil {
		return IndexedDocument{}, err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(source))
	var doc IndexedDocument
	err = dec.Decode(&doc)
	if err != nil {
		return IndexedDocument{}, err
	}

	return doc, nil
}

// Dump an index to file. This is a one-to-one in-memory dump of the inverted index, plus the statistics.
func (index *InvertedIndex) Dump() error {
	RegisterAnalysers()

	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)

	err := enc.Encode(index)
	if err != nil {
		log.Panicln(err)
	}

	return ioutil.WriteFile(path.Join(index.Name, index.Name+".weasel"), buff.Bytes(), 0664)
}

// LoadIndex loads an index from a file.
func LoadIndex(store string) (InvertedIndex, error) {
	RegisterAnalysers()

	data, err := ioutil.ReadFile(path.Join(store, store+".weasel"))
	if err != nil {
		return InvertedIndex{}, err
	}

	dec := gob.NewDecoder(bytes.NewReader(data))
	var i InvertedIndex
	err = dec.Decode(&i)
	if err != nil {
		return InvertedIndex{}, err
	}

	var fields []string
	for field := range i.FieldMapping {
		fields = append(fields, field)
	}
	p, err := NewPosting(store, fields...)
	if err != nil {
		panic(err)
	}
	i.posting = p

	i.disk = diskv.New(diskv.Options{
		BasePath: path.Join(store, "index"),
		//TempDir:      path.Join(store, "index_tmp"),
		Transform:    blockTransform(3),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	return i, nil
}
