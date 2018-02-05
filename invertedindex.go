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
	"hash/fnv"
	"io/ioutil"
	"log"
	"reflect"
	"unicode"
)

var (
	flatTransform  = func(s string) []string { return []string{} }
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
type DocumentID uint64

// IndexedDocument is a document that gets returned if the index is asked for a document
type IndexedDocument struct {
	ID            string
	TermFrequency map[string]map[uint32]uint32
	Source        map[string]interface{}
}

// TermStatistics contains the information about a term.
type TermStatistics struct {
	ID                 uint32
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
	// Number of documents in the index
	NumDocs uint64

	// Name of the index
	Name string

	// Ensure each field is the correct type
	// field->(type)
	FieldMapping map[string]reflect.Kind

	// Term statistics
	// termId->TermStatistics
	TermStatistics map[string]TermStatistics

	// Mapping of internal documentIDs to the string IDs.
	DocumentMapping map[DocumentID]string

	// Posting is a map of term id to document id
	// field->termId->docId
	Posting map[string]map[TermID][]DocumentID

	// Persistent disk.
	disk *diskv.Diskv
}

// NewInvertedIndex is the constructor for the InvertedIndex. It takes a document mapping.
func NewInvertedIndex(name string, mapping map[string]reflect.Kind) InvertedIndex {
	// Simplest transform function: put all the data files into the base dir.
	return InvertedIndex{
		Name:            name,
		Posting:         make(map[string]map[TermID][]DocumentID),
		FieldMapping:    mapping,
		TermStatistics:  make(map[string]TermStatistics),
		DocumentMapping: make(map[DocumentID]string),
		disk: diskv.New(diskv.Options{
			BasePath:     name,
			Transform:    blockTransform(3),
			CacheSizeMax: 4096 * 1024,
			Compression:  diskv.NewGzipCompression(),
		}),
	}
}

// termVector creates a sparse vector of the terms inside a document
func termVector(terms []string, mapping map[string]TermStatistics) []uint32 {
	vec := make([]uint32, len(terms))
	for i, t := range terms {
		vec[i] = mapping[t].ID
	}
	return vec
}

// isMn is a predicate for checking for non-spacing marks.
func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: non-spacing marks
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

// extractTerms normalises and splits fields into a slice of strings
func extractTerms(value interface{}) []string {
	switch v := value.(type) {
	case string:
		{
			textLength := len(v)

			if textLength == 0 {
				return []string{}
			}

			tokens := make([]string, 0)
			var token string

			for _, char := range v {
				if unicode.IsSpace(char) || unicode.IsPunct(char) || unicode.IsControl(char) {
					tokens = append(tokens, token)
					token = ""
				} else if unicode.IsGraphic(char) {
					token += string(unicode.ToLower(char))
				}
			}
			return tokens
		}
	case int, int64, float32, float64:
	}
	return []string{}
}

func (index *InvertedIndex) hashTerm(term string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(term))
	return h.Sum64()
}

// Index takes a document and adds it to the inverted index. It also stores the document source, plus statistics
func (index *InvertedIndex) Index(d Document) error {
	// make sure the mapping and the source have the same number of keys
	if len(index.FieldMapping) != len(d.Source()) {
		return errors.New("field count mismatch")
	}

	termFrequency := make(map[string]map[uint32]uint32)

	t := transform.Chain(norm.NFD, runes.Remove(runes.Predicate(isMn)), norm.NFC)

	docID := index.NumDocs
	// check the types of the mapping match that of the source
	for k := range d.Source() {
		termFrequency[k] = make(map[uint32]uint32)

		// type-check the document to the mapping in the index
		dType := index.FieldMapping[k]

		v := d.Source()[k]
		vType := reflect.TypeOf(v).Kind()
		if !(vType == dType) {
			return fmt.Errorf("incorrect type for %v. Expecting %v, got %v", k, dType, vType)
		}

		if vType == reflect.String {
			v, _, _ = transform.String(t, d.Source()[k].(string))
		}

		// Add new terms to the term mapping
		terms := extractTerms(v)
		distinctTerms := make(map[uint32]bool)
		for _, t := range terms {
			// Create a position of the term in the posting if one does not exist
			if stats, ok := index.TermStatistics[t]; !ok {
				stats.ID = uint32(len(index.TermStatistics))
				index.TermStatistics[t] = stats
			}

			termID := index.TermStatistics[t].ID

			// Recalculate term frequency
			if _, ok := index.TermStatistics[t]; ok {
				tmp := index.TermStatistics[t]
				tmp.TotalTermFrequency++
				if _, ok := distinctTerms[termID]; !ok {
					tmp.DocumentFrequency++
					distinctTerms[termID] = true
				}
				index.TermStatistics[t] = tmp
			} else {
				// Create a new term statistics object if one hasn't been created yet.
				index.TermStatistics[t] = TermStatistics{
					TotalTermFrequency: 1,
					DocumentFrequency:  1,
				}
			}
		}

		// add the document to the inverted index
		for _, p := range termVector(terms, index.TermStatistics) {
			if _, ok := index.Posting[k]; !ok {
				index.Posting[k] = make(map[TermID][]DocumentID)
			}
			index.Posting[k][TermID(p)] = append(index.Posting[k][TermID(p)], DocumentID(docID))
			index.DocumentMapping[DocumentID(docID)] = d.ID
			termFrequency[k][p]++
		}
	}

	doc := IndexedDocument{Source: d.Source(), ID: d.ID, TermFrequency: termFrequency}
	b, err := doc.toBytes()
	if err != nil {
		return err
	}
	index.disk.Write(doc.ID, b)
	docID++
	index.NumDocs++

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
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)

	err := enc.Encode(index)
	if err != nil {
		log.Panicln(err)
	}

	return ioutil.WriteFile(index.Name+".weasel", buff.Bytes(), 0664)
}

// LoadIndex loads an index from a file.
func LoadIndex(filename, store string) (InvertedIndex, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return InvertedIndex{}, err
	}

	dec := gob.NewDecoder(bytes.NewReader(data))
	var i InvertedIndex
	err = dec.Decode(&i)
	if err != nil {
		return InvertedIndex{}, err
	}

	i.disk = diskv.New(diskv.Options{
		BasePath:     store,
		Transform:    blockTransform(3),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	return i, nil
}
