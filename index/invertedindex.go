// Package index implements an inverted index used to store documents from package document.
package index

import (
	"github.com/hscells/weasel/document"
	"github.com/pkg/errors"
	"reflect"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"fmt"
	"encoding/gob"
	"bytes"
	"log"
	"io/ioutil"
	"strconv"
	"os"
	"sort"
	"unicode"
)

type int64arr []int64

func (a int64arr) Len() int {
	return len(a)
}
func (a int64arr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a int64arr) Less(i, j int) bool {
	return a[i] < a[j]
}

// Number of documents to bulk index before dumping. It seems like it's fairly okay to keep this number high
var dumpNum int64 = 50000

// IndexedDocument is a document that gets returned if the index is asked for a document
type IndexedDocument struct {
	Id            int64
	Source        map[string]interface{}
	TermFrequency map[string]map[int64]int64
	Rank          int64
}

type TermStatistics struct {
	TotalTermFrequency int64
	DocumentFrequency  int64
}

// InvertedIndex stores statistics and indexes the documents
type InvertedIndex struct {
	// Name of the index
	Name            string

	// Number of documents in the index
	NumDocs         int64

	// Ensure each field is the correct type
	// field->(type)
	DocumentMapping map[string]reflect.Kind

	// Posting is a map of term id to document id
	// field->termId->docId
	Posting         map[string]map[int64][]int64

	// Mapping of terms to the index in the term vector
	// term->position
	TermMapping     map[string]int64

	// Term statistics
	// termId->TermStatistics
	TermStatistics  map[int64]TermStatistics

	// Documents added to the index. This is volatile and should not be considered as the full collection.
	// docId->[]docSource
	documents       map[int64]IndexedDocument

	// Documents which are cached in memory and have already been indexed
	cachedDocuments map[int64]IndexedDocument

	// Filenames documents are stored in
	DocumentFiles   []int64
}

// New is the constructor for the InvertedIndex. It takes a document mapping.
func New(name string, mapping map[string]reflect.Kind) InvertedIndex {
	return InvertedIndex{
		Name: name,
		Posting: make(map[string]map[int64][]int64),
		DocumentMapping: mapping,
		TermMapping: make(map[string]int64),
		TermStatistics: make(map[int64]TermStatistics),
		documents: make(map[int64]IndexedDocument),
		cachedDocuments: make(map[int64]IndexedDocument),
	}
}

// expandVector takes a sparse vector and a transforms it into a regular term vector
func expandVector(vec []int64, mapping map[string]int64) []int64 {
	return []int64{0}
}

// termVector creates a sparse vector of the terms inside a document
func termVector(terms []string, mapping map[string]int64) []int64 {
	vec := make([]int64, len(terms))
	for i, t := range terms {
		vec[i] = mapping[t]
	}
	return vec
}

func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}

// extractTerms normalises and splits fields into a slice of strings
func extractTerms(value interface{}) []string {
	switch v := value.(type) {
	case string :{
		textLength := len(v)

		if textLength == 0 {
			return []string{}
		}

		tokens := make([]string, 0)
		var token string

		for _, char := range v {
			if !unicode.IsPunct(char) && !unicode.IsControl(char) && !unicode.IsSpace(char) {
				token += string(unicode.ToLower(char))
			} else if unicode.IsSpace(char) {
				tokens = append(tokens, token)
				token = ""
			}
		}
		return tokens
	}
	case int, int64, float32, float64:
	}
	return []string{}
}

// Index takes a document and adds it to the inverted index. It also stores the document source, plus statistics
func (i *InvertedIndex) Index(d document.Document) error {
	// make sure the mapping and the source have the same number of keys
	if len(i.DocumentMapping) != len(d.Source()) {
		return errors.New("Field count mismatch.")
	}

	termFrequency := make(map[string]map[int64]int64)

	t := transform.Chain(norm.NFD, transform.RemoveFunc(isMn), norm.NFC)

	docId := i.NumDocs
	// check the types of the mapping match that of the source
	for k := range d.Source() {
		termFrequency[k] = make(map[int64]int64)

		// type-check the document to the mapping in the index
		dType := i.DocumentMapping[k]

		v := d.Source()[k]
		vType := reflect.TypeOf(v).Kind()
		if !(vType == dType) {
			return errors.New(fmt.Sprintf("Incorrect type for %v. Expecting %v, got %v.", k, dType, vType))
		}

		if vType == reflect.String {
			v, _, _ = transform.String(t, d.Source()[k].(string))
		}

		// Add new terms to the term mapping
		terms := extractTerms(v)
		distinctTerms := make(map[int64]bool)
		for _, t := range terms {
			// Create a position of the term in the posting if one does not exist
			if _, ok := i.TermMapping[t]; !ok {
				i.TermMapping[t] = int64(len(i.TermMapping))
			}

			termId := i.TermMapping[t]

			// Recalculate term frequency
			if _, ok := i.TermStatistics[termId]; ok {
				tmp := i.TermStatistics[termId]
				tmp.TotalTermFrequency++
				if _, ok := distinctTerms[termId]; !ok {
					tmp.DocumentFrequency++
					distinctTerms[termId] = true
				}
				i.TermStatistics[termId] = tmp
			} else {
				// Create a new term statistics object if one hasn't been created yet.
				i.TermStatistics[termId] = TermStatistics{
					TotalTermFrequency: 1,
					DocumentFrequency: 1,
				}
			}
		}

		// add the document to the inverted index
		for _, p := range termVector(terms, i.TermMapping) {
			if _, ok := i.Posting[k]; !ok {
				i.Posting[k] = make(map[int64][]int64)
			}
			i.Posting[k][p] = append(i.Posting[k][p], docId)
			termFrequency[k][p]++
		}
	}

	doc := IndexedDocument{Source: d.Source(), Id: docId, TermFrequency: termFrequency}
	i.documents[docId] = doc
	i.NumDocs++

	return nil
}

// BulkIndex will index a whole list of documents at once. This is the preferred way of indexing, since as a side
// effect, this function will dump the documents to disk at regular intervals.
func (i *InvertedIndex) BulkIndex(docs []document.Document) error {
	for idx, d := range docs {
		err := i.Index(d)
		if err != nil {
			return err
		}

		if int64(idx + 1) % dumpNum == 0 {
			err := i.DumpDocs()
			if err != nil {
				return err
			}
		}
	}
	err := i.DumpDocs()
	if err != nil {
		return err
	}
	return nil
}

// Get is a function that returns the source of a single document in the index.
func (i *InvertedIndex) Get(docId int64) (IndexedDocument, error) {
	if v, ok := i.documents[docId]; ok {
		return v, nil
	} else if v, ok := i.cachedDocuments[docId]; ok {
		return v, nil
	} else {
		// Try to read all the dumped files, searching for the doc
		for _, docFile := range i.DocumentFiles {
			if docId <= docFile {
				data, err := ioutil.ReadFile(i.createDumpDocsName(docFile))
				if err != nil {
					return IndexedDocument{}, err
				}

				dec := gob.NewDecoder(bytes.NewReader(data))
				var indexedDocs map[int64]IndexedDocument
				err = dec.Decode(&indexedDocs)
				if err != nil {
					return IndexedDocument{}, err
				}

				if v, ok := indexedDocs[docId]; ok {
					i.cachedDocuments = indexedDocs
					return v, nil
				}
			}
		}
	}

	return IndexedDocument{}, errors.New(fmt.Sprintf("Document with id %v does not exist.", docId))
}

func (i *InvertedIndex) GetSources(docIds int64arr) ([]IndexedDocument, error) {
	sort.Sort(docIds)
	docs := make([]IndexedDocument, len(docIds))
	for j, k := range docIds {
		d, err := i.Get(k)
		if err != nil {
			return make([]IndexedDocument, 0), err
		}
		docs[j] = d
	}
	return docs, nil
}

// createDumpDocsName is a helper function for creating names of files to dump
func (i *InvertedIndex) createDumpDocsName(docId int64) string {
	return i.Name + "/" + strconv.FormatInt(docId, 10) + ".wdocs"
}

// Dump an index to file. This is a one-to-one in-memory dump of the inverted index, plus the statistics.
func (i *InvertedIndex) Dump() error {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)

	err := enc.Encode(i)
	if err != nil {
		log.Panicln(err)
	}

	return ioutil.WriteFile(i.Name + ".weasel", buff.Bytes(), 0664)
}

// DumpDocs dumps the documents inside the inverted index at any given time and dumps them to a file. The resulting file
// contains a one-to-one mapping of a slice of Documents ([]IndexedDocument)
func (i *InvertedIndex) DumpDocs() error {
	// Leave this function if there is nothing to do
	if len(i.DocumentFiles) > 0 && i.NumDocs == i.DocumentFiles[len(i.DocumentFiles) - 1] {
		return nil
	}

	// Create the folder for documents in the index if if doesn't exist
	if _, err := os.Stat(i.Name + "/"); os.IsNotExist(err) {
		err := os.Mkdir(i.Name + "/", 0777)
		if err != nil {
			return err
		}
	}

	// Dump the documents in memory to disk and clear the documents in memory
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)

	err := enc.Encode(i.documents)
	if err != nil {
		log.Panicln(err)
	}

	i.documents = make(map[int64]IndexedDocument, 0)

	i.DocumentFiles = append(i.DocumentFiles, i.NumDocs)
	docName := i.createDumpDocsName(i.NumDocs)

	return ioutil.WriteFile(docName, buff.Bytes(), 0664)
}

// Load an index from a file.
func Load(filename string) (InvertedIndex, error) {
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

	return i, nil
}