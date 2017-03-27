// package index implements an inverted index used to store documents from package document.
package index

import (
	"github.com/hscells/weasel/document"
	"github.com/pkg/errors"
	"reflect"
	"fmt"
	"strings"
	"regexp"
	"encoding/gob"
	"bytes"
	"log"
	"io/ioutil"
	"strconv"
	"os"
)

// punctuationRegex is the regex used to remove punctuation from fields in documents submitted to the index
var punctuationRegex *regexp.Regexp = regexp.MustCompile("[.,#!$?%^&*;:{}+|\\<>=_`~()/-]")

// spaceRegex finds multiple spaces
var spaceRegex *regexp.Regexp = regexp.MustCompile("[ \n\r]+")

// Number of documents to bulk index before dumping. It seems like it's fairly okay to keep this number high
var dumpNum int64 = 50000

// IndexedDocument is a document that gets returned if the index is asked for a document
type IndexedDocument struct {
	Source map[string]interface{}
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

	// Mapping is a map of term id to document id
	// field->termId->docId
	InvertedIndex   map[string]map[int64][]int64

	// Mapping of terms to the index in the term vector
	// term->position
	TermMapping     map[string]int64

	// Term frequency (number of terms)
	// termId->TF
	TermFrequency   map[int64]int64

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
		InvertedIndex: make(map[string]map[int64][]int64),
		DocumentMapping: mapping,
		TermMapping: make(map[string]int64),
		TermFrequency: make(map[int64]int64),
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

// extractTerms normalises and splits fields into a slice of strings
func extractTerms(value interface{}) []string {
	switch v := value.(type) {
	case string :{
		s := strings.ToLower(v)
		s = punctuationRegex.ReplaceAllString(s, "")
		s = spaceRegex.ReplaceAllString(s, " ")
		return strings.Split(s, " ")
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

	docId := i.NumDocs

	// check the types of the mapping match that of the source
	for k := range d.Source() {
		// type-check the document to the mapping in the index
		dType := i.DocumentMapping[k]
		v := d.Source()[k]
		vType := reflect.TypeOf(v).Kind()
		if !(vType == dType) {
			return errors.New(fmt.Sprintf("Incorrect type for %v. Expecting %v, got %v.", k, dType, vType))
		}

		// Add new terms to the term mapping
		terms := extractTerms(v)
		for _, t := range terms {
			// Create a position of the term in the posting if one does not exist
			if _, ok := i.TermMapping[t]; !ok {
				i.TermMapping[t] = int64(len(i.TermMapping))
			}

			// Recalculate term frequency
			if _, ok := i.TermFrequency[i.TermMapping[t]]; ok {
				i.TermFrequency[i.TermMapping[t]]++
			} else {
				i.TermFrequency[i.TermMapping[t]] = 1
			}

		}

		// add the document to the inverted index
		for _, j := range termVector(terms, i.TermMapping) {
			if _, ok := i.InvertedIndex[k]; !ok {
				i.InvertedIndex[k] = make(map[int64][]int64)
			}
			i.InvertedIndex[k][j] = append(i.InvertedIndex[k][j], docId)
		}
	}

	d.Set("id", docId)
	i.documents[docId] = IndexedDocument{Source: d.Source()}
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
func (i *InvertedIndex) Get(docId int64) (map[string]interface{}, error) {
	if v, ok := i.documents[docId]; ok {
		return v.Source, nil
	} else if v, ok := i.cachedDocuments[docId]; ok {
		return v.Source, nil
	} else {
		// Try to read all the dumped files, searching for the doc
		for _, docFile := range i.DocumentFiles {
			data, err := ioutil.ReadFile(i.createDumpDocsName(docFile))
			if err != nil {
				return make(map[string]interface{}), err
			}

			dec := gob.NewDecoder(bytes.NewReader(data))
			var indexedDocs map[int64]IndexedDocument
			err = dec.Decode(&indexedDocs)
			if err != nil {
				return make(map[string]interface{}), err
			}

			if v, ok := indexedDocs[docId]; ok {
				i.cachedDocuments = indexedDocs
				return v.Source, nil
			}
		}
	}

	return make(map[string]interface{}), errors.New(fmt.Sprintf("Document with id %v does not exist.", docId))
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

	i.documents = make(map[int64]IndexedDocument)

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