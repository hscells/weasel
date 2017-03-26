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
)

// punctuationRegex is the regex used to remove punctuation from fields in documents submitted to the index
var punctuationRegex *regexp.Regexp = regexp.MustCompile("[.,#!$?%^&*;:{}+|\\<>=_`~()/-]")

// spaceRegex finds multiple spaces
var spaceRegex *regexp.Regexp = regexp.MustCompile("[ \n\r]+")

// InvertedIndex stores our document mapping
type InvertedIndex struct {
	// Name of the index
	Name            string

	// Ensure each field is the correct type
	// field->(type)
	DocumentMapping map[string]reflect.Kind

	// Mapping is a map of term id to document id
	// field->termId->docId
	InvertedIndex   map[string]map[int64][]int64

	// mapping of terms to the index in the term vector
	// term->position
	TermMapping     map[string]int64

	// Documents added to the index
	// docId->[]docSource
	Documents       map[int64]map[string]interface{}
}

// New is the constructor for the InvertedIndex. It takes a document mapping.
func New(name string, mapping map[string]reflect.Kind) InvertedIndex {
	return InvertedIndex{
		Name: name,
		InvertedIndex: make(map[string]map[int64][]int64),
		DocumentMapping: mapping,
		TermMapping: make(map[string]int64),
		Documents: make(map[int64]map[string]interface{}),
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

	docId := int64(len(i.Documents))

	// check the types of the mapping match that of the source
	for k := range d.Source() {
		// type-check the document to the mapping in the index
		dType := i.DocumentMapping[k]
		v := d.Source()[k]
		vType := reflect.TypeOf(v).Kind()
		if !(vType == dType) {
			return errors.New(fmt.Sprintf("Incorrect type for %v. Expecting %v, got %v.", k, dType, vType))
		}

		// add new terms to the term mapping
		terms := extractTerms(v)
		for _, t := range terms {
			if _, ok := i.TermMapping[t]; !ok {
				i.TermMapping[t] = int64(len(i.TermMapping))
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
	i.Documents[docId] = d.Source()

	return nil
}

// Get is a function that returns the source of a single document in the index
func (i *InvertedIndex) Get(docId int64) map[string]interface{} {
	return i.Documents[docId]
}

// Dump an index to file
func (i *InvertedIndex) Dump() error {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)

	err := enc.Encode(i)
	if err != nil {
		log.Panicln(err)
	}

	return ioutil.WriteFile(i.Name + ".weasle", buff.Bytes(), 0664)
}

// Load an index from a file
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