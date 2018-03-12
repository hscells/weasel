package weasel

import (
	"github.com/surgebase/porter2"
	"strings"
	"encoding/gob"
)

var (
	// Porter2Stemmer is the implementation at https://github.com/dataence/porter2
	Porter2Stemmer = porter2Stemmer{}
	// LowercaseFilter transforms text to lowercase.
	LowercaseFilter = lowercaseFilter{}
	// StopEn is an english stopwords filter using the default Elasticsearch stop word list.
	StopEn = stopEn{}

	// English stopwords
	stopwordsEn = []string{"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
		"it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this",
		"to", "was", "will", "with"}
)

// Analyser applies some function to text at index time.
type Analyser interface {
	Analyse(text string) (string, error)
}

type porter2Stemmer struct {
}

type lowercaseFilter struct {
}

type stopEn struct {
}

func (stopEn) Analyse(text string) (string, error) {
	for _, word := range stopwordsEn {
		if word == text {
			return "", nil
		}
	}
	return text, nil
}

func (lowercaseFilter) Analyse(text string) (string, error) {
	return strings.ToLower(text), nil
}

func (porter2Stemmer) Analyse(text string) (string, error) {
	return porter2.Stem(text), nil
}

func RegisterAnalysers() {
	gob.Register(Porter2Stemmer)
	gob.Register(LowercaseFilter)
	gob.Register(StopEn)
}
