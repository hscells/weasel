// Package query contains functions and structures relating to the query DSL.
package query

import (
	"github.com/hscells/weasel/index"
	"github.com/hscells/weasel/document"
)

type RetrievedDocument struct {
	source map[string]interface{}
}

type Query interface {
	Query(index.InvertedIndex) []document.Document
}

func NewRetrievedDocument(source map[string]interface{}) RetrievedDocument {
	return RetrievedDocument{source: source}
}