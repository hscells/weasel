package query

import (
	"github.com/hscells/weasel/index"
	"github.com/hscells/weasel/document"
)

type booleanOperator struct {
	name string
}

var And booleanOperator = booleanOperator{name: "and"}
var Or booleanOperator = booleanOperator{name: "or"}
var Not booleanOperator = booleanOperator{name: "not"}

type BooleanQuery struct {
	Operator   booleanOperator
	Field      string
	QueryTerms []string
	Children   []BooleanQuery
}

// Query is an implementation of a boolean query
// TODO nested boolean queries don't work as expected and need fixing, NOT also needs implementing, but POC is there
func (b *BooleanQuery) Query(i index.InvertedIndex) []document.Document {
	docs := make([]document.Document, 0)

	if b.Operator == Or {
		// Handle the OR case

		// docId->docSource
		d := make(map[int64]document.Document)
		for _, t := range b.QueryTerms {
			docIds := i.InvertedIndex[b.Field][i.TermMapping[t]]
			for _, docId := range docIds {
				d[docId] = document.From(i.Documents[docId])
			}
		}

		// Now group the docs
		for _, v := range d {
			docs = append(docs, v)
		}
	} else if b.Operator == And {
		// Handle the AND case

		// docId->len(queryTerm)
		d := make(map[int64]int)
		for _, t := range b.QueryTerms {
			docIds := i.InvertedIndex[b.Field][i.TermMapping[t]]
			for _, docId := range docIds {
				if _, ok := d[docId]; ok {
					d[docId] += 1
				} else {
					d[docId] = 1
				}
			}
		}

		// Now group the docs
		for k, v := range d {
			if v == len(b.QueryTerms) {
				docs = append(docs, document.From(i.Documents[k]))
			}
		}
	}

	if len(b.Children) > 0 {
		// Recursively walk the tree to query the rest of the set (this isn't how this boolean queries work)
		for _, c := range b.Children {
			docs = append(docs, c.Query(i)...)
		}
	}

	return docs
}