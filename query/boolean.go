package query

import (
	"github.com/hscells/weasel/index"
	"log"
	"sort"
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

type booleanOperator struct {
	name string
}

var And booleanOperator = booleanOperator{name: "and"}
var Or booleanOperator = booleanOperator{name: "or"}
var Not booleanOperator = booleanOperator{name: "not"}

// BooleanQuery is the representation of a boolean query in prefix notation -> (AND term term (OR term term)). This is
// an easy query to parse and nesting is easy to calculate.
type BooleanQuery struct {
	Operator   booleanOperator
	Field      string
	QueryTerms []string
	Children   []BooleanQuery
}

// Query is an implementation of a boolean query.
//
// TODO nested boolean queries don't work as expected and need fixing, NOT also needs implementing, but POC is there
func (b *BooleanQuery) Query(i index.InvertedIndex) []RetrievedDocument {
	docs := make([]RetrievedDocument, 0)

	if b.Operator == Or {
		// Handle the OR case

		// docId->docSource
		d := make(map[int64]RetrievedDocument)
		for _, t := range b.QueryTerms {
			docIds := i.InvertedIndex[b.Field][i.TermMapping[t]]
			for _, docId := range docIds {
				source, err := i.Get(docId)
				if err != nil {

				}
				d[docId] = NewRetrievedDocument(source)
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

		log.Println("got list of docs")

		// Get a list of doc ids
		var docIds int64arr
		for k, v := range d {
			if v == len(b.QueryTerms) {
				docIds = append(docIds, k)
			}
		}

		log.Println("sorting docs")

		// now get the docs
		sort.Sort(docIds)

		log.Println("actually getting documents now")
		for _, k := range docIds {
			v, err := i.Get(k)
			if err != nil {

			}
			docs = append(docs, NewRetrievedDocument(v))
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