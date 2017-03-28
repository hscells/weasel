package query

import (
	"github.com/hscells/weasel/index"
	"log"
)

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

// intersect is a fast set intersection function that operates on multiple vectors and sorts the results.
func intersect(vecs [][]int64) []int64 {
	vecLen := len(vecs)
	docIds := distinct(vecs)

	log.Println(vecLen)

	docIdsIntersection := make([]int64, 0)
	for k, v := range docIds {
		if v == vecLen {
			docIdsIntersection = append(docIdsIntersection, k)
		}
	}
	return docIdsIntersection
}

// distinct is a fast set distinct function that operates on multiple vectors at once.
func distinct(vecs [][]int64) map[int64]int {
	docIds := make(map[int64]int)
	for _, i := range vecs {
		for _, v := range i {
			if _, ok := docIds[v]; ok {
				docIds[v]++
			} else {
				docIds[v] = 1
			}
		}
	}
	return docIds
}

// Query is an implementation of a boolean query.
func (b *BooleanQuery) Query(i index.InvertedIndex) ([]int64, error) {
	docs := make([]int64, 0)

	// First, get the docIds that correspond to each query term
	docIds := make([][]int64, len(b.QueryTerms))
	for j, t := range b.QueryTerms {
		docIds[j] = i.InvertedIndex[b.Field][i.TermMapping[t]]
	}

	// Secondly, filter based on operator
	if b.Operator == Or {
		// OR only requires a distinct set of documents
		distinctDocIds := distinct(docIds)
		for k := range distinctDocIds {
			docs = append(docs, k)
		}

		if len(b.Children) > 0 {
			// Recursively walk the tree to query the rest of the set
			for _, c := range b.Children {
				r, err := c.Query(i)
				if err != nil {
					return make([]int64, 0), err
				}
				docs = append(docs, r...)
			}
		}

	} else if b.Operator == And {
		// AND requires that all query terms retrieve the same documents
		intersectedDocIds := intersect(docIds)

		if len(b.Children) > 0 {
			andDocs := make([][]int64, 1 + len(b.Children))

			// Recursively walk the tree to retrieve the results of the nested boolean queries
			for j, c := range b.Children {
				childDocs, err := c.Query(i)
				if err != nil {
					return make([]int64, 0), err
				}

				// The AND cannot be matched because a subquery did not return any docs
				if len(childDocs) > 0 {
					andDocs[j] = childDocs
				} else {
					return make([]int64, 0), nil
				}
			}

			andDocs[len(andDocs) - 1] = intersectedDocIds

			// The intersection of the current layer, plus all the results of the children
			docs = append(docs, intersect(andDocs)...)
		} else {
			// Otherwise, there are no children, so append all at once
			docs = append(docs, intersectedDocIds...)
		}
	}

	return docs, nil
}