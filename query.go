package weasel

import (
	"errors"
	"fmt"
	"github.com/hscells/cqr"
	"hash/fnv"
	"strings"
)

// Documents is a slice of document ids.
type Documents []string

var (
	// OrOperator combines documents using `OR`.
	OrOperator = orOperator{}
	// AndOperator combines documents using `AND`.
	AndOperator = andOperator{}
	// NotOperator combines documents using `NOT`.
	NotOperator = notOperator{}
)

// Operator can combine different nodes of a tree together.
type Operator interface {
	Combine(clauses []LogicalTreeNode) Documents
	String() string
}

// LogicalTree can compute the number of documents retrieved for atomic components.
type LogicalTree struct {
	Root LogicalTreeNode
}

// LogicalTreeNode is a node in a logical tree.
type LogicalTreeNode interface {
	Query() cqr.CommonQueryRepresentation
}

// Clause is the most basic component of a logical tree.
type Clause struct {
	Hash      uint64
	Documents Documents
	Query     cqr.CommonQueryRepresentation
}

// Combinator is an operator in a query.
type Combinator struct {
	Operator
	Clause
	Clauses []LogicalTreeNode
}

// Atom is the smallest possible component of a query.
type Atom struct {
	Clause
}

// AdjAtom is a special type of atom for adjacent queries.
type AdjAtom struct {
	Clause
}

// andOperator is the intersection of documents.
type andOperator struct {
}

// orOperator is the union of documents.
type orOperator struct {
}

// notOperator is the relative compliment of documents.
type notOperator struct {
}

// Set creates a set from a slice of documents.
func (d Documents) Set() map[string]bool {
	m := make(map[string]bool)
	for _, doc := range d {
		m[doc] = true
	}
	return m
}

func (andOperator) Combine(nodes []LogicalTreeNode) Documents {
	nodeDocs := make([]map[string]bool, len(nodes))
	for i, node := range nodes {
		switch n := node.(type) {
		case Atom:
			nodeDocs[i] = n.Documents.Set()
		case AdjAtom:
			nodeDocs[i] = n.Documents.Set()
		case Combinator:
			nodeDocs[i] = n.Documents.Set()
		}
	}

	intersection := make(map[string]bool)
	for i := 0; i < len(nodeDocs)-1; i++ {
		for k := range nodeDocs[i] {
			if nodeDocs[i+1][k] {
				intersection[k] = true
			}
		}
	}

	docs := make(Documents, len(intersection))
	i := 0
	for doc := range intersection {
		docs[i] = doc
		i++
	}

	return docs
}

func (andOperator) String() string {
	return "and"
}

func (orOperator) Combine(nodes []LogicalTreeNode) Documents {
	union := make(map[string]bool)
	for _, node := range nodes {
		switch n := node.(type) {
		case Atom:
			for _, doc := range n.Documents {
				union[doc] = true
			}
		case AdjAtom:
			for _, doc := range n.Documents {
				union[doc] = true
			}
		case Combinator:
			for _, doc := range n.Documents {
				union[doc] = true
			}
		}
	}

	var docs Documents
	for k := range union {
		docs = append(docs, k)
	}

	return docs
}

func (orOperator) String() string {
	return "or"
}

func (notOperator) Combine(nodes []LogicalTreeNode) Documents {
	var a Documents
	b := make([]map[string]bool, len(nodes))

	switch n := nodes[0].(type) {
	case Atom:
		a = append(a, n.Documents...)
	case AdjAtom:
		a = append(a, n.Documents...)
	case Combinator:
		a = append(a, n.Documents...)
	}

	for i := 1; i < len(nodes); i++ {
		switch n := nodes[i].(type) {
		case Atom:
			b[i] = n.Documents.Set()
		case AdjAtom:
			b[i] = n.Documents.Set()
		case Combinator:
			b[i] = n.Documents.Set()
		}
	}

	// Now make b prime, comprising the docs not in a.
	bP := make(map[string]bool)
	for i := 0; i < len(b); i++ {
		for k := range b[i] {
			bP[k] = true
		}
	}

	// Relative compliment.
	var docs Documents
	for _, doc := range a {
		if !bP[doc] {
			docs = append(docs, doc)
		}
	}

	return docs
}

func (notOperator) String() string {
	return "not"
}

// Query returns the underlying query of the combinator.
func (c Combinator) Query() cqr.CommonQueryRepresentation {
	return c.Clause.Query
}

// Query returns the underlying query of the atom.
func (a Atom) Query() cqr.CommonQueryRepresentation {
	return a.Clause.Query
}

// Query returns the underlying query of the adjacency operator.
func (a AdjAtom) Query() cqr.CommonQueryRepresentation {
	return a.Clause.Query
}

// String returns the string representation of the documents.
func (d Document) String() string {
	return fmt.Sprintf("%v", d)
}

// NewAtom creates a new atom.
func NewAtom(keyword cqr.Keyword, docs Documents) Atom {
	return Atom{
		Clause{
			Hash:      HashCQR(keyword),
			Documents: docs,
			Query:     keyword,
		},
	}
}

// NewAdjAtom creates a new adjacent atom.
func NewAdjAtom(query cqr.BooleanQuery, docs Documents) AdjAtom {
	return AdjAtom{
		Clause{
			Hash:      HashCQR(query),
			Documents: docs,
			Query:     query,
		},
	}
}

// NewCombinator creates a new combinator.
func NewCombinator(query cqr.BooleanQuery, operator Operator, clauses ...LogicalTreeNode) Combinator {
	docs := operator.Combine(clauses)
	return Combinator{
		Operator: operator,
		Clause: Clause{
			Hash:      HashCQR(query),
			Documents: docs,
			Query:     query,
		},
		Clauses: clauses,
	}
}

// HashCQR creates a hash of the query.
func HashCQR(representation cqr.CommonQueryRepresentation) uint64 {
	h := fnv.New64a()
	h.Write([]byte(representation.String()))
	return h.Sum64()
}

// Execute creates a logical tree recursively by descending top down. If the operator of the query is unknown
// (i.e. it is not one of `or`, `and`, `not`, or an `adj` operator) the default operator will be `or`.
//
// Note that once one tree has been constructed, the returned map can be used to save processing.
func constructTree(query cqr.CommonQueryRepresentation, index InvertedIndex, seen map[uint64]LogicalTreeNode) (LogicalTreeNode, map[uint64]LogicalTreeNode, error) {
	if seen == nil {
		seen = make(map[uint64]LogicalTreeNode)
	}
	switch q := query.(type) {
	case cqr.Keyword:
		// Return a seen atom.
		if atom, ok := seen[HashCQR(q)]; ok {
			return atom, seen, nil
		}

		var docs []string
		terms := strings.Split(q.QueryString, " ")
		for _, field := range q.Fields {
			for _, term := range terms {
				fmt.Println(field, term)
				// Otherwise, get the docs for this atom.
				if results, ok := index.Posting[field]; ok {
					if term, ok := index.TermStatistics[term]; ok {
						for _, docID := range results[TermID(term.ID)] {
							docs = append(docs, index.DocumentMapping[docID])
						}
					}
				}
			}
		}
		// Create the new atom add it to the seen list.
		a := NewAtom(q, docs)
		seen[a.Hash] = a
		return a, seen, nil
	case cqr.BooleanQuery:
		var operator Operator
		switch strings.ToLower(q.Operator) {
		case "or":
			operator = OrOperator
		case "and":
			operator = AndOperator
		case "not":
			operator = NotOperator
		default:
			operator = OrOperator
		}
		// 10,000 = 100MB
		// 100,000 = 1000MB
		// TODO We need to create a special case for adjacent clauses.

		// Otherwise, we can just perform the operation with a typical operator.
		clauses := make([]LogicalTreeNode, len(q.Children))
		for i, child := range q.Children {
			var err error
			clauses[i], seen, err = constructTree(child, index, seen)
			if err != nil {
				return nil, seen, err
			}
		}
		c := NewCombinator(q, operator, clauses...)
		return c, seen, nil
	}
	return nil, nil, errors.New("supplied query is not supported")
}

// Execute creates a new logical tree which serves the purpose of retrieving documents.  If the operator of the query
// is unknown (i.e. it is not one of `or`, `and`, `not`, or an `adj` operator) the default operator will be `or`.
//
// Note that once one tree has been constructed, the returned map can be used to save processing.
func (index InvertedIndex) Execute(query cqr.CommonQueryRepresentation, seen map[uint64]LogicalTreeNode) (LogicalTree, map[uint64]LogicalTreeNode, error) {
	if seen == nil {
		seen = make(map[uint64]LogicalTreeNode)
	}
	root, seen, err := constructTree(query, index, seen)
	if err != nil {
		return LogicalTree{}, nil, nil
	}
	return LogicalTree{
		Root: root,
	}, seen, nil
}

// Documents returns the documents that the tree (query) would return if executed.
func (root LogicalTree) Documents() Documents {
	switch c := root.Root.(type) {
	case Atom:
		return c.Documents
	case AdjAtom:
		return c.Documents
	case Combinator:
		return c.Combine(c.Clauses)
	}
	return nil
}

// ToCQR creates a query backwards from a logical tree.
func (root LogicalTree) ToCQR() cqr.CommonQueryRepresentation {
	switch c := root.Root.(type) {
	case Atom:
		return c.Query()
	case AdjAtom:
		return c.Query()
	case Combinator:
		return c.Query()
	}
	return nil
}
