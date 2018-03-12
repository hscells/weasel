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
	Combine(clauses []logicalTreeNode) (Documents, error)
	String() string
}

// LogicalTree can compute the number of documents retrieved for atomic components.
type LogicalTree struct {
	Root logicalTreeNode
}

// logicalTreeNode is a node in a logical tree.
type logicalTreeNode interface {
	Query() cqr.CommonQueryRepresentation
}

// Clause is the most basic component of a logical tree.
type Clause struct {
	Hash      uint32
	Documents Documents
	Query     cqr.CommonQueryRepresentation
}

// Combinator is an operator in a query.
type Combinator struct {
	Operator
	Clause
	Clauses []logicalTreeNode
}

// Atom is the smallest possible component of a query.
type Atom struct {
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

type adjOperator struct {
	n       int32
	inOrder bool
	i       *InvertedIndex
}

func (adj adjOperator) Combine(clauses []logicalTreeNode) (Documents, error) {
	var adjDocIDs Documents
	docIDs, _ := AndOperator.Combine(clauses)

	var fields string

	for _, docID := range docIDs {
		if len(docID) == 0 {
			continue
		}

		doc, err := adj.i.Get(docID)
		if err != nil {
			return nil, err
		}

		type tuple struct {
			tPos int32
			dPos int32
		}

		docPositions := make(map[string][]tuple)
		for i, clause := range clauses {
			switch n := clause.(type) {
			case Atom:
				// Grab the fields for the query.
				q := n.Query().(cqr.Keyword)
				f := strings.Join(q.Fields, "")
				if len(fields) == 0 {
					fields = f
				} else if f != fields {
					return nil, errors.New("unable to combine different fields with adjacency")
				}

				// Get the positions for each field in the query.
				for _, field := range q.Fields {
					for _, k := range strings.Split(q.QueryString, " ") {
						for _, pos := range doc.Positions[field][adj.i.TermStatistics[k].ID] {
							docPositions[k] = append(docPositions[k], tuple{int32(i), pos})
						}
					}
				}
			case Combinator:
				return nil, errors.New("cannot combine nested fields with adjacency")
			}
		}

		// Determine the adjacency range for each of the terms.
		// Note that a term can appear multiple times in a document, thus the crazy 4 nested loop.
		for _, v := range docPositions {
			matches := 0
			for _, p := range docPositions {
				for _, i := range v {
					for _, j := range p {
						// We don't need to match on the same term.
						if i.dPos == j.dPos {
							continue
						}

						// If matching in order i cannot be larger than j.
						if adj.inOrder && i.dPos > j.dPos {
							continue
						}
						// Match positions in reverse (the way they should appear).
						if j.dPos-i.dPos <= adj.n && j.dPos-i.dPos > 0 {
							// A match is in order if both the terms and the positions in documents are the same.
							if adj.inOrder && i.tPos < j.tPos && i.dPos < j.dPos {
								matches++
							} else if !adj.inOrder { // Otherwise we match like normal.
								matches++
							}
						} else if i.dPos-j.dPos <= adj.n && i.dPos-j.dPos > 0 { // Match positions in real reverse.
							matches++
						}
					}
				}
			}
			// If the number of matches equals the number of clauses, this document can be added and the loop can stop.
			if matches >= len(clauses)-1 {
				adjDocIDs = append(adjDocIDs, doc.ID)
				break
			}
		}
	}

	return adjDocIDs, nil
}

func (adjOperator) String() string {
	return "adj"
}

// NewAdjOperator creates a new adjacency operator.
func NewAdjOperator(n int32, inOrder bool, i *InvertedIndex) adjOperator {
	return adjOperator{n, inOrder, i}
}

// Set creates a set from a slice of documents.
func (d Documents) Set() map[string]bool {
	m := make(map[string]bool)
	for _, doc := range d {
		m[doc] = true
	}
	return m
}

func (andOperator) Combine(nodes []logicalTreeNode) (Documents, error) {
	nodeDocs := make([]map[string]bool, len(nodes))
	for i, node := range nodes {
		switch n := node.(type) {
		case Atom:
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

	return docs, nil
}

func (andOperator) String() string {
	return "and"
}

func (orOperator) Combine(nodes []logicalTreeNode) (Documents, error) {
	union := make(map[string]bool)
	for _, node := range nodes {
		switch n := node.(type) {
		case Atom:
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

	return docs, nil
}

func (orOperator) String() string {
	return "or"
}

func (notOperator) Combine(nodes []logicalTreeNode) (Documents, error) {
	var a Documents
	b := make([]map[string]bool, len(nodes))

	switch n := nodes[0].(type) {
	case Atom:
		a = append(a, n.Documents...)
	case Combinator:
		a = append(a, n.Documents...)
	}

	for i := 1; i < len(nodes); i++ {
		switch n := nodes[i].(type) {
		case Atom:
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

	return docs, nil
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

// NewCombinator creates a new combinator.
func NewCombinator(query cqr.BooleanQuery, operator Operator, clauses ...logicalTreeNode) (Combinator, error) {
	docs, err := operator.Combine(clauses)
	if err != nil {
		return Combinator{}, err
	}
	return Combinator{
		Operator: operator,
		Clause: Clause{
			Hash:      HashCQR(query),
			Documents: docs,
			Query:     query,
		},
		Clauses: clauses,
	}, nil
}

// HashCQR creates a hash of the query.
func HashCQR(representation cqr.CommonQueryRepresentation) uint32 {
	h := fnv.New32a()
	h.Write([]byte(representation.String()))
	return h.Sum32()
}

// Execute creates a logical tree recursively by descending top down. If the operator of the query is unknown
// (i.e. it is not one of `or`, `and`, `not`, or an `adj` operator) the default operator will be `or`.
//
// Note that once one tree has been constructed, the returned map can be used to save processing.
func constructTree(query cqr.CommonQueryRepresentation, index InvertedIndex, seen map[uint32]logicalTreeNode) (logicalTreeNode, map[uint32]logicalTreeNode, error) {
	if seen == nil {
		seen = make(map[uint32]logicalTreeNode)
	}
	switch q := query.(type) {
	case cqr.Keyword:
		// Return a seen atom.
		if atom, ok := seen[HashCQR(q)]; ok {
			return atom, seen, nil
		}

		var docs []string

		q.QueryString = strings.Join(strings.Fields(q.QueryString), " ")

		// Perform a phase match.
		if strings.Contains(q.QueryString, " ") {
			terms := strings.Split(q.QueryString, " ")
			var atoms []logicalTreeNode
			for _, field := range q.Fields {
				for _, term := range terms {
					docIDs := index.DocumentIDs(field, term)
					d := make(Documents, len(docIDs))
					for _, docID := range docIDs {
						d = append(d, index.DocumentMapping[docID])
					}
					a := NewAtom(cqr.NewKeyword(term, field), d)
					seen[a.Hash] = a
					atoms = append(atoms, a)
				}
			}
			op := NewAdjOperator(1, true, &index)
			combined, err := op.Combine(atoms)
			if err != nil {
				return nil, seen, err
			}
			docs = append(docs, combined...)
		} else { // Just get the documents for a term.
			for _, field := range q.Fields {
				for _, docID := range index.DocumentIDs(field, q.QueryString) {
					docs = append(docs, index.DocumentMapping[docID])
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
		case "adj":
			var n float64
			var inOrder bool
			if distance, ok := q.Options["distance"].(float64); ok {
				n = distance
			}
			if o, ok := q.Options["in_order"].(bool); ok {
				inOrder = o
			}
			operator = NewAdjOperator(int32(n), inOrder, &index)
		default:
			operator = OrOperator
		}

		// Otherwise, we can just perform the operation with a typical operator.
		clauses := make([]logicalTreeNode, len(q.Children))
		for i, child := range q.Children {
			var err error
			clauses[i], seen, err = constructTree(child, index, seen)
			if err != nil {
				return nil, seen, err
			}
		}
		c, err := NewCombinator(q, operator, clauses...)
		return c, seen, err
	}
	return nil, nil, errors.New("supplied query is not supported")
}

// Execute creates a new logical tree which serves the purpose of retrieving documents.  If the operator of the query
// is unknown (i.e. it is not one of `or`, `and`, `not`, or an `adj` operator) the default operator will be `or`.
//
// Note that once one tree has been constructed, the returned map can be used to save processing.
func (index InvertedIndex) Execute(query cqr.CommonQueryRepresentation) (LogicalTree, map[uint32]logicalTreeNode, error) {
	seen := make(map[uint32]logicalTreeNode)
	root, seen, err := constructTree(query, index, seen)
	if err != nil {
		return LogicalTree{}, nil, err
	}
	return LogicalTree{
		Root: root,
	}, seen, nil
}

// Documents returns the documents that the tree (query) would return if executed.
func (root LogicalTree) Documents() (Documents, error) {
	switch c := root.Root.(type) {
	case Atom:
		return c.Documents, nil
	case Combinator:
		return c.Combine(c.Clauses)
	}
	return nil, nil
}

// ToCQR creates a query backwards from a logical tree.
func (root LogicalTree) ToCQR() cqr.CommonQueryRepresentation {
	switch c := root.Root.(type) {
	case Atom:
		return c.Query()
	case Combinator:
		return c.Query()
	}
	return nil
}
