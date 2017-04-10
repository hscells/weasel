package api

import (
	"github.com/hscells/weasel/query"
	"strings"
)

type QueryWrapper interface {
	Convert() query.Query
}

type BooleanQueryWrapper struct {
	Operator   string                 `json:"operator"`
	Field      string                 `json:"field"`
	QueryTerms []string               `json:"query_terms"`
	Children   []BooleanQueryWrapper  `json:"children"`
}

func (q BooleanQueryWrapper) Convert() query.Query {
	b := query.BooleanQuery{}
	op := strings.ToLower(q.Operator)
	switch op {
	case "and":
		b.Operator = query.And
	case "or":
		b.Operator = query.Or
	default:
		b.Operator = query.And
	}

	b.Field = q.Field
	b.QueryTerms = q.QueryTerms

	for i := range q.Children {
		b.Children[i] = q.Children[i].Convert().(query.BooleanQuery)
	}

	return b
}