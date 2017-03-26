package query

import (
	"github.com/hscells/weasel/index"
	"github.com/hscells/weasel/document"
)

type Query interface {
	Query(index.InvertedIndex) []document.Document
}
