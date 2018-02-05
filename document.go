package weasel

import (
	"github.com/pkg/errors"
	"time"
)

// Document is a container for a document
type Document struct {
	ID     string
	source map[string]interface{}
}

// NewDocument constructs a new document
func NewDocument(ID string) Document {
	return Document{
		ID:     ID,
		source: make(map[string]interface{}),
	}
}

// From marshals a document from a map, checking for type consistency
func From(source map[string]interface{}, ID string) Document {
	d := NewDocument(ID)
	for k, v := range source {
		d.Set(k, v)
	}
	return d
}

// Set adds fields and values to the document. Set is the only way new fields can be added to a document other than
// creating one using From. This function will override existing values if they exist.
func (d *Document) Set(field string, value interface{}) error {
	// Check the value is of a type that can be indexed
	switch value.(type) {
	case int64, int, float64, float32, time.Time, string:
		d.source[field] = value
	default:
		return errors.New("value is not able to be indexed")
	}
	return nil
}

// Get a single field from the document
func (d *Document) Get(field string) interface{} {
	return d.source[field]
}

// Source a document (the raw go map representation)
func (d *Document) Source() map[string]interface{} {
	return d.source
}
