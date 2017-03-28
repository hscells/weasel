// Package document is a container for documents stored in the inverted index. A document has a strict typing scheme
// that is enforced by the document mapping in the inverted index.
package document

import (
	"time"
	"github.com/pkg/errors"
)

// Document is a container for a document
type Document struct {
	id     interface{}
	source map[string]interface{}
}

// New constructs a new document
func New() Document {
	return Document{
		source: make(map[string]interface{}),
	}
}

// From marshals a document from a map, checking for type consistency
func From(source map[string]interface{}) Document {
	d := New()
	for k, v := range source {
		if k == "id" {
			d.id = v
		} else {
			d.Set(k, v)
		}
	}
	return d
}

// Set adds fields and values to the document. Set is the only way new fields can be added to a document other than
// creating one using From. This function will override existing values if they exist.
func (d *Document) Set(field string, value interface{}) error {
	// The id may be specified manually
	if field == "id" {
		if v, ok := value.(int64); ok {
			d.id = v
			return nil
		}
		return errors.New("Id value must be of type int64")
	}

	// Check the value is of a type that can be indexed
	switch value.(type) {
	case int64, int, float64, float32, time.Time, string:
		d.source[field] = value
	default:
		return errors.New("Value is not able to be indexed.")
	}
	return nil
}

// Get a single field from the document
func (d *Document) Get(field string) interface{} {
	// The id may be specified manually
	if field == "id" {
		return d.id
	}
	return d.source[field]
}

// Source a document (the raw go map representation)
func (d *Document) Source() map[string]interface{} {
	return d.source
}