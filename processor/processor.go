// Package processor groups consecutive N-Quad triples by Subject into documents.
package processor

import (
	"encoding/json"
	"strings"

	"github.com/redmer/rdf-index-elasticsearch/parser"
)

// Document represents a single Elasticsearch document ready for indexing.
type Document struct {
	// ID is the Subject URI used as the Elasticsearch document _id.
	ID string
	// Fields maps predicate keys (dots replaced by spaces) to arrays of object values.
	Fields map[string][]interface{}
}

// MarshalJSON flattens Fields into top-level keys and emits ID as "_id".
func (d Document) MarshalJSON() ([]byte, error) {
	out := make(map[string]interface{}, len(d.Fields)+1)
	out["_id"] = d.ID
	for k, v := range d.Fields {
		out[k] = v
	}
	return json.Marshal(out)
}

// EmitFunc is called whenever a complete document has been accumulated.
type EmitFunc func(doc Document)

// Grouper accumulates consecutive triples with the same Subject and emits a Document
// whenever the Subject changes or Flush is called.
type Grouper struct {
	emit    EmitFunc
	current *Document
}

// NewGrouper creates a new Grouper that calls emit for each completed document.
func NewGrouper(emit EmitFunc) *Grouper {
	return &Grouper{emit: emit}
}

// Add processes a single parsed quad. If the Subject differs from the current document's
// Subject, the current document is emitted and a new one is started.
func (g *Grouper) Add(q parser.Quad) {
	if g.current == nil || g.current.ID != q.Subject {
		if g.current != nil {
			g.emit(*g.current)
		}
		g.current = &Document{
			ID:     q.Subject,
			Fields: make(map[string][]interface{}),
		}
	}
	key := strings.ReplaceAll(q.Predicate, ".", " ")
	g.current.Fields[key] = append(g.current.Fields[key], q.Object)
}

// Flush emits any remaining buffered document. Call after the input stream is exhausted.
func (g *Grouper) Flush() {
	if g.current != nil {
		g.emit(*g.current)
		g.current = nil
	}
}
