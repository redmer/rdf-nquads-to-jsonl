// Package processor groups consecutive N-Quad triples by Subject into documents.
package processor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/redmer/rdf-nquads-to-jsonl/parser"
)

// DefaultMaxIDBytes is the default maximum length of a document _id in bytes.
// Elasticsearch rejects _id values longer than 512 bytes.
const DefaultMaxIDBytes = 512

// protectID returns id unchanged when it fits within maxBytes. Otherwise it
// returns a deterministic, collision-resistant value that fits: a truncated
// prefix of the id followed by "-" and the hex SHA-256 of the full id, kept
// within the byte budget. maxBytes <= 0 means no limit.
func protectID(id string, maxBytes int) string {
	if maxBytes <= 0 || len(id) <= maxBytes {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	digest := hex.EncodeToString(sum[:])
	// suffix = "-" + digest
	suffixLen := 1 + len(digest)
	if maxBytes <= suffixLen {
		// Not enough room for a prefix; use the digest alone (truncated if needed).
		if maxBytes >= len(digest) {
			return digest
		}
		return digest[:maxBytes]
	}
	prefixLen := maxBytes - suffixLen
	return id[:prefixLen] + "-" + digest
}

// Document represents a single Elasticsearch document ready for indexing.
type Document struct {
	// ID is the Subject URI used as the Elasticsearch document _id.
	ID string
	// Graphs is the list of graph URIs used as the Elasticsearch document _graph.
	Graphs []string
	// Fields maps predicate keys (dots replaced by spaces) to arrays of object values.
	Fields map[string][]interface{}
}

// MarshalJSON flattens Fields into top-level keys and emits ID as "_id".
func (d Document) MarshalJSON() ([]byte, error) {
	out := make(map[string]interface{}, len(d.Fields)+2)
	out["_id"] = d.ID
	if len(d.Graphs) > 0 {
		out["_graph"] = d.Graphs
	}
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
	// currentSubject is the raw Subject of the document being accumulated.
	// Kept separate from current.ID because the ID may be shortened (see protectID).
	currentSubject string
	// maxIDBytes limits the byte length of emitted document IDs (the _id).
	// Values <= 0 mean no limit. See DefaultMaxIDBytes.
	maxIDBytes int
}

// NewGrouper creates a new Grouper that calls emit for each completed document.
// Emitted document IDs are limited to DefaultMaxIDBytes bytes.
func NewGrouper(emit EmitFunc) *Grouper {
	return &Grouper{emit: emit, maxIDBytes: DefaultMaxIDBytes}
}

// SetMaxIDBytes configures the maximum byte length of emitted document IDs.
// A value <= 0 disables the limit.
func (g *Grouper) SetMaxIDBytes(maxBytes int) {
	g.maxIDBytes = maxBytes
}

// Add processes a single parsed quad. If the Subject differs from the current document's
// Subject, the current document is emitted and a new one is started.
func (g *Grouper) Add(q parser.Quad) {
	if g.current == nil || g.currentSubject != q.Subject {
		if g.current != nil {
			g.emit(*g.current)
		}
		g.currentSubject = q.Subject
		g.current = &Document{
			ID:     protectID(q.Subject, g.maxIDBytes),
			Graphs: []string{},
			Fields: make(map[string][]interface{}),
		}
	}

	if q.Graph != "" {
		found := false
		for _, graph := range g.current.Graphs {
			if graph == q.Graph {
				found = true
				break
			}
		}
		if !found {
			g.current.Graphs = append(g.current.Graphs, q.Graph)
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
		g.currentSubject = ""
	}
}
