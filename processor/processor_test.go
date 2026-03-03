package processor_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/redmer/rdf-index-elasticsearch/parser"
	"github.com/redmer/rdf-index-elasticsearch/processor"
)

func TestGrouper(t *testing.T) {
	quads := []parser.Quad{
		{Subject: "https://example.com/person/1", Predicate: "http://schema.org/name", Object: "Alice"},
		{Subject: "https://example.com/person/1", Predicate: "http://schema.org/age", Object: "30"},
		{Subject: "https://example.com/person/1", Predicate: "http://schema.org/knows", Object: "https://example.com/person/2"},
		{Subject: "https://example.com/person/2", Predicate: "http://schema.org/name", Object: "Bob"},
		{Subject: "https://example.com/person/2", Predicate: "http://schema.org/name", Object: "Robert"},
	}

	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	for _, q := range quads {
		g.Add(q)
	}
	g.Flush()

	if len(docs) != 2 {
		t.Fatalf("expected 2 documents, got %d", len(docs))
	}

	// First document: person/1
	doc1 := docs[0]
	if doc1.ID != "https://example.com/person/1" {
		t.Errorf("doc1 ID = %q, want %q", doc1.ID, "https://example.com/person/1")
	}
	if vals, ok := doc1.Fields["http://schema org/name"]; !ok || len(vals) != 1 || vals[0] != "Alice" {
		t.Errorf("doc1 name = %v, want [Alice]", doc1.Fields["http://schema org/name"])
	}
	if vals, ok := doc1.Fields["http://schema org/age"]; !ok || len(vals) != 1 || vals[0] != "30" {
		t.Errorf("doc1 age = %v, want [30]", doc1.Fields["http://schema org/age"])
	}
	if vals, ok := doc1.Fields["http://schema org/knows"]; !ok || len(vals) != 1 || vals[0] != "https://example.com/person/2" {
		t.Errorf("doc1 knows = %v", doc1.Fields["http://schema org/knows"])
	}

	// Second document: person/2 with two names
	doc2 := docs[1]
	if doc2.ID != "https://example.com/person/2" {
		t.Errorf("doc2 ID = %q, want %q", doc2.ID, "https://example.com/person/2")
	}
	if vals, ok := doc2.Fields["http://schema org/name"]; !ok || len(vals) != 2 {
		t.Errorf("doc2 name = %v, want [Bob Robert]", doc2.Fields["http://schema org/name"])
	}
}

func TestGrouperDotReplacement(t *testing.T) {
	quads := []parser.Quad{
		{Subject: "https://example.com/s", Predicate: "http://schema.org/name", Object: "Test"},
	}
	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	for _, q := range quads {
		g.Add(q)
	}
	g.Flush()

	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	if _, ok := docs[0].Fields["http://schema org/name"]; !ok {
		t.Errorf("expected key %q, got fields: %v", "http://schema org/name", docs[0].Fields)
	}
}

func TestGrouperEmptyFlush(t *testing.T) {
	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	g.Flush()
	if len(docs) != 0 {
		t.Errorf("expected 0 documents on empty flush, got %d", len(docs))
	}
}

func TestDocumentMarshalJSON(t *testing.T) {
	doc := processor.Document{
		ID: "https://example.com/person/1",
		Fields: map[string][]any{
			"http://schema.org/name": {"Alice"},
			"http://schema.org/age":  {"30"},
		},
	}

	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("unexpected error marshaling document: %v", err)
	}

	got := string(b)
	wantID := `"_id":"https://example.com/person/1"`
	if !strings.Contains(got, wantID) {
		t.Errorf("expected ID field %q, got: %s", wantID, got)
	}
	wantName := `"http://schema.org/name":["Alice"]`
	if !strings.Contains(got, wantName) {
		t.Errorf("expected name field %q, got: %s", wantName, got)
	}
}
