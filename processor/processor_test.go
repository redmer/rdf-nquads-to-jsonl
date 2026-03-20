package processor_test

import (
	"encoding/json"
	"testing"

	"github.com/redmer/rdf-nquads-to-jsonl/parser"
	"github.com/redmer/rdf-nquads-to-jsonl/processor"
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

	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unexpected error unmarshaling marshaled document: %v", err)
	}

	if got["_id"] != "https://example.com/person/1" {
		t.Fatalf("expected _id to be %q, got: %v", "https://example.com/person/1", got["_id"])
	}

	if _, ok := got["ID"]; ok {
		t.Fatalf("did not expect key %q in marshaled output", "ID")
	}
	if _, ok := got["Fields"]; ok {
		t.Fatalf("did not expect key %q in marshaled output", "Fields")
	}

	nameRaw, ok := got["http://schema.org/name"]
	if !ok {
		t.Fatalf("expected top-level key %q in marshaled output", "http://schema.org/name")
	}
	nameVals, ok := nameRaw.([]any)
	if !ok || len(nameVals) != 1 || nameVals[0] != "Alice" {
		t.Fatalf("unexpected value for key %q: %v", "http://schema.org/name", nameRaw)
	}

	ageRaw, ok := got["http://schema.org/age"]
	if !ok {
		t.Fatalf("expected top-level key %q in marshaled output", "http://schema.org/age")
	}
	ageVals, ok := ageRaw.([]any)
	if !ok || len(ageVals) != 1 || ageVals[0] != "30" {
		t.Fatalf("unexpected value for key %q: %v", "http://schema.org/age", ageRaw)
	}
}

func TestGrouperWithGraph(t *testing.T) {
	quad1 := parser.Quad{
		Subject:   "https://example.com/s",
		Predicate: "p",
		Object:    "o1",
		Graph:     "https://example.com/g1",
	}
	quad2 := parser.Quad{
		Subject:   "https://example.com/s",
		Predicate: "p",
		Object:    "o2",
		Graph:     "https://example.com/g2",
	}
	quad3 := parser.Quad{
		Subject:   "https://example.com/s",
		Predicate: "p",
		Object:    "o3",
		Graph:     "https://example.com/g1", // Duplicate graph
	}

	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	g.Add(quad1)
	g.Add(quad2)
	g.Add(quad3)
	g.Flush()

	if len(docs) != 1 {
		t.Fatalf("docs count = %d, want 1", len(docs))
	}
	doc := docs[0]
	if len(doc.Graphs) != 2 {
		t.Errorf("len(doc.Graphs) = %d, want 2", len(doc.Graphs))
	}
	// Check content (order depends on insertion, which is predictable here: g1, then g2)
	if doc.Graphs[0] != "https://example.com/g1" || doc.Graphs[1] != "https://example.com/g2" {
		t.Errorf("Graphs = %v, want [g1, g2]", doc.Graphs)
	}

	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	graphs, ok := m["_graph"].([]interface{})
	if !ok {
		t.Errorf("_graph missing or not array: %v", m)
	}
	if len(graphs) != 2 {
		t.Errorf("len(_graph) = %d, want 2", len(graphs))
	}
}
