package processor_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

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

func TestRDFJSONSurvivesDocumentEmission(t *testing.T) {
	quad, err := parser.ParseQuad(`<https://example.org/s> <https://example.org/data> "{\"nested\":{\"value\":1}}"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON> .`)
	if err != nil {
		t.Fatalf("ParseQuad failed: %v", err)
	}
	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	g.Add(quad)
	g.Flush()

	if len(docs) != 1 {
		t.Fatalf("documents = %d, want 1", len(docs))
	}
	b, err := json.Marshal(docs[0])
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	values, ok := got["https://example org/data"].([]interface{})
	if !ok || len(values) != 1 {
		t.Fatalf("data = %#v, want a one-element array", got["https://example org/data"])
	}
	nested, ok := values[0].(map[string]interface{})
	if !ok || nested["nested"] == nil {
		t.Fatalf("data value = %#v, want nested JSON object", values[0])
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

func TestGrouperLongSubjectID(t *testing.T) {
	longSubject := "http://example.org/" + strings.Repeat("a", 1000)
	quad := parser.Quad{Subject: longSubject, Predicate: "http://example.org/p", Object: "val"}

	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	g.Add(quad)
	g.Flush()

	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	id := docs[0].ID
	if len(id) > processor.DefaultMaxIDBytes {
		t.Errorf("ID length = %d bytes, want <= %d", len(id), processor.DefaultMaxIDBytes)
	}
	if !strings.HasPrefix(id, longSubject[:processor.DefaultMaxIDBytes-65]) {
		t.Errorf("ID should start with the truncated subject prefix, got %q...", id[:40])
	}
	// Deterministic: same input yields the same protected ID
	var docs2 []processor.Document
	g2 := processor.NewGrouper(func(doc processor.Document) { docs2 = append(docs2, doc) })
	g2.Add(quad)
	g2.Flush()
	if docs2[0].ID != id {
		t.Errorf("protectID not deterministic: %q vs %q", docs2[0].ID, id)
	}
}

func TestGrouperMaxIDBytesBelowMinimumClamped(t *testing.T) {
	// Positive limits below MinMaxIDBytes would truncate the SHA-256 digest
	// (and a negative prefix length would panic), so SetMaxIDBytes clamps
	// them up to MinMaxIDBytes.
	longSubject := "http://example.org/" + strings.Repeat("a", 1000)
	quad := parser.Quad{Subject: longSubject, Predicate: "http://example.org/p", Object: "val"}

	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	g.SetMaxIDBytes(1)
	g.Add(quad)
	g.Flush()

	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	id := docs[0].ID
	if len(id) > processor.MinMaxIDBytes {
		t.Errorf("ID length = %d bytes, want <= %d (clamped minimum)", len(id), processor.MinMaxIDBytes)
	}
	sum := sha256.Sum256([]byte(longSubject))
	wantDigest := hex.EncodeToString(sum[:])
	// The separator is "^", which cannot appear in an IRI.
	if !strings.HasSuffix(id, "^"+wantDigest) {
		t.Errorf("ID %q does not end with the separator + full SHA-256 digest of the subject", id[:20])
	}
}

// TestGrouperProtectedIDDisjointFromSubjects verifies that a protected _id
// always contains the "^" separator, which RFC 3987 excludes from IRIs. A
// shortened ID can therefore never equal an unmodified Subject, so long and
// short subjects cannot collide.
func TestGrouperProtectedIDDisjointFromSubjects(t *testing.T) {
	longSubject := "http://example.org/" + strings.Repeat("a", 1000)
	quad := parser.Quad{Subject: longSubject, Predicate: "http://example.org/p", Object: "val"}

	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	g.Add(quad)
	g.Flush()

	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	if !strings.Contains(docs[0].ID, "^") {
		t.Errorf("protected ID %q... lacks the separator that keeps it disjoint from real subjects", docs[0].ID[:40])
	}
}

func TestGrouperIDLimitDisabled(t *testing.T) {
	longSubject := "http://example.org/" + strings.Repeat("a", 1000)
	quad := parser.Quad{Subject: longSubject, Predicate: "http://example.org/p", Object: "val"}

	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	g.SetMaxIDBytes(0)
	g.Add(quad)
	g.Flush()

	if docs[0].ID != longSubject {
		t.Errorf("with limit disabled, ID = %q..., want the full subject", docs[0].ID[:40])
	}
}

func TestGrouperShortSubjectUnchanged(t *testing.T) {
	quad := parser.Quad{Subject: "http://example.org/s", Predicate: "http://example.org/p", Object: "val"}
	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) { docs = append(docs, doc) })
	g.Add(quad)
	g.Flush()
	if docs[0].ID != "http://example.org/s" {
		t.Errorf("short subject ID = %q, want unchanged", docs[0].ID)
	}
}

func TestGrouperLongSubjectIDUTF8Boundary(t *testing.T) {
	// A multibyte subject must be truncated at a rune boundary so the
	// marshaled _id stays valid UTF-8 and within the byte limit (a partial
	// rune would become U+FFFD under encoding/json and inflate the length).
	max := 100
	longSubject := "http://example.org/" + strings.Repeat("é", 200) // 'é' is 2 bytes in UTF-8
	quad := parser.Quad{Subject: longSubject, Predicate: "http://example.org/p", Object: "val"}

	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) {
		docs = append(docs, doc)
	})
	g.SetMaxIDBytes(max)
	g.Add(quad)
	g.Flush()

	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	id := docs[0].ID
	if len(id) > max {
		t.Errorf("ID length = %d bytes, want <= %d", len(id), max)
	}
	if !utf8.ValidString(id) {
		t.Errorf("ID is not valid UTF-8: %q", id)
	}
	b, err := json.Marshal(docs[0])
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got := m["_id"].(string); len(got) > max {
		t.Errorf("marshaled _id length = %d bytes, want <= %d", len(got), max)
	}
}

func TestGrouperLongSubjectsStillGrouped(t *testing.T) {
	// Two long subjects that share a long common prefix but differ at the end
	// must still be emitted as two separate documents.
	subj1 := "http://example.org/" + strings.Repeat("a", 1000) + "1"
	subj2 := "http://example.org/" + strings.Repeat("a", 1000) + "2"
	quads := []parser.Quad{
		{Subject: subj1, Predicate: "p", Object: "o1"},
		{Subject: subj2, Predicate: "p", Object: "o2"},
	}
	var docs []processor.Document
	g := processor.NewGrouper(func(doc processor.Document) { docs = append(docs, doc) })
	for _, q := range quads {
		g.Add(q)
	}
	g.Flush()
	if len(docs) != 2 {
		t.Fatalf("expected 2 documents, got %d", len(docs))
	}
	if docs[0].ID == docs[1].ID {
		t.Errorf("distinct subjects collapsed to the same _id %q", docs[0].ID)
	}
}
