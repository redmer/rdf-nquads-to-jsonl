package mapping_test

import (
	"encoding/json"
	"testing"

	"github.com/redmer/rdf-nquads-to-jsonl/mapping"
	"github.com/redmer/rdf-nquads-to-jsonl/parser"
)

func TestMapper_Generate(t *testing.T) {
	m := mapping.NewMapper()

	// 1. Integer
	m.Add(parser.Quad{Predicate: "http://example.org/age", Object: int64(30)})
	// 2. Float
	m.Add(parser.Quad{Predicate: "http://example.org/score", Object: 9.5})
	// 3. Bool
	m.Add(parser.Quad{Predicate: "http://example.org/active", Object: true})
	// 4. String
	m.Add(parser.Quad{Predicate: "http://example.org/name", Object: "Alice"})
	// 5. Mixed Int + Float -> Double
	m.Add(parser.Quad{Predicate: "http://example.org/mixed_num", Object: int64(10)})
	m.Add(parser.Quad{Predicate: "http://example.org/mixed_num", Object: 10.5})
	// 6. Mixed Int + String -> Text
	m.Add(parser.Quad{Predicate: "http://example.org/mixed_text", Object: int64(10)})
	m.Add(parser.Quad{Predicate: "http://example.org/mixed_text", Object: "ten"})

	output, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}

	mappings, ok := result["mappings"].(map[string]interface{})
	if !ok {
		t.Fatal("Missing mappings key")
	}
	props, ok := mappings["properties"].(map[string]interface{})
	if !ok {
		t.Fatal("Missing properties key")
	}

	tests := []struct {
		field string
		want  string
	}{
		{"http://example org/age", "long"},
		{"http://example org/score", "double"},
		{"http://example org/active", "boolean"},
		{"http://example org/name", "text"},
		{"http://example org/mixed_num", "double"},
		{"http://example org/mixed_text", "text"},
		{"_graph", "keyword"},
	}

	for _, tt := range tests {
		fieldMap, ok := props[tt.field].(map[string]interface{})
		if !ok {
			t.Errorf("Field %s missing", tt.field)
			continue
		}
		gotType, ok := fieldMap["type"].(string)
		if !ok {
			t.Errorf("Field %s: type not a string", tt.field)
			continue
		}
		if gotType != tt.want {
			t.Errorf("Field %s: got type %s, want %s", tt.field, gotType, tt.want)
		}
	}
}
