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

	// 7. URI Object -> Keyword
	// We use ParseQuad to get the correct type from the parser (which should be distinct from string)
	qURI, err := parser.ParseQuad(`<http://example.org/s> <http://example.org/ref> <http://example.org/obj> .`)
	if err != nil {
		t.Fatalf("ParseQuad failed: %v", err)
	}
	m.Add(qURI)

	// 8. Date + DateTime should remain date.
	m.Add(parser.Quad{Predicate: "http://example.org/date_mixed", Object: parser.Date("2024-01-31")})
	m.Add(parser.Quad{Predicate: "http://example.org/date_mixed", Object: parser.DateTime("2024-01-31T13:45:30Z")})

	// 9. Date + text should downgrade to text for upload safety.
	m.Add(parser.Quad{Predicate: "http://example.org/date_text_mixed", Object: parser.Date("2024-01-31")})
	m.Add(parser.Quad{Predicate: "http://example.org/date_text_mixed", Object: "not-a-date"})

	// 10. xsd:anyURI literal should map to keyword.
	m.Add(parser.Quad{Predicate: "http://example.org/any_uri", Object: parser.AnyURI("https://example.org/resource")})

	// 11. Short literal codes with enough samples should map to keyword.
	for _, code := range []string{"A1", "B2", "C3", "D4", "E5"} {
		m.Add(parser.Quad{Predicate: "http://example.org/code", Object: code})
	}

	// 12. Language-tagged literals should map to text.
	m.Add(parser.Quad{Predicate: "http://example.org/description", Object: parser.LangString{Value: "Korte omschrijving", Lang: "nl"}})
	m.Add(parser.Quad{Predicate: "http://example.org/description", Object: parser.LangString{Value: "Lange beschrijving", Lang: "nl-NL"}})

	// 13. rdf:HTML should map to text with html_strip analyzer.
	m.Add(parser.Quad{Predicate: "http://example.org/html", Object: parser.RDFHTML("<p>Hello</p>")})

	// 14. geosparql literals should map to geo_shape.
	m.Add(parser.Quad{Predicate: "http://example.org/wkt", Object: parser.GeoWKT("POINT (30 10)")})
	m.Add(parser.Quad{Predicate: "http://example.org/geojson", Object: parser.GeoJSON(`{"type":"Point","coordinates":[30,10]}`)})

	// 15. Triply markdown datatype should map to text.
	m.Add(parser.Quad{Predicate: "http://example.org/markdown", Object: parser.TriplyMarkdown("# Title")})

	output, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}

	// We expect the result to start with "properties" now.
	props, ok := result["properties"].(map[string]interface{})
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
		{"http://example org/ref", "keyword"},
		{"http://example org/any_uri", "keyword"},
		{"http://example org/code", "keyword"},
		{"http://example org/description", "text"},
		{"http://example org/html", "text"},
		{"http://example org/wkt", "geo_shape"},
		{"http://example org/geojson", "geo_shape"},
		{"http://example org/markdown", "text"},
		{"http://example org/date_mixed", "date"},
		{"http://example org/date_text_mixed", "text"},
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

		if gotType == "text" {
			if _, hasFields := fieldMap["fields"]; hasFields {
				t.Errorf("Field %s: text field should not have a keyword subfield", tt.field)
			}
		}

		if tt.field == "http://example org/html" {
			if gotAnalyzer, hasAnalyzer := fieldMap["analyzer"].(string); hasAnalyzer {
				t.Errorf("Field %s: rdf:HTML field should not set a custom analyzer, got %s", tt.field, gotAnalyzer)
			}
		}
	}

	descriptionMap, ok := props["http://example org/description"].(map[string]interface{})
	if !ok {
		t.Fatal("description field missing")
	}
	analyzer, ok := descriptionMap["analyzer"].(string)
	if !ok {
		t.Fatalf("description analyzer missing or not string: %v", descriptionMap["analyzer"])
	}
	if analyzer != "dutch" {
		t.Fatalf("description analyzer = %q, want %q", analyzer, "dutch")
	}
}

func TestMapper_Generate_WithForcedTextAnalyzer(t *testing.T) {
	m := mapping.NewMapper()
	m.SetTextAnalyzer("standard")

	m.Add(parser.Quad{Predicate: "http://example.org/name", Object: "Alice Wonderland"})
	m.Add(parser.Quad{Predicate: "http://example.org/description", Object: parser.LangString{Value: "Een voorbeeld", Lang: "nl"}})

	output, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}

	props, ok := result["properties"].(map[string]interface{})
	if !ok {
		t.Fatal("Missing properties key")
	}

	for _, field := range []string{"http://example org/name", "http://example org/description"} {
		fieldMap, ok := props[field].(map[string]interface{})
		if !ok {
			t.Fatalf("Field %s missing", field)
		}
		if gotType, _ := fieldMap["type"].(string); gotType != "text" {
			t.Fatalf("Field %s type = %q, want text", field, gotType)
		}
		if analyzer, _ := fieldMap["analyzer"].(string); analyzer != "standard" {
			t.Fatalf("Field %s analyzer = %q, want standard", field, analyzer)
		}
	}
}
