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

	// 15. Markdown datatype should map to text.
	m.Add(parser.Quad{Predicate: "http://example.org/markdown", Object: parser.Markdown("# Title")})

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
			if gotAnalyzer, _ := fieldMap["analyzer"].(string); gotAnalyzer != "html_strip" {
				t.Errorf("Field %s: rdf:HTML field should use the html_strip analyzer, got %q", tt.field, gotAnalyzer)
			}
		}
	}

	templates, ok := result["dynamic_templates"].([]interface{})
	if !ok || len(templates) == 0 {
		t.Fatalf("Missing dynamic_templates in generated mapping: %v", result["dynamic_templates"])
	}
	foundStringsAsKeyword := false
	for _, tpl := range templates {
		tplMap, ok := tpl.(map[string]interface{})
		if !ok {
			continue
		}
		for _, rule := range tplMap {
			ruleMap, ok := rule.(map[string]interface{})
			if !ok {
				continue
			}
			if ruleMap["match_mapping_type"] == "string" {
				mappingField, _ := ruleMap["mapping"].(map[string]interface{})
				if mappingField["type"] == "keyword" {
					foundStringsAsKeyword = true
				}
			}
		}
	}
	if !foundStringsAsKeyword {
		t.Errorf("dynamic_templates should map dynamic strings to keyword, got: %v", templates)
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

	// The html field references the html_strip analyzer, so the generated
	// mapping must define it under settings.analysis.analyzer.
	settings, ok := result["settings"].(map[string]interface{})
	if !ok {
		t.Fatal("settings key missing: html_strip analyzer must be defined")
	}
	analysis, ok := settings["analysis"].(map[string]interface{})
	if !ok {
		t.Fatal("settings.analysis missing")
	}
	analyzers, ok := analysis["analyzer"].(map[string]interface{})
	if !ok {
		t.Fatal("settings.analysis.analyzer missing")
	}
	htmlStrip, ok := analyzers["html_strip"].(map[string]interface{})
	if !ok {
		t.Fatalf("html_strip analyzer not defined, got: %v", analyzers)
	}
	if htmlStrip["type"] != "custom" {
		t.Errorf("html_strip type = %v, want custom", htmlStrip["type"])
	}
	charFilters, ok := htmlStrip["char_filter"].([]interface{})
	if !ok || len(charFilters) != 1 || charFilters[0] != "html_strip" {
		t.Errorf("html_strip char_filter = %v, want [html_strip]", htmlStrip["char_filter"])
	}
	if htmlStrip["tokenizer"] != "standard" {
		t.Errorf("html_strip tokenizer = %v, want standard", htmlStrip["tokenizer"])
	}
}

func TestMapper_Generate_NoHTMLNoSettings(t *testing.T) {
	m := mapping.NewMapper()
	m.Add(parser.Quad{Predicate: "http://example.org/name", Object: "Alice"})

	output, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}
	if _, ok := result["settings"]; ok {
		t.Errorf("settings should not be emitted when no field uses the html_strip analyzer: %v", result["settings"])
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

func TestMapper_Generate_HTMLWinsOverForcedAnalyzer(t *testing.T) {
	m := mapping.NewMapper()
	m.SetTextAnalyzer("standard")

	m.Add(parser.Quad{Predicate: "http://example.org/html", Object: parser.RDFHTML("<p>Hello</p>")})

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
	fieldMap, ok := props["http://example org/html"].(map[string]interface{})
	if !ok {
		t.Fatal("html field missing")
	}
	if analyzer, _ := fieldMap["analyzer"].(string); analyzer != "html_strip" {
		t.Errorf("rdf:HTML field analyzer = %q, want html_strip even with -text-analyzer set", analyzer)
	}
}
