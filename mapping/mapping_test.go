package mapping_test

import (
	"encoding/json"
	"strings"
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
			fields, hasFields := fieldMap["fields"].(map[string]interface{})
			if !hasFields {
				t.Errorf("Field %s: expected a keyword subfield by default", tt.field)
			} else {
				keyword, ok := fields["keyword"].(map[string]interface{})
				if !ok || keyword["type"] != "keyword" || keyword["ignore_above"] != float64(256) {
					t.Errorf("Field %s: unexpected keyword subfield: %v", tt.field, fields["keyword"])
				}
			}
		}

		if tt.field == "http://example org/html" {
			if gotAnalyzer, _ := fieldMap["analyzer"].(string); gotAnalyzer != "html_strip" {
				t.Errorf("Field %s: rdf:HTML field should use the html_strip analyzer, got %q", tt.field, gotAnalyzer)
			}
		}
		if tt.field == "http://example org/ref" && fieldMap["ignore_above"] != float64(256) {
			t.Errorf("URI keyword ignore_above = %v, want 256", fieldMap["ignore_above"])
		}
		if tt.field == "http://example org/date_mixed" {
			format, _ := fieldMap["format"].(string)
			if !strings.Contains(format, "epoch_millis") || !strings.Contains(format, "strict_hour_minute_second") {
				t.Errorf("date format = %q, want epoch and time formats", format)
			}
		}
		if (tt.field == "http://example org/wkt" || tt.field == "http://example org/geojson") && fieldMap["ignore_malformed"] != true {
			t.Errorf("geo_shape should ignore malformed values by default, got %v", fieldMap)
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
	if result["date_detection"] != false || result["numeric_detection"] != false {
		t.Errorf("date/numeric detection should be disabled, got date=%v numeric=%v", result["date_detection"], result["numeric_detection"])
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

	// The mapping is sent to PUT /<index>/_mapping (e.g. via esbulk
	// -mapping), which rejects a top-level "settings" key; analyzer
	// definitions belong to the index-creation body (see GenerateConfig).
	if _, ok := result["settings"]; ok {
		t.Errorf("Generate must not emit top-level settings: %v", result["settings"])
	}
}

func TestMapper_GenerateConfig(t *testing.T) {
	m := mapping.NewMapper()
	m.Add(parser.Quad{Predicate: "http://example.org/html", Object: parser.RDFHTML("<p>hi</p>")})
	m.Add(parser.Quad{Predicate: "http://example.org/name", Object: "Alice"})

	output, err := m.GenerateConfig()
	if err != nil {
		t.Fatalf("GenerateConfig failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}

	// The html field references the html_strip analyzer, so the index
	// config must define it under settings.analysis.analyzer.
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

	mappings, ok := result["mappings"].(map[string]interface{})
	if !ok {
		t.Fatal("mappings key missing")
	}
	props, ok := mappings["properties"].(map[string]interface{})
	if !ok {
		t.Fatal("mappings.properties missing")
	}
	htmlField, ok := props["http://example org/html"].(map[string]interface{})
	if !ok {
		t.Fatal("html field missing from mappings.properties")
	}
	if got := htmlField["analyzer"]; got != "html_strip" {
		t.Errorf("html field analyzer = %v, want html_strip", got)
	}
}

func TestMapper_GenerateConfig_SettingsAlwaysPresent(t *testing.T) {
	// Even without rdf:HTML fields, the settings must be included: analyzer
	// definitions cannot be added to a live index, so omitting them would
	// leave no way to add an rdf:HTML field later without recreating the
	// index.
	m := mapping.NewMapper()
	m.Add(parser.Quad{Predicate: "http://example.org/name", Object: "Alice"})

	output, err := m.GenerateConfig()
	if err != nil {
		t.Fatalf("GenerateConfig failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}
	settings, ok := result["settings"].(map[string]interface{})
	if !ok {
		t.Fatal("settings should be emitted even without rdf:HTML fields")
	}
	analysis, ok := settings["analysis"].(map[string]interface{})
	if !ok {
		t.Fatal("settings.analysis missing")
	}
	analyzers, ok := analysis["analyzer"].(map[string]interface{})
	if !ok {
		t.Fatal("settings.analysis.analyzer missing")
	}
	if _, ok := analyzers["html_strip"]; !ok {
		t.Errorf("html_strip analyzer not defined, got: %v", analyzers)
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

func TestMapper_GenerateDatatypeAndOptionMappings(t *testing.T) {
	options := mapping.DefaultOptions()
	options.DecimalScalingFactor = 1000
	options.CopyTo = "full_text"
	options.KeywordLowercaseNormalizer = true
	options.DateSubfield = true
	options.IgnoreMalformedGeo = true
	m := mapping.NewMapper(options)
	m.SetTextAnalyzer("standard")

	for _, line := range []string{
		`<s> <http://example.org/decimal> "1.25"^^<http://www.w3.org/2001/XMLSchema#decimal> .`,
		`<s> <http://example.org/mixed> "1.25"^^<http://www.w3.org/2001/XMLSchema#decimal> .`,
		`<s> <http://example.org/mixed> "2.5"^^<http://www.w3.org/2001/XMLSchema#double> .`,
		`<s> <http://example.org/time> "12:34:56.123Z"^^<http://www.w3.org/2001/XMLSchema#time> .`,
		`<s> <http://example.org/language> "en"^^<http://www.w3.org/2001/XMLSchema#language> .`,
		`<s> <http://example.org/keyword> "P1Y"^^<http://www.w3.org/2001/XMLSchema#duration> .`,
		`<s> <http://example.org/json> "{\"nested\":{\"value\":1}}"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON> .`,
		`<s> <http://example.org/json> "plain scalar" .`,
		`<s> <http://example.org/geo> "POINT (30 10)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .`,
		`<s> <http://example.org/description> "A long description"@en .`,
	} {
		quad, err := parser.ParseQuad(line)
		if err != nil {
			t.Fatalf("ParseQuad(%q): %v", line, err)
		}
		m.Add(quad)
	}
	for _, datatype := range []string{
		"integer", "int", "long", "short", "byte", "nonNegativeInteger",
		"nonPositiveInteger", "positiveInteger", "negativeInteger", "unsignedLong",
		"unsignedInt", "unsignedShort", "unsignedByte",
	} {
		line := `<s> <http://example.org/integer_` + datatype + `> "123"^^<http://www.w3.org/2001/XMLSchema#` + datatype + `> .`
		quad, err := parser.ParseQuad(line)
		if err != nil {
			t.Fatalf("ParseQuad(%q): %v", line, err)
		}
		m.Add(quad)
	}
	for datatype, value := range map[string]string{
		"base64Binary": "YWJj",
		"hexBinary":    "AABB",
		"QName":        "ns:name",
		"NMTOKEN":      "name-token",
	} {
		m.Add(parser.Quad{Predicate: "http://example.org/keyword_" + datatype, Object: parser.XSDKeyword(value)})
	}
	invalidJSON, err := parser.ParseQuad(`<s> <http://example.org/invalid_json> "{invalid"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON> .`)
	if err != nil {
		t.Fatalf("ParseQuad(invalid rdf:JSON): %v", err)
	}
	m.Add(invalidJSON)

	output, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Invalid mapping JSON: %v", err)
	}
	props := result["properties"].(map[string]interface{})

	assertMapping := func(field, wantType string) map[string]interface{} {
		t.Helper()
		fieldMapping, ok := props[field].(map[string]interface{})
		if !ok || fieldMapping["type"] != wantType {
			t.Fatalf("%s mapping = %v, want type %q", field, props[field], wantType)
		}
		return fieldMapping
	}

	decimal := assertMapping("http://example org/decimal", "scaled_float")
	if decimal["scaling_factor"] != float64(1000) {
		t.Errorf("decimal scaling_factor = %v, want 1000", decimal["scaling_factor"])
	}
	assertMapping("http://example org/mixed", "double")
	for _, datatype := range []string{
		"integer", "int", "long", "short", "byte", "nonNegativeInteger",
		"nonPositiveInteger", "positiveInteger", "negativeInteger", "unsignedLong",
		"unsignedInt", "unsignedShort", "unsignedByte",
	} {
		assertMapping("http://example org/integer_"+datatype, "long")
	}
	date := assertMapping("http://example org/time", "text")
	dateFields := date["fields"].(map[string]interface{})
	dateField := dateFields["date"].(map[string]interface{})
	if dateField["type"] != "date" || dateField["ignore_malformed"] != true {
		t.Errorf("date sub-field = %v, want tolerant date", dateField)
	}
	if format, _ := dateField["format"].(string); !strings.Contains(format, "strict_hour_minute_second") || !strings.Contains(format, "epoch_millis") {
		t.Errorf("date format = %q, want time and epoch formats", format)
	}

	language := assertMapping("http://example org/language", "keyword")
	if language["ignore_above"] != float64(256) || language["normalizer"] != "lowercase_normalizer" {
		t.Errorf("language keyword mapping = %v", language)
	}
	assertMapping("http://example org/keyword", "keyword")
	for _, datatype := range []string{"base64Binary", "hexBinary", "QName", "NMTOKEN"} {
		assertMapping("http://example org/keyword_"+datatype, "keyword")
	}
	assertMapping("http://example org/json", "text")
	assertMapping("http://example org/invalid_json", "text")
	geo := assertMapping("http://example org/geo", "geo_shape")
	if geo["ignore_malformed"] != true {
		t.Errorf("geo mapping = %v, want ignore_malformed", geo)
	}

	description := assertMapping("http://example org/description", "text")
	if description["copy_to"] != "full_text" {
		t.Errorf("description copy_to = %v, want full_text", description["copy_to"])
	}
	if description["analyzer"] != "standard" {
		t.Errorf("description analyzer = %v, want standard", description["analyzer"])
	}
	fullText := assertMapping("full_text", "text")
	if fullText["analyzer"] != "copy_to_html_strip" {
		t.Errorf("full_text analyzer = %v, want copy_to_html_strip", fullText["analyzer"])
	}
	if _, ok := fullText["copy_to"]; ok {
		t.Errorf("copy-to target must not copy to itself: %v", fullText)
	}
	if _, ok := result["settings"]; ok {
		t.Fatal("Generate should not include index settings")
	}

	config, err := m.GenerateConfig()
	if err != nil {
		t.Fatalf("GenerateConfig failed: %v", err)
	}
	var configDoc map[string]interface{}
	if err := json.Unmarshal(config, &configDoc); err != nil {
		t.Fatalf("Invalid config JSON: %v", err)
	}
	analysis := configDoc["settings"].(map[string]interface{})["analysis"].(map[string]interface{})
	normalizers := analysis["normalizer"].(map[string]interface{})
	if normalizers["lowercase_normalizer"] == nil {
		t.Fatalf("normalizer missing from settings: %v", analysis)
	}
}

func TestMapper_GenerateOptionDisables(t *testing.T) {
	options := mapping.DefaultOptions()
	options.TextKeywordSubfield = false
	options.KeywordIgnoreAbove = 0
	options.IgnoreMalformedGeo = false
	m := mapping.NewMapper(options)
	m.Add(parser.Quad{Predicate: "http://example.org/text", Object: "a long enough description"})
	m.Add(parser.Quad{Predicate: "http://example.org/geo", Object: parser.GeoWKT("POINT (30 10)")})

	output, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Invalid mapping JSON: %v", err)
	}
	props := result["properties"].(map[string]interface{})
	text := props["http://example org/text"].(map[string]interface{})
	if _, ok := text["fields"]; ok {
		t.Errorf("text mapping unexpectedly has keyword sub-fields: %v", text["fields"])
	}
	geo := props["http://example org/geo"].(map[string]interface{})
	if _, ok := geo["ignore_malformed"]; ok {
		t.Errorf("geo ignore_malformed should be omitted when disabled: %v", geo)
	}
}

func TestMapper_GenerateCopyToConflict(t *testing.T) {
	options := mapping.DefaultOptions()
	options.CopyTo = "full_text"
	m := mapping.NewMapper(options)
	m.Add(parser.Quad{Predicate: "full_text", Object: "source"})
	if _, err := m.Generate(); err == nil {
		t.Fatal("expected copy_to target conflict error")
	}
}

func TestMapper_CopyToTargetStripsMarkup(t *testing.T) {
	options := mapping.DefaultOptions()
	options.CopyTo = "full_text"
	m := mapping.NewMapper(options)
	// An rdf:HTML field copies raw markup into the catch-all; the catch-all
	// must strip it so searches cannot match tag or attribute names.
	m.Add(parser.Quad{Predicate: "http://example.org/html", Object: parser.RDFHTML(`<p class="x">Hello</p>`)})
	m.Add(parser.Quad{Predicate: "http://example.org/description", Object: "A long enough description"})

	output, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}
	props := result["properties"].(map[string]interface{})

	html := props["http://example org/html"].(map[string]interface{})
	if html["analyzer"] != "html_strip" {
		t.Errorf("rdf:HTML source analyzer = %v, want html_strip", html["analyzer"])
	}
	if html["copy_to"] != "full_text" {
		t.Errorf("rdf:HTML source copy_to = %v, want full_text (raw markup is copied)", html["copy_to"])
	}

	fullText := props["full_text"].(map[string]interface{})
	if fullText["type"] != "text" {
		t.Fatalf("full_text type = %v, want text", fullText["type"])
	}
	if fullText["analyzer"] != "copy_to_html_strip" {
		t.Errorf("full_text analyzer = %v, want copy_to_html_strip", fullText["analyzer"])
	}
	if _, ok := fullText["copy_to"]; ok {
		t.Errorf("copy-to target must not copy to itself: %v", fullText)
	}

	// The catch-all analyzer must be defined in the index-creation settings,
	// otherwise the mapping references an analyzer that does not exist.
	config, err := m.GenerateConfig()
	if err != nil {
		t.Fatalf("GenerateConfig failed: %v", err)
	}
	var configDoc map[string]interface{}
	if err := json.Unmarshal(config, &configDoc); err != nil {
		t.Fatalf("Invalid config JSON: %v", err)
	}
	analyzers := configDoc["settings"].(map[string]interface{})["analysis"].(map[string]interface{})["analyzer"].(map[string]interface{})
	catchAll, ok := analyzers["copy_to_html_strip"].(map[string]interface{})
	if !ok {
		t.Fatalf("copy_to_html_strip analyzer not defined, got: %v", analyzers)
	}
	if catchAll["type"] != "custom" || catchAll["tokenizer"] != "standard" {
		t.Errorf("copy_to_html_strip = %v, want custom analyzer with standard tokenizer", catchAll)
	}
	charFilters, ok := catchAll["char_filter"].([]interface{})
	if !ok || len(charFilters) != 1 || charFilters[0] != "html_strip" {
		t.Errorf("copy_to_html_strip char_filter = %v, want [html_strip]", catchAll["char_filter"])
	}
}

func TestMapper_AnalyzerTieIsDeterministic(t *testing.T) {
	quads := []parser.Quad{
		{Predicate: "http://example.org/description", Object: parser.LangString{Value: "A sufficiently long English description", Lang: "en"}},
		{Predicate: "http://example.org/description", Object: parser.LangString{Value: "Een voldoende lange Nederlandse omschrijving", Lang: "nl"}},
	}
	generate := func(input []parser.Quad) []byte {
		t.Helper()
		m := mapping.NewMapper()
		for _, q := range input {
			m.Add(q)
		}
		output, err := m.Generate()
		if err != nil {
			t.Fatalf("Generate failed: %v", err)
		}
		return output
	}

	forward := generate(quads)
	reversed := generate([]parser.Quad{quads[1], quads[0]})
	if string(forward) != string(reversed) {
		t.Fatalf("mapping differs by input order:\nforward: %s\nreversed: %s", forward, reversed)
	}
	for i := 0; i < 10; i++ {
		if got := generate(quads); string(got) != string(forward) {
			t.Fatalf("repeated generation %d changed mapping:\nfirst: %s\nnext: %s", i, forward, got)
		}
	}

	var result map[string]interface{}
	if err := json.Unmarshal(forward, &result); err != nil {
		t.Fatalf("Invalid mapping JSON: %v", err)
	}
	props := result["properties"].(map[string]interface{})
	description := props["http://example org/description"].(map[string]interface{})
	if description["analyzer"] != "dutch" {
		t.Errorf("tied analyzer = %v, want deterministic lexical winner \"dutch\"", description["analyzer"])
	}
}
