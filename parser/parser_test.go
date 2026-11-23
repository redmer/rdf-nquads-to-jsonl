package parser_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/redmer/rdf-nquads-to-jsonl/parser"
)

func TestParseQuad(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		wantSubj  string
		wantPred  string
		wantObj   interface{}
		wantGraph string
		wantErr   bool
	}{
		{
			name:      "URI object",
			line:      `<https://example.com/person/123> <http://schema.org/knows> <https://example.com/person/456> <https://example.com/graph> .`,
			wantSubj:  "https://example.com/person/123",
			wantPred:  "http://schema.org/knows",
			wantObj:   parser.URI("https://example.com/person/456"),
			wantGraph: "https://example.com/graph",
		},
		{
			name:      "plain literal",
			line:      `<https://example.com/person/123> <http://schema.org/name> "John Doe" <https://example.com/graph> .`,
			wantSubj:  "https://example.com/person/123",
			wantPred:  "http://schema.org/name",
			wantObj:   "John Doe",
			wantGraph: "https://example.com/graph",
		},
		{
			name:      "literal with language tag",
			line:      `<https://example.com/person/123> <http://schema.org/name> "John Doe"@en <https://example.com/graph> .`,
			wantSubj:  "https://example.com/person/123",
			wantPred:  "http://schema.org/name",
			wantObj:   parser.LangString{Value: "John Doe", Lang: "en"},
			wantGraph: "https://example.com/graph",
		},
		{
			name:      "literal with datatype",
			line:      `<https://example.com/person/123> <http://schema.org/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> <https://example.com/graph> .`,
			wantSubj:  "https://example.com/person/123",
			wantPred:  "http://schema.org/age",
			wantObj:   int64(42),
			wantGraph: "https://example.com/graph",
		},
		{
			name:     "no graph (triple format)",
			line:     `<https://example.com/person/123> <http://schema.org/name> "Jane" .`,
			wantSubj: "https://example.com/person/123",
			wantPred: "http://schema.org/name",
			wantObj:  "Jane",
		},
		{
			name:    "empty line",
			line:    ``,
			wantErr: true,
		},
		{
			name:    "comment line",
			line:    `# this is a comment`,
			wantErr: true,
		},
		{
			name:     "boolean true",
			line:     `<http://example.org/s> <http://example.org/p> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  true,
		},
		{
			name:     "boolean false",
			line:     `<http://example.org/s> <http://example.org/p> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  false,
		},
		{
			name:     "integer",
			line:     `<http://example.org/s> <http://example.org/p> "123"^^<http://www.w3.org/2001/XMLSchema#integer> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  int64(123),
		},
		{
			name:     "xsd:int",
			line:     `<http://example.org/s> <http://example.org/p> "456"^^<http://www.w3.org/2001/XMLSchema#int> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  int64(456),
		},
		{
			name:     "decimal",
			line:     `<http://example.org/s> <http://example.org/p> "123.45"^^<http://www.w3.org/2001/XMLSchema#decimal> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  parser.XSDDecimal("123.45"),
		},
		{
			name:     "xsd:double",
			line:     `<http://example.org/s> <http://example.org/p> "1.23e2"^^<http://www.w3.org/2001/XMLSchema#double> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  123.0,
		},
		{
			name:     "xsd:float",
			line:     `<http://example.org/s> <http://example.org/p> "1.23e2"^^<http://www.w3.org/2001/XMLSchema#float> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  123.0,
		},
		{
			name:     "xsd:date",
			line:     `<http://example.org/s> <http://example.org/p> "2024-01-31"^^<http://www.w3.org/2001/XMLSchema#date> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  parser.Date("2024-01-31"),
		},
		{
			name:     "xsd:dateTime",
			line:     `<http://example.org/s> <http://example.org/p> "2024-01-31T13:45:30Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  parser.DateTime("2024-01-31T13:45:30Z"),
		},
		{
			name:     "xsd:anyURI",
			line:     `<http://example.org/s> <http://example.org/p> "https://example.org/resource"^^<http://www.w3.org/2001/XMLSchema#anyURI> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  parser.AnyURI("https://example.org/resource"),
		},
		{
			name:     "rdf:HTML",
			line:     `<http://example.org/s> <http://example.org/p> "<p>Hello</p>"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  parser.RDFHTML("<p>Hello</p>"),
		},
		{
			name:     "geosparql:wktLiteral",
			line:     `<http://example.org/s> <http://example.org/p> "POINT (30 10)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  parser.GeoWKT("POINT (30 10)"),
		},
		{
			name:     "geosparql:geoJSONLiteral",
			line:     `<http://example.org/s> <http://example.org/p> "{\"type\":\"Point\",\"coordinates\":[30,10]}"^^<http://www.opengis.net/ont/geosparql#geoJSONLiteral> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  parser.GeoJSON(`{"type":"Point","coordinates":[30,10]}`),
		},
		{
			name:     "Triply markdown datatype",
			line:     `<http://example.org/s> <http://example.org/p> "# Heading"^^<https://triplydb.com/Triply/vocab/def/markdown> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  parser.Markdown("# Heading"),
		},
		{
			name:     "Ontowiki markdown datatype",
			line:     `<http://example.org/s> <http://example.org/p> "# Heading"^^<http://ns.ontowiki.net/SysOnt/Markdown> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  parser.Markdown("# Heading"),
		},
		{
			name:     "literal with escaped quote",
			line:     `<https://example.com/s> <https://example.com/p> "He said \"hello\"" .`,
			wantSubj: "https://example.com/s",
			wantPred: "https://example.com/p",
			wantObj:  `He said "hello"`,
		}, {
			name:     "very long literal (1MB)",
			line:     `<http://example.org/s> <http://example.org/p> "` + strings.Repeat("a", 1024*1024) + `" .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  strings.Repeat("a", 1024*1024),
		},
		{
			name:     "very long URI",
			line:     `<http://example.org/` + strings.Repeat("path/", 2000) + `s> <http://example.org/p> "val" .`,
			wantSubj: "http://example.org/" + strings.Repeat("path/", 2000) + "s",
			wantPred: "http://example.org/p",
			wantObj:  "val",
		},
		{
			name:     "xsd:double NaN stays a string",
			line:     `<http://example.org/s> <http://example.org/p> "NaN"^^<http://www.w3.org/2001/XMLSchema#double> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  "NaN",
		},
		{
			name:     "xsd:float INF stays a string",
			line:     `<http://example.org/s> <http://example.org/p> "INF"^^<http://www.w3.org/2001/XMLSchema#float> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  "INF",
		},
		{
			name:     "xsd:decimal -INF stays a string",
			line:     `<http://example.org/s> <http://example.org/p> "-INF"^^<http://www.w3.org/2001/XMLSchema#decimal> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  "-INF",
		},
		{
			name:     "xsd:double +INF stays a string",
			line:     `<http://example.org/s> <http://example.org/p> "+INF"^^<http://www.w3.org/2001/XMLSchema#double> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  "+INF",
		},
		{
			name:     "xsd:double overflowing float64 stays a string",
			line:     `<http://example.org/s> <http://example.org/p> "1e999"^^<http://www.w3.org/2001/XMLSchema#double> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  "1e999",
		},
		{
			name:     "integer overflowing int64 falls back to float",
			line:     `<http://example.org/s> <http://example.org/p> "9223372036854775808"^^<http://www.w3.org/2001/XMLSchema#integer> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  float64(9223372036854775808),
		},
		{
			name:     "integer so large it overflows float64 stays a string",
			line:     `<http://example.org/s> <http://example.org/p> "` + strings.Repeat("9", 400) + `"^^<http://www.w3.org/2001/XMLSchema#integer> .`,
			wantSubj: "http://example.org/s",
			wantPred: "http://example.org/p",
			wantObj:  strings.Repeat("9", 400),
		}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quad, err := parser.ParseQuad(tt.line)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseQuad(%q) expected error, got nil", tt.line)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseQuad(%q) unexpected error: %v", tt.line, err)
			}
			if quad.Subject != tt.wantSubj {
				t.Errorf("Subject = %q, want %q", quad.Subject, tt.wantSubj)
			}
			if quad.Predicate != tt.wantPred {
				t.Errorf("Predicate = %q, want %q", quad.Predicate, tt.wantPred)
			}
			if quad.Object != tt.wantObj {
				t.Errorf("Object = %v, want %v", quad.Object, tt.wantObj)
			}
			if quad.Graph != tt.wantGraph {
				t.Errorf("Graph = %q, want %q", quad.Graph, tt.wantGraph)
			}
		})
	}
}

func TestGeoJSONMarshalJSON(t *testing.T) {
	tests := []struct {
		name  string
		input parser.GeoJSON
		// wantObj is set when the literal should marshal as a JSON object
		wantObj map[string]interface{}
		// wantStr is set when the literal should marshal as a plain JSON string
		wantStr string
	}{
		{
			name:    "valid GeoJSON object",
			input:   parser.GeoJSON(`{"type":"Point","coordinates":[30,10]}`),
			wantObj: map[string]interface{}{"type": "Point", "coordinates": []interface{}{30.0, 10.0}},
		},
		{
			name:    "precision loss int GeoJSON object",
			input:   parser.GeoJSON(`{"type":"Point","coordinates":[9007199254740993,10]}`),
			wantObj: map[string]interface{}{"type": "Point", "coordinates": []interface{}{9007199254740993.0, 10.0}},
		},
		{
			name:    "WKT-like value stays a string",
			input:   parser.GeoJSON("POINT (30 10)"),
			wantStr: "POINT (30 10)",
		},
		{
			name:    "JSON array is not an object, stays a string",
			input:   parser.GeoJSON(`[1,2,3]`),
			wantStr: "[1,2,3]",
		},
		{
			name:    "JSON null is not an object, stays a string",
			input:   parser.GeoJSON(`null`),
			wantStr: "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.input)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}
			if tt.wantObj != nil {
				var got map[string]interface{}
				if err := json.Unmarshal(b, &got); err != nil {
					t.Fatalf("expected JSON object, got %q: %v", b, err)
				}
				if !reflect.DeepEqual(got, tt.wantObj) {
					t.Errorf("got %v, want %v", got, tt.wantObj)
				}
			} else {
				var got string
				if err := json.Unmarshal(b, &got); err != nil {
					t.Fatalf("expected JSON string, got %q: %v", b, err)
				}
				if got != tt.wantStr {
					t.Errorf("got %q, want %q", got, tt.wantStr)
				}
			}
		})
	}
}

func TestXSDIntegerFamily(t *testing.T) {
	tests := []struct {
		datatype string
		valid    []string
		invalid  []string
	}{
		{datatype: "integer", valid: []string{"123", "9223372036854775808", strings.Repeat("9", 400)}},
		{datatype: "int", valid: []string{"-2147483648", "2147483647"}, invalid: []string{"-2147483649", "2147483648"}},
		{datatype: "long", valid: []string{"-9223372036854775808", "9223372036854775807"}, invalid: []string{"-9223372036854775809", "9223372036854775808"}},
		{datatype: "short", valid: []string{"-32768", "32767"}, invalid: []string{"-32769", "32768"}},
		{datatype: "byte", valid: []string{"-128", "127"}, invalid: []string{"-129", "128"}},
		{datatype: "nonNegativeInteger", valid: []string{"0", "9223372036854775808"}, invalid: []string{"-1"}},
		{datatype: "nonPositiveInteger", valid: []string{"-9223372036854775809", "0"}, invalid: []string{"1"}},
		{datatype: "positiveInteger", valid: []string{"1", "9223372036854775808"}, invalid: []string{"-1", "0"}},
		{datatype: "negativeInteger", valid: []string{"-9223372036854775809", "-1"}, invalid: []string{"0", "1"}},
		{datatype: "unsignedLong", valid: []string{"0", "18446744073709551615"}, invalid: []string{"-1", "18446744073709551616"}},
		{datatype: "unsignedInt", valid: []string{"0", "4294967295"}, invalid: []string{"-1", "4294967296"}},
		{datatype: "unsignedShort", valid: []string{"0", "65535"}, invalid: []string{"-1", "65536"}},
		{datatype: "unsignedByte", valid: []string{"0", "255"}, invalid: []string{"-1", "256"}},
	}
	integerFallback := map[string]bool{
		"integer": true, "nonNegativeInteger": true, "nonPositiveInteger": true,
		"positiveInteger": true, "negativeInteger": true, "unsignedLong": true,
	}
	for _, tt := range tests {
		t.Run(tt.datatype, func(t *testing.T) {
			for _, value := range tt.valid {
				quad, err := parser.ParseQuad(`<s> <p> "` + value + `"^^<http://www.w3.org/2001/XMLSchema#` + tt.datatype + `> .`)
				if err != nil {
					t.Errorf("ParseQuad(%q) failed: %v", value, err)
					continue
				}
				if len(value) < 400 && (!integerFallback[tt.datatype] || len(value) < 19) {
					if _, ok := quad.Object.(int64); !ok {
						t.Errorf("ParseQuad(%q) object = %#v (%T), want int64", value, quad.Object, quad.Object)
					}
				} else if len(value) < 400 {
					if _, ok := quad.Object.(float64); !ok {
						t.Errorf("ParseQuad(%q) object = %#v (%T), want float64 fallback", value, quad.Object, quad.Object)
					}
				} else if got, ok := quad.Object.(string); !ok || got != value {
					t.Errorf("ParseQuad(%q) object = %#v (%T), want lexical string fallback", value, quad.Object, quad.Object)
				}
			}
			for _, value := range tt.invalid {
				if _, err := parser.ParseQuad(`<s> <p> "` + value + `"^^<http://www.w3.org/2001/XMLSchema#` + tt.datatype + `> .`); err == nil {
					t.Errorf("ParseQuad(%q) accepted a value outside %s", value, tt.datatype)
				}
			}
		})
	}
}

func TestXSDDecimalSpecialValuesStayStrings(t *testing.T) {
	for _, value := range []string{"INF", "+INF", "-INF", "NaN", "1e999"} {
		t.Run(value, func(t *testing.T) {
			quad, err := parser.ParseQuad(`<s> <p> "` + value + `"^^<http://www.w3.org/2001/XMLSchema#decimal> .`)
			if err != nil {
				t.Fatalf("ParseQuad failed: %v", err)
			}
			if got, ok := quad.Object.(string); !ok || got != value {
				t.Fatalf("object = %#v (%T), want string %q", quad.Object, quad.Object, value)
			}
		})
	}
}

func TestXSDTimeAndKeywordDatatypes(t *testing.T) {
	tests := []struct {
		datatype string
		value    string
		want     interface{}
	}{
		{"time", "12:34:56Z", parser.XSDTime("12:34:56Z")},
		{"time", "12:34:56.123+02:00", parser.XSDTime("12:34:56.123+02:00")},
		{"language", "en", parser.XSDLanguage("en")},
		{"duration", "P1Y2M", parser.XSDKeyword("P1Y2M")},
		{"base64Binary", "YWJj", parser.XSDKeyword("YWJj")},
		{"hexBinary", "AABB", parser.XSDKeyword("AABB")},
		{"QName", "ns:name", parser.XSDKeyword("ns:name")},
		{"NMTOKEN", "name-token", parser.XSDKeyword("name-token")},
	}
	for _, tt := range tests {
		t.Run(tt.datatype+"/"+tt.value, func(t *testing.T) {
			quad, err := parser.ParseQuad(`<s> <p> "` + tt.value + `"^^<http://www.w3.org/2001/XMLSchema#` + tt.datatype + `> .`)
			if err != nil {
				t.Fatalf("ParseQuad failed: %v", err)
			}
			if quad.Object != tt.want {
				t.Fatalf("object = %#v, want %#v", quad.Object, tt.want)
			}
		})
	}

	if _, err := parser.ParseQuad(`<s> <p> "25:61:00"^^<http://www.w3.org/2001/XMLSchema#time> .`); err == nil {
		t.Fatal("invalid xsd:time was accepted")
	}
}

func TestRDFJSONParsesToPlainString(t *testing.T) {
	// rdf:JSON has no dedicated representation: the lexical form is kept as a
	// plain string so a predicate can mix rdf:JSON objects with scalars and
	// still map to a single Elasticsearch field type.
	for _, value := range []string{`{"nested":{"value":1}}`, `{invalid`, `[1,2]`} {
		quad, err := parser.ParseQuad(`<s> <p> "` + strings.ReplaceAll(value, `"`, `\"`) + `"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON> .`)
		if err != nil {
			t.Fatalf("ParseQuad failed: %v", err)
		}
		if got, ok := quad.Object.(string); !ok || got != value {
			t.Fatalf("object = %#v (%T), want string %q", quad.Object, quad.Object, value)
		}
	}
}
