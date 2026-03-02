package parser_test

import (
	"testing"

	"github.com/redmer/rdf-index-elasticsearch/parser"
)

func TestParseQuad(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		wantSubj  string
		wantPred  string
		wantObj   string
		wantErr   bool
	}{
		{
			name:     "URI object",
			line:     `<https://example.com/person/123> <http://schema.org/knows> <https://example.com/person/456> <https://example.com/graph> .`,
			wantSubj: "https://example.com/person/123",
			wantPred: "http://schema.org/knows",
			wantObj:  "https://example.com/person/456",
		},
		{
			name:     "plain literal",
			line:     `<https://example.com/person/123> <http://schema.org/name> "John Doe" <https://example.com/graph> .`,
			wantSubj: "https://example.com/person/123",
			wantPred: "http://schema.org/name",
			wantObj:  "John Doe",
		},
		{
			name:     "literal with language tag",
			line:     `<https://example.com/person/123> <http://schema.org/name> "John Doe"@en <https://example.com/graph> .`,
			wantSubj: "https://example.com/person/123",
			wantPred: "http://schema.org/name",
			wantObj:  "John Doe",
		},
		{
			name:     "literal with datatype",
			line:     `<https://example.com/person/123> <http://schema.org/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> <https://example.com/graph> .`,
			wantSubj: "https://example.com/person/123",
			wantPred: "http://schema.org/age",
			wantObj:  "42",
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
			name:     "literal with escaped quote",
			line:     `<https://example.com/s> <https://example.com/p> "He said \"hello\"" .`,
			wantSubj: "https://example.com/s",
			wantPred: "https://example.com/p",
			wantObj:  `He said "hello"`,
		},
	}

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
				t.Errorf("Object = %q, want %q", quad.Object, tt.wantObj)
			}
		})
	}
}
