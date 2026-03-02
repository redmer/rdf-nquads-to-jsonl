// Package parser provides N-Quad line parsing into Subject, Predicate, and Object.
package parser

import (
	"fmt"
	"strings"
)

// Quad holds the parsed components of an N-Quad line.
type Quad struct {
	Subject   string
	Predicate string
	Object    string
}

// ParseQuad parses a single N-Quad line and returns a Quad.
// It ignores the graph IRI (fourth field) and the trailing dot.
// Returns an error for empty lines, comments, or malformed input.
func ParseQuad(line string) (Quad, error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return Quad{}, fmt.Errorf("skip: %q", line)
	}

	subj, rest, err := parseField(line)
	if err != nil {
		return Quad{}, fmt.Errorf("subject: %w", err)
	}
	pred, rest, err := parseField(strings.TrimSpace(rest))
	if err != nil {
		return Quad{}, fmt.Errorf("predicate: %w", err)
	}
	obj, _, err := parseField(strings.TrimSpace(rest))
	if err != nil {
		return Quad{}, fmt.Errorf("object: %w", err)
	}

	return Quad{Subject: subj, Predicate: pred, Object: obj}, nil
}

// parseField reads one N-Quad field from s and returns the parsed value and the remainder.
// Fields can be:
//   - <URI>            → returns URI string
//   - "literal"        → returns literal string (handles @lang and ^^<datatype>)
func parseField(s string) (value, rest string, err error) {
	if len(s) == 0 {
		return "", "", fmt.Errorf("empty input")
	}
	switch s[0] {
	case '<':
		return parseURI(s)
	case '"':
		return parseLiteral(s)
	default:
		return "", "", fmt.Errorf("unexpected character %q", s[0])
	}
}

// parseURI reads a URI enclosed in <> and returns the URI value and remainder.
func parseURI(s string) (value, rest string, err error) {
	// s starts with '<'
	end := strings.Index(s[1:], ">")
	if end < 0 {
		return "", "", fmt.Errorf("unterminated URI: %q", s)
	}
	return s[1 : end+1], s[end+2:], nil
}

// parseLiteral reads a quoted literal and returns the unescaped string value plus the remainder.
// The remainder may include @lang or ^^<datatype> which are consumed and discarded.
func parseLiteral(s string) (value, rest string, err error) {
	// s starts with '"'
	var buf strings.Builder
	i := 1
	for i < len(s) {
		c := s[i]
		switch c {
		case '\\':
			i++
			if i >= len(s) {
				return "", "", fmt.Errorf("unterminated escape in literal")
			}
			switch s[i] {
			case '"':
				buf.WriteByte('"')
			case '\\':
				buf.WriteByte('\\')
			case 'n':
				buf.WriteByte('\n')
			case 'r':
				buf.WriteByte('\r')
			case 't':
				buf.WriteByte('\t')
			default:
				buf.WriteByte('\\')
				buf.WriteByte(s[i])
			}
		case '"':
			// End of literal content
			rest := s[i+1:]
			rest = strings.TrimSpace(rest)
			// Consume optional @lang or ^^<datatype>
			if strings.HasPrefix(rest, "@") {
				// consume up to next whitespace
				end := strings.IndexAny(rest, " \t")
				if end < 0 {
					rest = ""
				} else {
					rest = rest[end:]
				}
			} else if strings.HasPrefix(rest, "^^") {
				// consume ^^<datatype>
				rest = strings.TrimPrefix(rest, "^^")
				rest = strings.TrimSpace(rest)
				if len(rest) > 0 && rest[0] == '<' {
					_, rest, err = parseURI(rest)
					if err != nil {
						return "", "", fmt.Errorf("datatype URI: %w", err)
					}
				}
			}
			return buf.String(), rest, nil
		default:
			buf.WriteByte(c)
		}
		i++
	}
	return "", "", fmt.Errorf("unterminated literal: %q", s)
}
