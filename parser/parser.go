// Package parser provides N-Quad line parsing into Subject, Predicate, and Object.
package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Quad holds the parsed components of an N-Quad line.
type Quad struct {
	Subject   string
	Predicate string
	Object    interface{}
}

// ParseQuad parses a single N-Quad line and returns a Quad.
// It ignores the graph IRI (fourth field) and the trailing dot.
// Returns an error for empty lines, comments, or malformed input.
func ParseQuad(line string) (Quad, error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return Quad{}, fmt.Errorf("skip: %q", line)
	}

	subjVal, rest, err := parseField(line)
	if err != nil {
		return Quad{}, fmt.Errorf("subject: %w", err)
	}
	subj, ok := subjVal.(string)
	if !ok {
		return Quad{}, fmt.Errorf("subject must be a URI, got %T", subjVal)
	}

	predVal, rest, err := parseField(strings.TrimSpace(rest))
	if err != nil {
		return Quad{}, fmt.Errorf("predicate: %w", err)
	}
	pred, ok := predVal.(string)
	if !ok {
		return Quad{}, fmt.Errorf("predicate must be a URI, got %T", predVal)
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
func parseField(s string) (value interface{}, rest string, err error) {
	if len(s) == 0 {
		return nil, "", fmt.Errorf("empty input")
	}
	switch s[0] {
	case '<':
		return parseURI(s)
	case '"':
		return parseLiteral(s)
	default:
		return nil, "", fmt.Errorf("unexpected character %q", s[0])
	}
}

// parseURI reads a URI enclosed in <> and returns the URI value and remainder.
func parseURI(s string) (value interface{}, rest string, err error) {
	// s starts with '<'
	end := strings.Index(s[1:], ">")
	if end < 0 {
		return nil, "", fmt.Errorf("unterminated URI: %q", s)
	}
	return s[1 : end+1], s[end+2:], nil
}

// parseLiteral reads a quoted literal and returns the unescaped string value plus the remainder.
// The remainder may include @lang or ^^<datatype> which are consumed and discarded.
func parseLiteral(s string) (value interface{}, rest string, err error) {
	// s starts with '"'
	var buf strings.Builder
	i := 1
	for i < len(s) {
		c := s[i]
		switch c {
		case '\\':
			i++
			if i >= len(s) {
				return nil, "", fmt.Errorf("unterminated escape in literal")
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

			rawVal := buf.String()
			var datatype string

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
				tempRest := strings.TrimPrefix(rest, "^^")
				tempRest = strings.TrimSpace(tempRest)
				if len(tempRest) > 0 && tempRest[0] == '<' {
					var uriVal interface{}
					uriVal, rest, err = parseURI(tempRest)
					if err != nil {
						return nil, "", fmt.Errorf("datatype URI: %w", err)
					}
					datatype = uriVal.(string)
				}
			}

			if datatype != "" {
				switch datatype {
				case "http://www.w3.org/2001/XMLSchema#boolean":
					b, err := strconv.ParseBool(rawVal)
					if err != nil {
						return nil, "", fmt.Errorf("invalid boolean: %q", rawVal)
					}
					return b, rest, nil
				case "http://www.w3.org/2001/XMLSchema#integer", "http://www.w3.org/2001/XMLSchema#int":
					n, err := strconv.ParseInt(rawVal, 10, 64)
					if err != nil {
						return nil, "", fmt.Errorf("invalid integer: %q", rawVal)
					}
					return n, rest, nil
				case "http://www.w3.org/2001/XMLSchema#decimal", "http://www.w3.org/2001/XMLSchema#double", "http://www.w3.org/2001/XMLSchema#float":
					f, err := strconv.ParseFloat(rawVal, 64)
					if err != nil {
						return nil, "", fmt.Errorf("invalid decimal: %q", rawVal)
					}
					return f, rest, nil
				}
			}

			return rawVal, rest, nil
		default:
			buf.WriteByte(c)
		}
		i++
	}
	return nil, "", fmt.Errorf("unterminated literal: %q", s)
}
