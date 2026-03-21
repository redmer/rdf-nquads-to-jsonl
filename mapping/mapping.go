package mapping

import (
	"encoding/json"
	"strings"

	"github.com/redmer/rdf-nquads-to-jsonl/parser"
)

type FieldType int

const (
	TypeUnknown FieldType = iota
	TypeBool
	TypeLong
	TypeDouble
	TypeText
)

// Mapper accumulates field types from N-Quads to generate an Elasticsearch mapping.
type Mapper struct {
	// fields maps the JSON property name (predicate) to the detected field type.
	fields map[string]FieldType
}

// NewMapper creates a new Mapper.
func NewMapper() *Mapper {
	return &Mapper{
		fields: make(map[string]FieldType),
	}
}

// Add processes a quad and updates the type inference for the predicate.
func (m *Mapper) Add(q parser.Quad) {
	// Transform predicate to JSON key (same logic as processor)
	key := strings.ReplaceAll(q.Predicate, ".", " ")

	newType := inferType(q.Object)
	currentType, exists := m.fields[key]

	if !exists {
		m.fields[key] = newType
	} else {
		m.fields[key] = resolveType(currentType, newType)
	}
}

// inferType determines the FieldType from a Go value.
func inferType(obj interface{}) FieldType {
	switch obj.(type) {
	case bool:
		return TypeBool
	case int, int64:
		return TypeLong
	case float32, float64:
		return TypeDouble
	default:
		return TypeText
	}
}

// resolveType determines the common type that can hold both t1 and t2.
func resolveType(t1, t2 FieldType) FieldType {
	if t1 == t2 {
		return t1
	}
	if t1 == TypeText || t2 == TypeText {
		return TypeText
	}
	// If one is Unknown (shouldn't happen if initialized properly), take the other.
	if t1 == TypeUnknown {
		return t2
	}
	if t2 == TypeUnknown {
		return t1
	}

	// Mixed numeric types upgrade to Double.
	if (t1 == TypeLong && t2 == TypeDouble) || (t1 == TypeDouble && t2 == TypeLong) {
		return TypeDouble
	}

	// Any other mix (e.g. Bool + Long) falls back to Text for safety.
	return TypeText
}

// Generate produces the Elasticsearch mapping JSON.
func (m *Mapper) Generate() ([]byte, error) {
	properties := make(map[string]interface{})

	// Always map _id and _graph
	// _id is metadata, doesn't go in 'properties' usually, but outputting it
	// might confuse ES if it considers it a field. ES handles _id automatically.
	// _graph is a string array.
	properties["_graph"] = map[string]interface{}{
		"type": "keyword",
	}

	for field, fieldType := range m.fields {
		var mapping map[string]interface{}
		switch fieldType {
		case TypeBool:
			mapping = map[string]interface{}{"type": "boolean"}
		case TypeLong:
			mapping = map[string]interface{}{"type": "long"}
		case TypeDouble:
			mapping = map[string]interface{}{"type": "double"}
		case TypeText:
			mapping = map[string]interface{}{
				"type": "text",
				"fields": map[string]interface{}{
					"keyword": map[string]interface{}{
						"type":         "keyword",
						"ignore_above": 256,
					},
				},
			}
		default:
			// Fallback
			mapping = map[string]interface{}{
				"type": "text",
				"fields": map[string]interface{}{
					"keyword": map[string]interface{}{
						"type":         "keyword",
						"ignore_above": 256,
					},
				},
			}
		}
		properties[field] = mapping
	}

	// Wrap in standard ES mapping structure
	// { "mappings": { "properties": { ... } } }
	result := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": properties,
		},
	}

	return json.MarshalIndent(result, "", "  ")
}
