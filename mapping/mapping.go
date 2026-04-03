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
	TypeDate
	TypeText
	TypeKeyword
)

// Mapper accumulates field types from N-Quads to generate an Elasticsearch mapping.
type Mapper struct {
	// fields maps the JSON property name (predicate) to the detected field profile.
	fields map[string]*fieldProfile
}

type fieldProfile struct {
	baseType FieldType

	stringCount     int
	stringTotalLen  int
	stringMaxLen    int
	langStringCount int
}

// NewMapper creates a new Mapper.
func NewMapper() *Mapper {
	return &Mapper{
		fields: make(map[string]*fieldProfile),
	}
}

// Add processes a quad and updates the type inference for the predicate.
func (m *Mapper) Add(q parser.Quad) {
	// Transform predicate to JSON key (same logic as processor)
	key := strings.ReplaceAll(q.Predicate, ".", " ")

	profile, exists := m.fields[key]
	if !exists {
		profile = &fieldProfile{baseType: TypeUnknown}
		m.fields[key] = profile
	}

	newType, hasStringLike, isLangString, stringLen := inferType(q.Object)

	if hasStringLike {
		profile.stringCount++
		profile.stringTotalLen += stringLen
		if stringLen > profile.stringMaxLen {
			profile.stringMaxLen = stringLen
		}
		if isLangString {
			profile.langStringCount++
		}
	}

	if newType != TypeUnknown {
		profile.baseType = resolveType(profile.baseType, newType)
	}
}

// inferType determines the FieldType from a Go value.
func inferType(obj interface{}) (fieldType FieldType, hasStringLike bool, isLangString bool, stringLen int) {
	switch obj := obj.(type) {
	case bool:
		return TypeBool, false, false, 0
	case int, int64:
		return TypeLong, false, false, 0
	case float32, float64:
		return TypeDouble, false, false, 0
	case parser.Date, parser.DateTime:
		return TypeDate, false, false, 0
	case parser.URI, parser.AnyURI:
		return TypeKeyword, false, false, 0
	case parser.LangString:
		return TypeUnknown, true, true, len(obj)
	case string:
		return TypeUnknown, true, false, len(obj)
	default:
		return TypeUnknown, false, false, 0
	}
}

// resolveType determines the common type that can hold both t1 and t2.
func resolveType(t1, t2 FieldType) FieldType {
	if t1 == t2 {
		return t1
	}
	if t1 == TypeUnknown {
		return t2
	}
	if t2 == TypeUnknown {
		return t1
	}
	if t1 == TypeText || t2 == TypeText {
		return TypeText
	}

	// Mixed numeric types upgrade to Double.
	if (t1 == TypeLong && t2 == TypeDouble) || (t1 == TypeDouble && t2 == TypeLong) {
		return TypeDouble
	}

	// Any other mix (e.g. Bool + Long) falls back to Text for safety.
	return TypeText
}

func inferStringFieldType(profile *fieldProfile) FieldType {
	if profile.stringCount == 0 {
		return TypeUnknown
	}

	// Language-tagged literals are almost always natural language and should be analyzed.
	if profile.langStringCount > 0 {
		return TypeText
	}

	avgLen := float64(profile.stringTotalLen) / float64(profile.stringCount)

	// Favor text by default to avoid accidentally classifying natural language as keyword.
	if avgLen >= 20 || profile.stringMaxLen > 128 {
		return TypeText
	}

	// Only infer keyword for consistently short literals when there is enough evidence.
	if profile.stringCount >= 5 && avgLen <= 16 && profile.stringMaxLen <= 32 {
		return TypeKeyword
	}

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

	for field, profile := range m.fields {
		var mapping map[string]interface{}

		fieldType := profile.baseType
		stringType := inferStringFieldType(profile)
		if stringType != TypeUnknown {
			fieldType = resolveType(fieldType, stringType)
		}

		switch fieldType {
		case TypeBool:
			mapping = map[string]interface{}{"type": "boolean"}
		case TypeLong:
			mapping = map[string]interface{}{"type": "long"}
		case TypeDouble:
			mapping = map[string]interface{}{"type": "double"}
		case TypeDate:
			mapping = map[string]interface{}{
				"type":   "date",
				"format": "strict_date_optional_time||strict_date",
			}
		case TypeKeyword:
			mapping = map[string]interface{}{"type": "keyword"}
		case TypeText:
			mapping = map[string]interface{}{"type": "text"}
		default:
			// Fallback
			mapping = map[string]interface{}{"type": "text"}
		}
		properties[field] = mapping
	}

	// Wrap in standard ES mapping structure
	// { "properties": { ... } }
	// We do NOT wrap this in "mappings" because tools like esbulk expect
	// the mapping definition directly (which starts with "properties").
	result := map[string]interface{}{
		"properties": properties,
	}

	return json.MarshalIndent(result, "", "  ")
}
