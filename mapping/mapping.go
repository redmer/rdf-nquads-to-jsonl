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
	TypeGeoShape
)

// Mapper accumulates field types from N-Quads to generate an Elasticsearch mapping.
type Mapper struct {
	// fields maps the JSON property name (predicate) to the detected field profile.
	fields map[string]*fieldProfile

	// textAnalyzer forces analyzer for every inferred text field when non-empty.
	textAnalyzer string
}

type fieldProfile struct {
	baseType FieldType

	stringCount     int
	stringTotalLen  int
	stringMaxLen    int
	langStringCount int
	langCounts      map[string]int
	hasRDFHTML      bool
}

// NewMapper creates a new Mapper.
func NewMapper() *Mapper {
	return &Mapper{
		fields: make(map[string]*fieldProfile),
	}
}

// SetTextAnalyzer forces the same analyzer for all generated text fields.
func (m *Mapper) SetTextAnalyzer(analyzer string) {
	m.textAnalyzer = strings.TrimSpace(analyzer)
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

	newType, hasStringLike, lang, isRDFHTML, stringLen := inferType(q.Object)

	if hasStringLike {
		profile.stringCount++
		profile.stringTotalLen += stringLen
		if stringLen > profile.stringMaxLen {
			profile.stringMaxLen = stringLen
		}
		if lang != "" {
			profile.langStringCount++
			if profile.langCounts == nil {
				profile.langCounts = make(map[string]int)
			}
			profile.langCounts[lang]++
		}
		if isRDFHTML {
			profile.hasRDFHTML = true
		}
	}

	if newType != TypeUnknown {
		profile.baseType = resolveType(profile.baseType, newType)
	}
}

// inferType determines the FieldType from a Go value.
func inferType(obj interface{}) (fieldType FieldType, hasStringLike bool, lang string, isRDFHTML bool, stringLen int) {
	switch obj := obj.(type) {
	case bool:
		return TypeBool, false, "", false, 0
	case int, int64:
		return TypeLong, false, "", false, 0
	case float32, float64:
		return TypeDouble, false, "", false, 0
	case parser.Date, parser.DateTime:
		return TypeDate, false, "", false, 0
	case parser.URI, parser.AnyURI:
		return TypeKeyword, false, "", false, 0
	case parser.GeoWKT, parser.GeoJSON:
		return TypeGeoShape, false, "", false, 0
	case parser.TriplyMarkdown:
		return TypeText, true, "", false, len(obj)
	case parser.RDFHTML:
		return TypeText, true, "", true, len(obj)
	case parser.LangString:
		return TypeUnknown, true, obj.BaseLang(), false, len(obj.Value)
	case string:
		return TypeUnknown, true, "", false, len(obj)
	default:
		return TypeUnknown, false, "", false, 0
	}
}

var analyzerByLang = map[string]string{
	"ar": "arabic",
	"bg": "bulgarian",
	"ca": "catalan",
	"cs": "czech",
	"da": "danish",
	"de": "german",
	"el": "greek",
	"en": "english",
	"es": "spanish",
	"eu": "basque",
	"fa": "persian",
	"fi": "finnish",
	"fr": "french",
	"ga": "irish",
	"gl": "galician",
	"hi": "hindi",
	"hu": "hungarian",
	"hy": "armenian",
	"id": "indonesian",
	"it": "italian",
	"lt": "lithuanian",
	"lv": "latvian",
	"nl": "dutch",
	"no": "norwegian",
	"pt": "portuguese",
	"ro": "romanian",
	"ru": "russian",
	"sv": "swedish",
	"th": "thai",
	"tr": "turkish",
	"zh": "chinese",
}

func inferAnalyzer(profile *fieldProfile) string {
	if profile.langStringCount == 0 || len(profile.langCounts) == 0 {
		return ""
	}

	avgLen := float64(profile.stringTotalLen) / float64(profile.stringCount)
	if avgLen < 10 {
		return ""
	}

	totalKnown := 0
	bestAnalyzer := ""
	bestCount := 0
	for lang, count := range profile.langCounts {
		analyzer, ok := analyzerByLang[lang]
		if !ok {
			continue
		}
		totalKnown += count
		if count > bestCount {
			bestCount = count
			bestAnalyzer = analyzer
		}
	}

	if totalKnown == 0 || bestCount == 0 {
		return ""
	}

	if float64(bestCount)/float64(totalKnown) < 0.5 {
		return ""
	}

	return bestAnalyzer
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

	if t1 == TypeGeoShape || t2 == TypeGeoShape {
		if t1 == TypeUnknown {
			return t2
		}
		if t2 == TypeUnknown {
			return t1
		}
		if t1 == TypeGeoShape && t2 == TypeGeoShape {
			return TypeGeoShape
		}
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
	if profile.stringCount >= 5 && avgLen <= 16 && profile.stringMaxLen <= 64 {
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
		case TypeGeoShape:
			mapping = map[string]interface{}{"type": "geo_shape"}
		case TypeText:
			mapping = map[string]interface{}{"type": "text"}
			if m.textAnalyzer != "" {
				mapping["analyzer"] = m.textAnalyzer
			} else if profile.hasRDFHTML {
				mapping["analyzer"] = "html_strip"
			} else if analyzer := inferAnalyzer(profile); analyzer != "" {
				mapping["analyzer"] = analyzer
			}
		default:
			// Fallback
			mapping = map[string]interface{}{"type": "text"}
			if m.textAnalyzer != "" {
				mapping["analyzer"] = m.textAnalyzer
			} else if analyzer := inferAnalyzer(profile); analyzer != "" {
				mapping["analyzer"] = analyzer
			}
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
