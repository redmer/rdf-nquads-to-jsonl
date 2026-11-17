package mapping

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/redmer/rdf-nquads-to-jsonl/parser"
)

type FieldType int

const (
	TypeUnknown FieldType = iota
	TypeBool
	TypeLong
	TypeDecimal
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
	options      Options
}

// Options controls the generated Elasticsearch mapping shape.
type Options struct {
	DecimalScalingFactor       float64
	TextKeywordSubfield        bool
	KeywordIgnoreAbove         int
	CopyTo                     string
	KeywordLowercaseNormalizer bool
	IgnoreMalformedGeo         bool
	DateSubfield               bool
}

// DefaultOptions returns the default mapping behavior.
func DefaultOptions() Options {
	return Options{
		DecimalScalingFactor: 10000,
		TextKeywordSubfield:  true,
		KeywordIgnoreAbove:   256,
		IgnoreMalformedGeo:   true,
	}
}

type fieldProfile struct {
	baseType FieldType

	// maxAbsDecimal is the largest absolute xsd:decimal value seen, used to
	// detect when scaled_float would overflow its int64 backing storage.
	maxAbsDecimal   float64
	stringCount     int
	stringTotalLen  int
	stringMaxLen    int
	langStringCount int
	langCounts      map[string]int
	hasRDFHTML      bool
}

// NewMapper creates a new Mapper. If options are omitted, DefaultOptions are used.
func NewMapper(options ...Options) *Mapper {
	opts := DefaultOptions()
	if len(options) > 0 {
		opts = options[0]
	}
	return &Mapper{
		fields:  make(map[string]*fieldProfile),
		options: opts,
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
	if d, ok := q.Object.(parser.XSDDecimal); ok {
		if a := math.Abs(float64(d)); a > profile.maxAbsDecimal {
			profile.maxAbsDecimal = a
		}
	}
}

// inferType determines the FieldType from a Go value.
func inferType(obj interface{}) (fieldType FieldType, hasStringLike bool, lang string, isRDFHTML bool, stringLen int) {
	switch obj := obj.(type) {
	case bool:
		return TypeBool, false, "", false, 0
	case int, int64:
		return TypeLong, false, "", false, 0
	case parser.XSDDecimal:
		return TypeDecimal, false, "", false, 0
	case float32, float64:
		return TypeDouble, false, "", false, 0
	case parser.Date, parser.DateTime, parser.XSDTime:
		return TypeDate, false, "", false, 0
	case parser.URI, parser.AnyURI:
		return TypeKeyword, false, "", false, 0
	case parser.XSDLanguage, parser.XSDKeyword:
		return TypeKeyword, false, "", false, 0
	case parser.GeoWKT, parser.GeoJSON:
		return TypeGeoShape, false, "", false, 0
	case parser.Markdown:
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
	"ar":    "arabic",
	"bg":    "bulgarian",
	"bn":    "bengali",
	"ca":    "catalan",
	"cs":    "czech",
	"da":    "danish",
	"de":    "german",
	"el":    "greek",
	"en":    "english",
	"es":    "spanish",
	"et":    "estonian",
	"eu":    "basque",
	"fa":    "persian",
	"fi":    "finnish",
	"fr":    "french",
	"ga":    "irish",
	"gl":    "galician",
	"hi":    "hindi",
	"hu":    "hungarian",
	"hy":    "armenian",
	"id":    "indonesian",
	"it":    "italian",
	"ja":    "cjk",
	"ko":    "cjk",
	"ku":    "sorani", // Central Kurdish
	"lt":    "lithuanian",
	"lv":    "latvian",
	"nl":    "dutch",
	"no":    "norwegian",
	"pt":    "portuguese",
	"pt-br": "brazilian",
	"ro":    "romanian",
	"ru":    "russian",
	"sr":    "serbian",
	"sv":    "swedish",
	"th":    "thai",
	"tr":    "turkish",
	"zh":    "cjk",
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
		if count > bestCount || (count == bestCount && analyzer < bestAnalyzer) {
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

	// Mixed numeric types (including decimal with integer or binary floats)
	// upgrade to Double; only decimal-only fields use scaled_float.
	if isNumericType(t1) && isNumericType(t2) {
		return TypeDouble
	}

	// Any other mix (e.g. Bool + Long) falls back to Text for safety.
	return TypeText
}

func isNumericType(t FieldType) bool {
	return t == TypeLong || t == TypeDecimal || t == TypeDouble
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

// Settings is the index-creation settings body defining the custom
// html_strip analyzers referenced by rdf:HTML fields and the copy_to
// catch-all. It is constant and data-independent: analyzer definitions can
// only be applied when an index is created (or while it is closed), so they
// cannot travel in the mapping body passed to PUT /<index>/_mapping (e.g.
// via `esbulk -mapping`).
var Settings = map[string]interface{}{
	"analysis": map[string]interface{}{
		"analyzer": map[string]interface{}{
			// html_strip strips markup from rdf:HTML source fields.
			"html_strip": map[string]interface{}{
				"type":        "custom",
				"char_filter": []interface{}{"html_strip"},
				"tokenizer":   "standard",
			},
			// catch_all_filter strips casing and HTML, as rdf:HTLM content
			// might else match tags and attributes
			"catch_all_filter": map[string]interface{}{
				"type":        "custom",
				"char_filter": []interface{}{"html_strip"},
				"tokenizer":   "standard",
				"filter":      []interface{}{"lowercase"},
			},
		},
	},
}

// Generate produces the Elasticsearch mapping JSON. The output is
// mapping-only (no top-level "settings") so it can be sent to
// PUT /<index>/_mapping, as `esbulk -mapping` does. Use GenerateConfig for a
// full index-creation body that also defines the analyzers.
func (m *Mapper) Generate() ([]byte, error) {
	properties := make(map[string]interface{})

	// Always map _id and _graph
	// _id is metadata, doesn't go in 'properties' usually, but outputting it
	// might confuse ES if it considers it a field. ES handles _id automatically.
	// _graph is a string array.
	properties["_graph"] = m.keywordMapping()

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
		case TypeDecimal:
			// scaled_float stores value * scaling_factor in an int64; larger
			// values silently saturate and collide, so fall back to double.
			if profile.maxAbsDecimal*math.Abs(m.options.DecimalScalingFactor) > math.MaxInt64 {
				mapping = map[string]interface{}{"type": "double"}
			} else {
				mapping = map[string]interface{}{
					"type":           "scaled_float",
					"scaling_factor": m.options.DecimalScalingFactor,
				}
			}
		case TypeDouble:
			mapping = map[string]interface{}{"type": "double"}
		case TypeDate:
			mapping = m.dateMapping()
			if m.options.DateSubfield {
				mapping = m.textMapping(profile)
				fields, ok := mapping["fields"].(map[string]interface{})
				if !ok {
					fields = make(map[string]interface{})
					mapping["fields"] = fields
				}
				fields["date"] = m.dateMappingWithMalformed()
			}
		case TypeKeyword:
			mapping = m.keywordMapping()
		case TypeGeoShape:
			mapping = map[string]interface{}{"type": "geo_shape"}
			if m.options.IgnoreMalformedGeo {
				mapping["ignore_malformed"] = true
			}
		case TypeText:
			mapping = m.textMapping(profile)
		default:
			// Fallback
			mapping = m.textMapping(profile)
		}
		properties[field] = mapping
	}

	if m.options.CopyTo != "" {
		if m.options.CopyTo == "_id" {
			return nil, fmt.Errorf("copy_to target %q conflicts with Elasticsearch metadata", m.options.CopyTo)
		}
		if _, exists := properties[m.options.CopyTo]; exists {
			return nil, fmt.Errorf("copy_to target %q conflicts with an inferred field", m.options.CopyTo)
		}
		targetMapping := m.textMapping(nil)
		delete(targetMapping, "copy_to")
		// rdf:HTML source fields copy their raw markup into the catch-all,
		// so it needs the markup-stripping analyzer: otherwise searches
		// there can match tag names and attribute names that the source
		// field strips.
		targetMapping["analyzer"] = "catch_all_filter"
		properties[m.options.CopyTo] = targetMapping
	}

	// Wrap in standard ES mapping structure
	// { "properties": { ... } }
	// We do NOT wrap this in "mappings" because tools like esbulk expect
	// the mapping definition directly (which starts with "properties").
	result := map[string]interface{}{
		"date_detection":    false,
		"numeric_detection": false,
		// Fields not covered by an explicit property above are dynamic.
		// Map dynamically detected strings to keyword so they stay aggregatable
		// instead of being indexed as analyzed text.
		"dynamic_templates": []interface{}{
			map[string]interface{}{
				"strings_as_keyword": map[string]interface{}{
					"match_mapping_type": "string",
					"mapping":            m.keywordMapping(),
				},
			},
		},
		"properties": properties,
	}

	return json.MarshalIndent(result, "", "  ")
}

func (m *Mapper) dateMapping() map[string]interface{} {
	return map[string]interface{}{
		"type":   "date",
		"format": "strict_date_optional_time||strict_date||strict_hour_minute_second||strict_hour_minute_second_fraction||strict_time_no_millis||strict_time||epoch_millis",
	}
}

func (m *Mapper) dateMappingWithMalformed() map[string]interface{} {
	result := m.dateMapping()
	result["ignore_malformed"] = true
	return result
}

func (m *Mapper) keywordMapping() map[string]interface{} {
	result := map[string]interface{}{"type": "keyword"}
	if m.options.KeywordIgnoreAbove > 0 {
		result["ignore_above"] = m.options.KeywordIgnoreAbove
	}
	if m.options.KeywordLowercaseNormalizer {
		result["normalizer"] = "lowercase_normalizer"
	}
	return result
}

func (m *Mapper) textMapping(profile *fieldProfile) map[string]interface{} {
	result := map[string]interface{}{"type": "text"}
	switch {
	case profile != nil && profile.hasRDFHTML:
		result["analyzer"] = "html_strip"
	case m.textAnalyzer != "":
		result["analyzer"] = m.textAnalyzer
	case profile != nil:
		if analyzer := inferAnalyzer(profile); analyzer != "" {
			result["analyzer"] = analyzer
		}
	}
	if m.options.CopyTo != "" {
		result["copy_to"] = m.options.CopyTo
	}
	if m.options.TextKeywordSubfield {
		keyword := map[string]interface{}{"type": "keyword"}
		if m.options.KeywordIgnoreAbove > 0 {
			keyword["ignore_above"] = m.options.KeywordIgnoreAbove
		}
		if m.options.KeywordLowercaseNormalizer {
			keyword["normalizer"] = "lowercase_normalizer"
		}
		fields := make(map[string]interface{})
		fields["keyword"] = keyword
		result["fields"] = fields
	}
	return result
}

// GenerateConfig produces a complete Elasticsearch index-creation body:
// the generated mapping plus the settings defining the html_strip analyzers.
// Feed it to `esbulk -c` so the analyzers are defined when the index is
// created. The settings are included unconditionally because analyzer
// definitions cannot be added to a live index, so omitting them when no
// rdf:HTML field was detected would leave no way to add such a field later
// without recreating the index.
func (m *Mapper) GenerateConfig() ([]byte, error) {
	mappingJSON, err := m.Generate()
	if err != nil {
		return nil, err
	}

	var mappingDoc map[string]interface{}
	if err := json.Unmarshal(mappingJSON, &mappingDoc); err != nil {
		return nil, fmt.Errorf("unmarshaling generated mapping: %w", err)
	}

	analysis := Settings["analysis"].(map[string]interface{})
	configAnalysis := map[string]interface{}{
		"analyzer": analysis["analyzer"],
	}
	if m.options.KeywordLowercaseNormalizer {
		configAnalysis["normalizer"] = map[string]interface{}{
			"lowercase_normalizer": map[string]interface{}{
				"type":   "custom",
				"filter": []string{"lowercase"},
			},
		}
	}
	return json.MarshalIndent(map[string]interface{}{
		"settings": map[string]interface{}{"analysis": configAnalysis},
		"mappings": mappingDoc,
	}, "", "  ")
}
