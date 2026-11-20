package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"

	"github.com/redmer/rdf-nquads-to-jsonl/mapping"
	"github.com/redmer/rdf-nquads-to-jsonl/parser"
	"github.com/redmer/rdf-nquads-to-jsonl/processor"
)

var generateMapping = flag.Bool("generate-mapping", false, "Generate Elasticsearch mapping from input")
var generateConfig = flag.Bool("generate-config", false, "Generate Elasticsearch index-creation body (settings + mappings) from input, for use with esbulk -c")
var includeGraphs = flag.String("include", "", "Comma-separated list of graph URIs to include (allowlist)")
var excludeGraphs = flag.String("exclude", "", "Comma-separated list of graph URIs to exclude (blocklist)")
var textAnalyzer = flag.String("text-analyzer", "", "Elasticsearch analyzer to apply to inferred text fields (overrides language-based inference)")
var idMaxBytes = flag.Int("id-max-bytes", processor.DefaultMaxIDBytes, fmt.Sprintf("Maximum byte length of emitted document _id values; longer Subject URIs are truncated with a SHA-256 suffix. 0 disables the limit; values below %d are rejected", processor.MinMaxIDBytes))
var decimalScalingFactor = flag.Float64("decimal-scaling-factor", 10000, "Scaling factor for Elasticsearch scaled_float fields inferred from xsd:decimal")
var textKeywordSubfield = flag.Bool("text-keyword-subfield", true, "Add a keyword sub-field to inferred text fields")
var keywordIgnoreAbove = flag.Int("keyword-ignore-above", 8191, "Maximum keyword value length before Elasticsearch skips indexing; 0 omits ignore_above")
var copyTo = flag.String("copy-to", "", "Add copy_to to inferred text fields and create this catch-all text field")
var keywordLowercaseNormalizer = flag.Bool("keyword-lowercase-normalizer", false, "Apply a lowercase normalizer to keyword fields and text keyword sub-fields")
var ignoreMalformedGeo = flag.Bool("ignore-malformed-geo", true, "Skip malformed geo_shape values instead of rejecting the document")
var dateSubfield = flag.Bool("date-subfield", false, "Map date fields as text with an ignore_malformed date sub-field")

func main() {
	flag.Parse()

	switch {
	case *generateMapping && *generateConfig:
		log.Fatal("-generate-mapping and -generate-config are mutually exclusive")
	case *generateMapping:
		if err := RunMapping(false); err != nil {
			log.Fatalf("mapping error: %v", err)
		}
	case *generateConfig:
		if err := RunMapping(true); err != nil {
			log.Fatalf("config error: %v", err)
		}
	default:
		includes := parseGraphList(*includeGraphs)
		excludes := parseGraphList(*excludeGraphs)
		if err := RunProcessor(includes, excludes); err != nil {
			log.Fatalf("processing error: %v", err)
		}
	}
}

func parseGraphList(list string) map[string]bool {
	res := make(map[string]bool)
	if list == "" {
		return res
	}
	for _, s := range strings.Split(list, ",") {
		res[strings.TrimSpace(s)] = true
	}
	return res
}

func RunMapping(config bool) error {
	if math.IsNaN(*decimalScalingFactor) || math.IsInf(*decimalScalingFactor, 0) || *decimalScalingFactor <= 0 {
		return fmt.Errorf("invalid -decimal-scaling-factor %g: must be greater than 0", *decimalScalingFactor)
	}
	if *keywordIgnoreAbove < 0 {
		return fmt.Errorf("invalid -keyword-ignore-above %d: use 0 to omit ignore_above, or a positive value", *keywordIgnoreAbove)
	}

	mapper := mapping.NewMapper(mapping.Options{
		DecimalScalingFactor:       *decimalScalingFactor,
		TextKeywordSubfield:        *textKeywordSubfield,
		KeywordIgnoreAbove:         *keywordIgnoreAbove,
		CopyTo:                     strings.TrimSpace(*copyTo),
		KeywordLowercaseNormalizer: *keywordLowercaseNormalizer,
		IgnoreMalformedGeo:         *ignoreMalformedGeo,
		DateSubfield:               *dateSubfield,
	})
	mapper.SetTextAnalyzer(*textAnalyzer)
	reader := bufio.NewReader(os.Stdin)

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("reading stdin: %w", err)
		}

		line = strings.TrimSpace(line)
		if len(line) > 0 {
			quad, parseErr := parser.ParseQuad(line)
			if parseErr == nil {
				mapper.Add(quad)
			}
		}

		if err == io.EOF {
			break
		}
	}

	var result []byte
	var err error
	if config {
		result, err = mapper.GenerateConfig()
	} else {
		result, err = mapper.Generate()
	}
	if err != nil {
		return fmt.Errorf("generating output: %w", err)
	}

	_, err = os.Stdout.Write(result)
	return err
}

func RunProcessor(includes, excludes map[string]bool) error {
	if *idMaxBytes < 0 || (*idMaxBytes > 0 && *idMaxBytes < processor.MinMaxIDBytes) {
		return fmt.Errorf("invalid -id-max-bytes %d: use 0 to disable the limit, or a value of at least %d bytes (separator + 64-char SHA-256 suffix)", *idMaxBytes, processor.MinMaxIDBytes)
	}

	var indexErr error

	grouper := processor.NewGrouper(func(doc processor.Document) {
		if indexErr != nil {
			return
		}
		b, err := json.Marshal(doc)
		if err != nil {
			indexErr = err
			return
		}
		// Write valid JSONL
		os.Stdout.Write(append(b, '\n'))
	})
	grouper.SetMaxIDBytes(*idMaxBytes)

	reader := bufio.NewReader(os.Stdin)

	for {
		if indexErr != nil {
			break
		}
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("reading stdin: %w", err)
		}

		line = strings.TrimSpace(line)
		if len(line) > 0 {
			quad, parseErr := parser.ParseQuad(line)
			if parseErr == nil {
				// Graph filter logic: by default, include all graphs.
				if len(includes) > 0 {
					if !includes[quad.Graph] {
						continue
					}
				}
				if excludes[quad.Graph] {
					continue
				}

				grouper.Add(quad)
			}
		}

		if err == io.EOF {
			break
		}
	}

	if indexErr != nil {
		return indexErr
	}

	grouper.Flush()

	if indexErr != nil {
		return indexErr
	}
	return nil
}
