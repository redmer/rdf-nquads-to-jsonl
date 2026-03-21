package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/redmer/rdf-nquads-to-jsonl/mapping"
	"github.com/redmer/rdf-nquads-to-jsonl/parser"
	"github.com/redmer/rdf-nquads-to-jsonl/processor"
)

var generateMapping = flag.Bool("generate-mapping", false, "Generate Elasticsearch mapping from input")
var includeGraphs = flag.String("include", "", "Comma-separated list of graph URIs to include (allowlist)")
var excludeGraphs = flag.String("exclude", "", "Comma-separated list of graph URIs to exclude (blocklist)")

func main() {
	flag.Parse()

	if *generateMapping {
		if err := RunMapping(); err != nil {
			log.Fatalf("mapping error: %v", err)
		}
	} else {
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

func RunMapping() error {
	mapper := mapping.NewMapper()
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

	result, err := mapper.Generate()
	if err != nil {
		return fmt.Errorf("generating mapping: %w", err)
	}

	_, err = os.Stdout.Write(result)
	return err
}

func RunProcessor(includes, excludes map[string]bool) error {
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
