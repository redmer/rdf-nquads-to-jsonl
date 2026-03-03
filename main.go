package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"

	"github.com/redmer/rdf-nquads-to-jsonl/parser"
	"github.com/redmer/rdf-nquads-to-jsonl/processor"
)

func main() {
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
			log.Fatalf("reading stdin: %v", err)
		}

		line = strings.TrimSpace(line)
		if len(line) > 0 {
			quad, parseErr := parser.ParseQuad(line)
			if parseErr != nil {
				// Skip unparseable lines (comments, empty lines, etc.)
			} else {
				grouper.Add(quad)
			}
		}

		if err == io.EOF {
			break
		}
	}
	if indexErr != nil {
		log.Fatalf("indexing error: %v", indexErr)
	}

	grouper.Flush()

	// check if flush produced error
	if indexErr != nil {
		log.Fatalf("indexing error: %v", indexErr)
	}
}
