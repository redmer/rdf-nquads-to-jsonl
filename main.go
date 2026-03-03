package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"

	"github.com/redmer/rdf-index-elasticsearch/parser"
	"github.com/redmer/rdf-index-elasticsearch/processor"
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

	scanner := bufio.NewScanner(os.Stdin)
	// Increase buffer size to handle long lines
	const maxBuf = 1024 * 1024
	buf := make([]byte, maxBuf)
	scanner.Buffer(buf, maxBuf)

	for scanner.Scan() {
		if indexErr != nil {
			break
		}
		line := scanner.Text()
		quad, err := parser.ParseQuad(line)
		if err != nil {
			// Skip unparseable lines (comments, empty lines, etc.)
			continue
		}
		grouper.Add(quad)
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("reading stdin: %v", err)
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
