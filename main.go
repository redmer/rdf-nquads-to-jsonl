package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/redmer/rdf-index-elasticsearch/indexer"
	"github.com/redmer/rdf-index-elasticsearch/parser"
	"github.com/redmer/rdf-index-elasticsearch/processor"
)

const defaultBatchSize = 500

func main() {
	esURL := os.Getenv("ES_URL")
	if esURL == "" {
		esURL = "http://localhost:9200"
	}
	esIndex := os.Getenv("ES_INDEX")
	if esIndex == "" {
		log.Fatal("ES_INDEX environment variable is required")
	}

	idx, err := indexer.New(esURL, esIndex, defaultBatchSize)
	if err != nil {
		log.Fatalf("create indexer: %v", err)
	}

	ctx := context.Background()
	var indexErr error

	grouper := processor.NewGrouper(func(doc processor.Document) {
		if indexErr != nil {
			return
		}
		if err := idx.Add(ctx, doc); err != nil {
			indexErr = fmt.Errorf("index document %q: %w", doc.ID, err)
		}
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

	if err := idx.Flush(ctx); err != nil {
		log.Fatalf("final flush: %v", err)
	}

	log.Println("Indexing complete.")
}
