// Package indexer provides Elasticsearch bulk indexing for RDF documents.
package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/redmer/rdf-index-elasticsearch/processor"
)

// Indexer manages bulk indexing of documents into Elasticsearch.
type Indexer struct {
	client    *elasticsearch.Client
	indexName string
	buf       bytes.Buffer
	count     int
	batchSize int
}

// New creates a new Indexer.
// esURL is the Elasticsearch URL (e.g. "http://localhost:9200").
// apiKey is the Elasticsearch API key (optional).
// indexName is the target Elasticsearch index.
// batchSize is the number of documents to accumulate before flushing.
func New(esURL, indexName, apiKey string, batchSize int) (*Indexer, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{esURL},
		APIKey:    apiKey,
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch client: %w", err)
	}
	return &Indexer{
		client:    client,
		indexName: indexName,
		batchSize: batchSize,
	}, nil
}

// Add encodes a Document into the bulk buffer and flushes when the batch size is reached.
func (idx *Indexer) Add(ctx context.Context, doc processor.Document) error {
	meta := map[string]any{
		"index": map[string]any{
			"_index": idx.indexName,
			"_id":    doc.ID,
		},
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	// Build the document body from Fields
	body := make(map[string]any, len(doc.Fields))
	for k, v := range doc.Fields {
		body[k] = v
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}
	idx.buf.Write(metaBytes)
	idx.buf.WriteByte('\n')
	idx.buf.Write(bodyBytes)
	idx.buf.WriteByte('\n')
	idx.count++
	if idx.count >= idx.batchSize {
		return idx.Flush(ctx)
	}
	return nil
}

// Flush sends any buffered documents to Elasticsearch.
func (idx *Indexer) Flush(ctx context.Context) error {
	if idx.count == 0 {
		return nil
	}
	req := esapi.BulkRequest{
		Index: idx.indexName,
		Body:  strings.NewReader(idx.buf.String()),
	}
	res, err := req.Do(ctx, idx.client)
	if err != nil {
		return fmt.Errorf("bulk request: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("bulk response error: %s", res.String())
	}
	// Check for per-item errors
	var result map[string]any
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode bulk response: %w", err)
	}
	if errors, ok := result["errors"].(bool); ok && errors {
		pretty, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("bulk indexing reported errors: %v", result)
		}
		return fmt.Errorf("bulk indexing reported errors:\n%s", pretty)
	}
	idx.buf.Reset()
	idx.count = 0
	return nil
}
