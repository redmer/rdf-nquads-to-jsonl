# rdf-nquads-to-jsonl

Transforms a pre-sorted N-Quads stream into JSONL documents, ready to be loaded into ElasticSearch.

## Requirements

- a pre-sorted N-Quads stream from stdin (e.g. `zcat data.nq.gz | LC_ALL=C sort`)
- a bulk loader for ElasticSearch ([esbulk])

[esbulk]: https://github.com/miku/esbulk

## Installation

1. Compile from source: `go install github.com/redmer/rdf-nquads-to-jsonl`

## Usage

```sh
zcat data.nq.gz | LC_ALL=C sort | rdf-nquads-to-jsonl | esbulk -server http://localhost:9200 -index my-index2 -apikey '...' -id _id
```

## Document Format

Each Subject becomes one Elasticsearch document:

```json
{
  "_id": "https://example.com/person/123",
  "http://schema org/name": ["John Doe"],
  "http://schema org/knows": ["https://example.com/person/456"]
}
```

- The `_id` is the Subject URI.
- Predicate URIs are used as field keys with `.` replaced by ` ` (space) -- periods are special in ElasticSearch fields.
- All values are arrays of strings.

## Development

```sh
go test ./...
go build .
```
