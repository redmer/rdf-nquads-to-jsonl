# rdf-nquads-to-jsonl

Transforms a pre-sorted N-Quads stream into JSONL documents, ready to be loaded into ElasticSearch.

## Requirements

- a pre-sorted N-Quads stream from stdin (e.g., `zcat data.nq.gz | LC_ALL=C sort`)
- a bulk loader for ElasticSearch (e.g., [`esbulk`])
- the N-Quads stream must not contain blank nodes. Use skolemization beforehand.

[esbulk]: https://github.com/miku/esbulk

## Installation

Ensure Go is installed, then compile from source: `go install github.com/redmer/rdf-nquads-to-jsonl@latest`.

## Usage

Use `rdf-nquads-to-jsonl` in a pipeline with connected stdin and stdout.

```
$ rdf-nquads-to-jsonl -help
Usage of rdf-nquads-to-jsonl:
  -exclude string
        Comma-separated list of graph URIs to exclude (blocklist)
  -generate-mapping
        Generate Elasticsearch mapping from input
  -include string
        Comma-separated list of graph URIs to include (allowlist)
```

Demonstrating output: downloading a Gzipped NQuads file, unzipping, sorting and sending colorizing output to less:

```bash
curl -sL https://datasets.crow.nl/crow/thesaurus/download.nq.gz | zcat | LC_ALL=C sort | rdf-nquads-to-jsonl | jq -c '.' | less
```

<details><summary>Output: example line</summary>

```json
{
  "_id": "https://data.crow.nl/thesaurus/term/ffab94d4-59aa-4c4a-b6fb-b42807f94515",
  "http://purl org/dc/terms/created": ["2023-09-22"],
  "http://purl org/dc/terms/source": [
    "https://data.crow.nl/thesaurus/term/e6ba7a92-73c7-43ea-a90a-7c54e3d3c474"
  ],
  "http://www w3 org/1999/02/22-rdf-syntax-ns#type": [
    "http://www.w3.org/2004/02/skos/core#Concept"
  ],
  "http://www w3 org/2004/02/skos/core#broader": [
    "https://data.crow.nl/thesaurus/term/D01A33D2-330A-49CB-A23C-522E0D877BD3"
  ],
  "http://www w3 org/2004/02/skos/core#definition": [
    "Inwendige wapening is betonstaal dat in de te versterken betonconstructie aanwezig is."
  ],
  "http://www w3 org/2004/02/skos/core#inScheme": [
    "https://data.crow.nl/thesaurus/term/conceptScheme_d315be96"
  ],
  "http://www w3 org/2004/02/skos/core#prefLabel": ["inwendige wapening"]
}
```

</details>

A more typical example, sending to a ElasticSearch server with an API key:

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
- Language-tagged strings become plain strings.
- Numbers and boolean datatype become JSON numbers and booleans. All other strings with a datatype become plain strings.

## Development

```sh
go test ./...
go build .
```
