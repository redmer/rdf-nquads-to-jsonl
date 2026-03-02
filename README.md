# rdf-index-elasticsearch

Transform and load an RDF file (`.nq.gz`) into an Elasticsearch server.

Reads a pre-sorted N-Quads stream from `stdin`, groups triples by Subject, and bulk-indexes them as JSON documents into Elasticsearch.

## Usage

```sh
zcat data.nq.gz | LC_ALL=C sort | ES_URL=http://localhost:9200 ES_INDEX=my-index rdf-index-elasticsearch
```

### Environment Variables

| Variable   | Default                  | Description                     |
|------------|---------------------------|---------------------------------|
| `ES_URL`   | `http://localhost:9200`  | Elasticsearch URL               |
| `ES_INDEX` | *(required)*             | Target Elasticsearch index name |
| `ES_API_KEY` | *(optional)*           | Elasticsearch API Key           |

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
- Predicate URIs are used as field keys with `.` replaced by ` ` (space).
- All values are arrays of strings.

## Docker

```sh
docker build -t rdf-index-elasticsearch .
zcat data.nq.gz | LC_ALL=C sort | docker run --rm -i \
  -e ES_URL=http://host.docker.internal:9200 \
  -e ES_INDEX=my-index \
  rdf-index-elasticsearch
```

## Development

```sh
go test ./...
go build .
```
