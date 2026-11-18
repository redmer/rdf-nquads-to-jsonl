# rdf-nquads-to-jsonl

Transforms a pre-sorted N-Quads stream into JSONL documents, ready to be loaded into ElasticSearch.

## Requirements

- a pre-sorted N-Quads stream from stdin (e.g., `zcat data.nq.gz | LC_ALL=C sort`)
- a bulk loader for ElasticSearch (e.g., [`esbulk`])

> [!CAUTION]
> The N-Quads source must not contain blank nodes. Use skolemization beforehand.

[esbulk]: https://github.com/miku/esbulk

## Installation

Ensure Go is installed, then compile from source: `go install github.com/redmer/rdf-nquads-to-jsonl@latest`.

## Usage

Use `rdf-nquads-to-jsonl` in a pipeline with connected stdin and stdout.

Generally, use the options `-generate-config` and `-generate-mapping` to generate index settings and a mapping.
The config and the mapping are based on the shape of the quad stream.
Then, stream in the quads and connect stdout for all ES documents as JSONL.

This tool can be used for zero-downtime reindexing: create the new index with `esbulk -c`, bulk-load, then reassign the alias.
`esbulk -c` is a no-op when the index already exists, so the settings never overwrite a live index.

<details>
<summary><code>$ rdf-nquads-to-jsonl -help</code></summary>

```
$ rdf-nquads-to-jsonl -help
Usage of rdf-nquads-to-jsonl:
  -copy-to string
        Add copy_to to inferred text fields and create this catch-all text field
  -date-subfield
        Map date fields as text with an ignore_malformed date sub-field
  -decimal-scaling-factor float
        Scaling factor for Elasticsearch scaled_float fields inferred from xsd:decimal (default 10000)
  -exclude string
        Comma-separated list of graph URIs to exclude (blocklist)
  -generate-config
        Generate Elasticsearch index-creation body (settings + mappings) from input, for use with esbulk -c
  -generate-mapping
        Generate Elasticsearch mapping from input
  -id-max-bytes int
        Maximum byte length of emitted document _id values; longer Subject URIs are truncated with a SHA-256 suffix. 0 disables the limit; values below 65 are rejected (default 512)
  -ignore-malformed-geo
        Skip malformed geo_shape values instead of rejecting the document (default true)
  -include string
        Comma-separated list of graph URIs to include (allowlist)
  -keyword-ignore-above int
        Maximum keyword value length before Elasticsearch skips indexing; 0 omits ignore_above (default 8191)
  -keyword-lowercase-normalizer
        Apply a lowercase normalizer to keyword fields and text keyword sub-fields
  -text-analyzer string
        Elasticsearch analyzer to apply to inferred text fields (overrides language-based inference)
  -text-keyword-subfield
        Add a keyword sub-field to inferred text fields (default true)
```

</details>

The below example shows a download of a Gzipped NQuads file, unzipping, sorting, processing and sending colorizing output to less:

```bash
curl -sL https://datasets.crow.nl/crow/thesaurus/download.nq.gz | zcat | LC_ALL=C sort | rdf-nquads-to-jsonl | jq -Cs '.' | less
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
zcat data.nq.gz | LC_ALL=C sort | rdf-nquads-to-jsonl -generate-mapping > mapping.json
zcat data.nq.gz | LC_ALL=C sort | rdf-nquads-to-jsonl -generate-config > config.json
zcat data.nq.gz | LC_ALL=C sort | rdf-nquads-to-jsonl | esbulk -server http://localhost:9200 -index my-index2 -apikey '...' -id _id -c config.json -mapping mapping.json
```

## Document Format

Each RDF subject becomes one Elasticsearch document, with RDF predicates and RDF objects from all graphs combined together.
Some predicates and datatypes are processed for native processing by Elasticsearch, so that e.g. `xsd:date`s become range-filterable.
It is certainly a lossy mapping, but probably useful for full text search, filtering and aggregation.

- `_id` is the RDF subject URI and the Elasticsearch document ID.

  > [!IMPORTANT]  
  > Elasticsearch rejects jobs with any `_id` value longer than 512 bytes. Longer RDF subject URIs are truncated to fit, but still kept unique with a full-URI hash suffix.
  >
  > Use `-id-max-bytes` to change the limit; disable with `0`.
  > stderr outputs remapped URIs.

- `_graph` contains the URIs of all graphs the subject was found in.
- RDF predicate URIs are used as field keys. Periods are special in ES fields, so `.` in a predicate is replaced by ` ` (space).
- All values are arrays of JSON values (strings, numbers, or booleans).
- Language-tagged strings become plain strings.
  - For language-tagged text fields, mapping generation picks the best matching Elasticsearch analyzer from observed language tags (for example `@nl` -> `dutch`).
  - Use `-text-analyzer` to force a specific analyzer for all inferred `text` fields (except `rdf:HTML`, which always uses `html_strip`). Set it to the empty string to turn off all text analyzer inference.
- The XSD integer family (`integer`, `int`, `long`, `short`, `byte`, and the signed/unsigned constrained integer types) becomes JSON integers when in `int64` range, then falls back to floating point or its lexical string for larger values.
  - `xsd:decimal` values become JSON numbers and map to `scaled_float` with `-decimal-scaling-factor` (default `10000`). A field that also contains `xsd:float` or `xsd:double`, or that would overflow, maps to `double`.
- `xsd:date`, `xsd:dateTime`, and `xsd:time` literals are treated as dates for mapping generation. Generated date formats accept strict date/time values and `epoch_millis`.
- `xsd:language`, `xsd:duration`, `xsd:base64Binary`, `xsd:hexBinary`, `xsd:QName`, and `xsd:NMTOKEN` map to `keyword`.
- `xsd:anyURI` literals are treated as keyword values for mapping generation.
- `rdf:HTML` literals map to `text` with the `html_strip` analyzer; `-generate-config` defines that custom analyzer under `settings.analysis.analyzer.html_strip` (HTML char filter + standard tokenizer), so it is in place when the index is created.
- `geosparql:wktLiteral` and `geosparql:geoJSONLiteral` map to `geo_shape`.
- Markdown literals map to `text`. Recognized datatypes:
  - `http://ns.ontowiki.net/SysOnt/Markdown`
  - `<https://triplydb.com/Triply/vocab/def/markdown>`
  - If there're more Markdown datatypes, please open an issue.
- Language-tagged literals (e.g. `"label"@en`) are treated as text values for mapping generation.
- The generated mapping includes a `dynamic_templates` rule that maps any dynamically added string fields to `keyword`, so ad-hoc string fields stay exactly searchable instead of being analyzed `text`.
- Plain string literals are inferred per field: long or mixed-content fields map to `text`, while consistently short values (with enough samples) can map to `keyword`. This heuristic avoids keyword-indexing prose while keeping short identifiers aggregatable.
- Inferred `text` fields have a `keyword` multi-field by default (disables with `-text-keyword-subfield=false`); all keyword fields use `ignore_above: 8191` by default (disable with `-keyword-ignore-above=0`). `-keyword-lowercase-normalizer` applies a lowercase normalizer, defined in `-generate-config` settings.
- `-copy-to full_text` adds `copy_to` to inferred text fields and declares a catch-all `full_text` text property.
- Generated mappings disable Elasticsearch date and numeric detection. Geo-shape fields ignore malformed values by default (disable with `-ignore-malformed-geo=false`). `-date-subfield` opts into a text field with an `ignore_malformed` date multi-field; strict date mappings remain the default.
- All other strings with a datatype become plain strings.

## Development

```sh
go test ./...
go build .
```
