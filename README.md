# es-sqlite

An OpenSearch/Elasticsearch-compatible REST API server backed by SQLite's FTS5 full-text search engine. Drop-in replacement for common OpenSearch operations using a single lightweight binary with no external dependencies.

## Why?

For fun

## Quick Start

```bash
# Build
cargo build --release

# Run (defaults to port 9200, data in ./data/)
./target/release/es-sqlite

# Or with custom settings
./target/release/es-sqlite --port 9200 --data-dir ./data --host 0.0.0.0
```

```bash
# Create an index
curl -X PUT localhost:9200/my-index -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "title": {"type": "text"},
      "price": {"type": "float"},
      "status": {"type": "keyword"}
    }
  }
}'

# Index a document
curl -X POST localhost:9200/my-index/_doc/1 -H 'Content-Type: application/json' -d '{
  "title": "Introduction to Full-Text Search",
  "price": 29.99,
  "status": "published"
}'

# Search
curl -X POST localhost:9200/my-index/_search -H 'Content-Type: application/json' -d '{
  "query": {
    "match": {"title": "full-text search"}
  }
}'
```

## Running Tests

Tests start the server automatically -- no manual server needed:

```bash
# Run all tests (integration + YAML specs)
cargo test

# Run only OpenSearch client compatibility tests
cargo test --test opensearch_client

# Run only YAML REST API spec tests
cargo test --test yaml_runner

# Run comparison tests vs real OpenSearch (requires Docker)
cargo test --test comparison -- --test-threads=1

# Run performance benchmarks with real Gutenberg books (requires Docker)
cargo bench --bench gutenberg_bench
```

## Implemented Endpoints

### Index Management

| Endpoint | Method | Description |
|---|---|---|
| `/{index}` | `PUT` | Create index with mappings and settings |
| `/{index}` | `DELETE` | Delete index |
| `/{index}` | `HEAD` | Check if index exists |
| `/{index}/_mapping` | `GET` | Get index mappings |
| `/{index}/_mapping` | `PUT` | Update index mappings |
| `/{index}/_refresh` | `POST` | Refresh (WAL checkpoint) |

### Document CRUD

| Endpoint | Method | Description |
|---|---|---|
| `/{index}/_doc/{id}` | `PUT/POST` | Index document with explicit ID |
| `/{index}/_doc` | `POST` | Index document with auto-generated ID |
| `/{index}/_doc/{id}` | `GET` | Get document by ID |
| `/{index}/_doc/{id}` | `DELETE` | Delete document |
| `/{index}/_update/{id}` | `POST` | Partial update (merge fields) |
| `/{index}/_create/{id}` | `PUT/POST` | Create document (alias for index) |

### Search & Query

| Endpoint | Method | Description |
|---|---|---|
| `/{index}/_search` | `GET/POST` | Search with Query DSL |
| `/{index}/_count` | `GET/POST` | Count matching documents |
| `/{index}/_delete_by_query` | `POST` | Delete documents matching a query |

### Aliases

| Endpoint | Method | Description |
|---|---|---|
| `/_aliases` | `POST` | Add/remove index aliases (bulk actions) |
| `/{index}/_alias` | `GET` | Get aliases for an index |
| `/_aliases` | `GET` | Get all aliases |

### Bulk Operations

| Endpoint | Method | Description |
|---|---|---|
| `/_bulk` | `POST` | Bulk index/create/update/delete (NDJSON) |
| `/_mget` | `GET/POST` | Multi-get by IDs (global) |
| `/{index}/_mget` | `GET/POST` | Multi-get by IDs (index-scoped) |

### Cluster (Stubbed)

| Endpoint | Method | Description |
|---|---|---|
| `/_cluster/health` | `GET` | Cluster health (always green) |
| `/_cat/indices` | `GET` | List indices with doc counts |

**Total: 26 endpoint-method combinations across 18 routes**

## Supported Query DSL

| Query Type | Description | Translation |
|---|---|---|
| `match_all` | Match all documents | `SELECT * FROM _source` |
| `match` | Full-text match on text fields | FTS5 `MATCH '{column}: (terms)'` |
| `match` | Exact match on keyword/numeric fields | SQL `WHERE field = value` |
| `term` | Exact term match | SQL `WHERE field = value` |
| `terms` | Match any of multiple values | SQL `WHERE field IN (...)` |
| `range` | Range filter (gt, gte, lt, lte) | SQL comparison operators |
| `bool` | Boolean combination (must/should/must_not/filter) | SQL AND/OR/NOT composition |
| `multi_match` | Match across multiple fields | FTS5 column filter `{col1 col2}: query` |
| `exists` | Field exists check | SQL `WHERE field IS NOT NULL` |
| `query_string` | Query string syntax (basic) | FTS5 MATCH with field detection |
| `function_score` | Wrapper with scoring functions | Delegates to inner query (scores ignored) |
| `wrapper` | Base64-encoded query wrapper | Decoded and parsed as normal query |

Scoring uses SQLite FTS5's built-in BM25 with default parameters (k1=1.2, b=0.75), matching OpenSearch defaults. Ranking order is comparable but exact scores will differ from OpenSearch due to IDF scope differences.

## Aggregations

Terms aggregations are supported, translating to SQL `GROUP BY` queries.

```bash
# Terms aggregation
curl -X POST localhost:9200/my-index/_search -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_status": {
      "terms": { "field": "status", "size": 10 }
    }
  }
}'
# Response:
# { "aggregations": { "by_status": { "buckets": [
#     { "key": "published", "doc_count": 42 },
#     { "key": "draft", "doc_count": 7 }
# ]}}}

# Multiple aggregations with typed_keys (OpenSearch compatibility)
curl -X POST 'localhost:9200/my-index/_search?typed_keys=true' \
  -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_status": { "terms": { "field": "status" } },
    "by_category": { "terms": { "field": "category" } }
  }
}'
# Response keys are prefixed: "sterms#by_status", "sterms#by_category"

# Aggregation with query filter
curl -X POST localhost:9200/my-index/_search -H 'Content-Type: application/json' -d '{
  "size": 0,
  "query": { "range": { "price": { "gte": 10.0 } } },
  "aggs": {
    "categories": { "terms": { "field": "category" } }
  }
}'
```

**Aggregation features:**
- `terms` aggregation with `field` and `size` parameters
- `typed_keys=true` query parameter (prefixes keys with `sterms#`)
- Both `aggs` and `aggregations` field names accepted
- Works with JSON array fields (auto-detects and uses `json_each`)
- Aggregations on `.keyword` sub-fields (suffix automatically stripped)
- Multi-index aggregation with bucket merging
- Empty buckets returned when no documents match (prevents frontend crashes)

## Index Aliases

Aliases allow querying one or more indices under a single name. Aliases are persisted to disk and survive server restarts.

```bash
# Create an alias
curl -X POST localhost:9200/_aliases -H 'Content-Type: application/json' -d '{
  "actions": [
    { "add": { "index": "my-index", "alias": "my-alias" } }
  ]
}'

# Alias pointing to multiple indices
curl -X POST localhost:9200/_aliases -H 'Content-Type: application/json' -d '{
  "actions": [
    { "add": { "index": "logs-2024-01", "alias": "logs-all" } },
    { "add": { "index": "logs-2024-02", "alias": "logs-all" } }
  ]
}'

# Search via alias (searches all underlying indices)
curl -X POST localhost:9200/my-alias/_search -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}'

# Remove an alias
curl -X POST localhost:9200/_aliases -H 'Content-Type: application/json' -d '{
  "actions": [
    { "remove": { "index": "my-index", "alias": "my-alias" } }
  ]
}'

# List all aliases
curl localhost:9200/_aliases
```

## Sorting

Results can be sorted by any non-text field.

```bash
# Sort by price ascending
curl -X POST localhost:9200/my-index/_search -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} },
  "sort": [
    { "price": { "order": "asc" } }
  ]
}'

# Sort by multiple fields
curl -X POST localhost:9200/my-index/_search -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} },
  "sort": [
    { "category": { "order": "asc" } },
    { "price": { "order": "desc" } }
  ]
}'
```

## Multi-Index Search

Search across multiple indices using comma-separated names or wildcard patterns.

```bash
# Comma-separated indices
curl -X POST 'localhost:9200/index-a,index-b,index-c/_search' \
  -H 'Content-Type: application/json' -d '{
  "query": { "match": { "title": "hello" } }
}'

# Wildcard pattern
curl -X POST 'localhost:9200/logs-*/_search' \
  -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}'
```

## Delete by Query

Delete all documents matching a query.

```bash
curl -X POST localhost:9200/my-index/_delete_by_query \
  -H 'Content-Type: application/json' -d '{
  "query": {
    "term": { "status": "inactive" }
  }
}'
# Response: { "deleted": 5, "total": 5, ... }
```

## Storage Architecture

- **One SQLite database per index** -- Clean isolation, simple deletion, no cross-index locking
- **WAL mode** -- Concurrent reads with serialized writes
- **External content FTS5** -- Full-text index references `_source` table, avoiding duplicate text storage (~50% disk savings)
- **Automatic triggers** -- AFTER INSERT/UPDATE/DELETE triggers keep FTS5 in sync with `_source`
- **Dynamic mapping** -- Auto-detects field types on first document, adds columns via `ALTER TABLE`

### Schema per index

```
_meta           -- key/value store for mappings, settings, and aliases (JSON)
_source         -- _id (PK), _version, _seq_no, _source (JSON), extracted columns
_fts            -- FTS5 virtual table (external content mode, text fields only)
_fts_ai/ad/au   -- Triggers to sync FTS5 with _source
```

### Supported Field Types

| OpenSearch Type | SQLite Type | Notes |
|---|---|---|
| `text` | FTS5 column | Full-text searchable with BM25 |
| `keyword` | `TEXT` | Exact match, stored in _source table |
| `long`, `integer`, `short`, `byte` | `INTEGER` | Range queries supported |
| `float`, `double`, `half_float` | `REAL` | Range queries supported |
| `boolean` | `INTEGER` | Stored as 0/1 |
| `date` | `TEXT` | Auto-detected from RFC3339/ISO8601 |
| `object` | `TEXT` | Stored as nested JSON |

## Performance Benchmark

Performance benchmarks are run separately via `cargo bench` using real Project Gutenberg books (1600 documents). The benchmark tests at increasing document body sizes (1K, 5K, 10K, 50K, 100K chars) to show how performance scales. Results below are from [CI (GitHub Actions, Ubuntu 24.04)](https://github.com/luqmansen/es-sqlite/actions/runs/22474762499/job/65099316921), not a tuned local machine.

```bash
# Run benchmarks (requires Docker for OpenSearch comparison)
cargo bench --bench gutenberg_bench
```

The corpus is automatically downloaded from the [Gutendex API](https://gutendex.com/) on first run and cached locally (5 concurrent workers, per-file caching to disk). Benchmarks cover: `match_all`, `match`, `multi_match`, `bool` must+filter, `terms` aggregation, `sort`, `query_string`, and `filtered_agg` queries.

**Results** (1600 Gutenberg books, es-sqlite vs OpenSearch 2.17.1, GitHub Actions runner):

| Query | 1K (avg 999B) | 5K (avg 5.0KB) | 10K (avg 9.9KB) | 50K (avg 48.9KB) | 100K (avg 96.3KB) |
|---|---|---|---|---|---|
| match_all | **44ms** vs 51ms (1.2x) | **44ms** vs 54ms (1.2x) | **45ms** vs 57ms (1.3x) | **48ms** vs 80ms (1.7x) | **53ms** vs 110ms (2.1x) |
| match_single | **45ms** vs 51ms (1.1x) | **45ms** vs 52ms (1.2x) | **45ms** vs 54ms (1.2x) | **49ms** vs 62ms (1.3x) | **54ms** vs 77ms (1.4x) |
| match_body | **49ms** vs 53ms (1.1x) | **51ms** vs 55ms (1.1x) | **57ms** vs 59ms (1.0x) | **72ms** vs 86ms (1.2x) | **102ms** vs 119ms (1.2x) |
| multi_match | **45ms** vs 55ms (1.2x) | **48ms** vs 55ms (1.1x) | **51ms** vs 59ms (1.1x) | **63ms** vs 87ms (1.4x) | **80ms** vs 116ms (1.4x) |
| bool_must_filter | **45ms** vs 50ms (1.1x) | **47ms** vs 53ms (1.1x) | **50ms** vs 57ms (1.1x) | **75ms** vs 84ms (1.1x) | 123ms vs 122ms (~1x) |
| terms_agg | 52ms vs **50ms** (~1x) | 69ms vs **49ms** (1.4x OS) | 91ms vs **50ms** (1.8x OS) | 300ms vs **49ms** (6.1x OS) | 540ms vs **50ms** (10.9x OS) |
| sort_popularity | **46ms** vs 52ms (1.1x) | **49ms** vs 53ms (1.1x) | **54ms** vs 58ms (1.1x) | **82ms** vs 85ms (1.0x) | 123ms vs 120ms (~1x) |
| sort_multi | **46ms** vs 56ms (1.2x) | **49ms** vs 55ms (1.1x) | **55ms** vs 59ms (1.1x) | **82ms** vs 83ms (1.0x) | 126ms vs 114ms (1.1x OS) |
| query_string | **44ms** vs 51ms (1.2x) | **46ms** vs 54ms (1.2x) | **47ms** vs 56ms (1.2x) | **56ms** vs 86ms (1.5x) | **71ms** vs 112ms (1.6x) |
| filtered_agg | **45ms** vs 50ms (1.1x) | 53ms vs **49ms** (1.1x OS) | 69ms vs **52ms** (1.3x OS) | 228ms vs **49ms** (4.6x OS) | 444ms vs **49ms** (9.1x OS) |
| **Bulk index** | **623ms** vs 1240ms (2.0x) | **1.1s** vs 2.5s (2.2x) | **1.6s** vs 2.5s (1.5x) | **4.8s** vs 6.2s (1.3x) | **8.9s** vs 10.8s (1.2x) |

**Key takeaways:**
- es-sqlite is **1.1-2.1x faster** for search/match/sort queries across all body sizes, with the advantage growing at larger document sizes for `match_all`, `multi_match`, and `query_string`
- Bulk indexing is consistently **1.2-2.2x faster** at all sizes
- `query_string` uses FTS5 (not LIKE fallback), keeping it **1.2-1.6x faster** than OpenSearch at all sizes
- OpenSearch dominates on `terms_agg` and `filtered_agg` at larger body sizes (10K+), where its columnar aggregation engine scales better than SQLite's `GROUP BY` on JSON-extracted fields -- up to **10.9x faster** at 100K
- At 100K body sizes, `bool_must_filter`, `sort_popularity`, and `sort_multi` converge to roughly equal performance between both engines
- These are CI numbers (shared GitHub Actions runner); local results may differ due to CPU, disk, and JVM warm-up variance

#### Resource Benchmark (Memory & Disk)

The same benchmark also measures memory (RSS) and disk usage for both engines. es-sqlite runs as a single lightweight process with SQLite storage, while OpenSearch runs a full JVM-based server. Memory is measured via continuous sampling (every 500ms) using `ps -o rss=` for es-sqlite and `/proc/1/status` VmRSS inside the container for OpenSearch. Note: the environments differ (native macOS vs Linux container under Docker Desktop), so these numbers are indicative rather than a precise comparison.

**Memory (RSS)** (1593 Gutenberg books, local macOS, OpenSearch JVM heap `-Xms512m -Xmx512m`):

| Body Size | es-sqlite (avg) | es-sqlite (peak) | OpenSearch (avg) | OpenSearch (peak) | Ratio (avg) |
|---|---|---|---|---|---|
| idle | **5.5 MB** | 6.5 MB | 1.0 GB | 1.1 GB | **195x less** |
| 1K | **8.1 MB** | 11.3 MB | 1.1 GB | 1.1 GB | **138x less** |
| 5K | **27.7 MB** | 32.2 MB | 1.1 GB | 1.1 GB | **41x less** |
| 10K | **35.4 MB** | 40.9 MB | 1.1 GB | 1.1 GB | **32x less** |
| 50K | **40.3 MB** | 48.1 MB | 1.1 GB | 1.1 GB | **27x less** |
| 100K | **54.0 MB** | 79.4 MB | 1.1 GB | 1.3 GB | **22x less** |

**Key takeaways:**
- es-sqlite uses **22-195x less memory** than OpenSearch across all document sizes
- Idle: **5.5 MB** vs **1.0 GB** — the JVM heap (512 MB) plus off-heap memory (Lucene segments, direct buffers, thread stacks) dominates OpenSearch's footprint
- At 100K body size with 1593 documents, es-sqlite peaks at **79 MB** vs OpenSearch's **1.3 GB**
- es-sqlite memory grows sub-linearly with data size (SQLite's page cache is bounded); OpenSearch stays relatively flat due to its pre-allocated JVM heap

## OpenSearch API Compatibility

Tested with the official [`opensearch` Rust crate](https://crates.io/crates/opensearch) v2.3.0 and validated against a real OpenSearch 2.17.1 instance.

| Category | Compatible |
|---|---|
| Index CRUD (create, delete, exists, mappings) | Yes |
| Document CRUD (index, get, update, delete) | Yes |
| Dynamic mapping (no pre-defined schema) | Yes |
| `match`, `match_all`, `term`, `terms`, `range` queries | Yes |
| `bool` (must/should/must_not/filter) | Yes |
| `multi_match`, `query_string`, `exists` | Yes |
| `function_score`, `wrapper` | Yes |
| `terms` aggregation (`typed_keys`, `.keyword`, JSON arrays) | Yes |
| Index aliases (single + multi-index, persisted) | Yes |
| Sort by keyword, numeric, and date fields | Yes |
| Multi-index search (comma-separated + wildcard) | Yes |
| Delete by query | Yes |
| Bulk indexing and multi-get | Yes |
| Pagination (`from`/`size` via query params and body) | Yes |
| Cluster health, `_cat/indices` | Yes (stubbed) |

### Known limitations

- **Single node only** -- Obviously
- **Only terms aggregations** -- Metric aggregations (avg, sum, cardinality) and nested/pipeline aggregations are not supported
- **No scroll/search_after** -- Only basic `from`/`size` pagination
- **No _source filtering** -- Always returns full `_source`
- **No stored fields** -- All fields come from `_source` JSON
- **No index open/close** -- Indices are always open
- **No cluster coordination** -- Single-process, single-node architecture
- **BM25 scores differ** -- Same parameters as OpenSearch (k1=1.2, b=0.75) but IDF scope is per-index (inherent to SQLite FTS5)
- **FTS5 text columns are fixed at creation** -- New text fields added via dynamic mapping won't be full-text searchable; queries automatically fall back to `LIKE`-based matching when FTS columns are missing

## OpenSearch YAML REST API Spec Tests

All 408 YAML spec files (from 114 categories) are vendored from the [OpenSearch repository](https://github.com/opensearch-project/OpenSearch/tree/main/rest-api-spec/src/main/resources/rest-api-spec/test) and run against es-sqlite with explicit pass/skip classification.

```
Expected to pass:  114 tests   (regression = hard failure)
Skipped:           326 tests   (documented reasons)
Known failures:    398 tests   (tracked for future work)
Regressions:       0
```

### Passing spec categories

| Category | Tests | What's covered |
|---|---|---|
| `bulk` | 1 | Array of objects (auto-create index) |
| `cluster.health` | 5 | Basic health, wait_for_active_shards |
| `cluster.*` (allocation_explain, reroute, remote_info, voting_config) | 7 | Error handling, empty state |
| `create` | 3 | Create with/without ID, nested limit |
| `delete` | 2 | Basic delete, missing document handling |
| `exists` | 1 | Document exists check |
| `get` / `get_source` | 8 | Basic CRUD, default values, realtime refresh, missing documents |
| `index` | 3 | Index with ID, result field, flat object teardown |
| `indices.create` | 3 | Create with/without mappings, invalid mappings |
| `indices.delete` | 4 | Delete against aliases, wildcards |
| `indices.get` | 7 | Index info, wildcards, missing index errors |
| `indices.get_mapping` | 6 | Empty/populated mappings, missing index, wildcards |
| `indices.put_mapping` | 1 | Invalid mappings |
| `indices.get_alias` / `put_alias` / `delete_alias` | 10 | CRUD, wildcards, error handling |
| `indices.*` (open, blocks, clear_cache, forcemerge, analyze, exists, recovery, rollover, stats, upgrade, settings, templates) | 22 | Various index management operations |
| `info` / `ping` | 3 | Server info, ping |
| `search` | 6 | Field collapsing, batch reduce, pre-filter, indices options |
| `search.aggregation` | 5 | Terms (error cases), sig_terms, moving_fn, median_absolute_deviation |
| `scroll` | 3 | Error cases, teardown |
| `snapshot.*` | 4 | Error handling for missing snapshots/repos |
| `update` | 4 | Upsert, doc_as_upsert, error messages, require_alias |
| `ingest` / `mtermvectors` | 2 | Error handling |

### Skipped spec categories (not supported)

| Reason | Count | Examples |
|---|---|---|
| Unsupported field types | 14 files | geo_point, flat_object, unsigned_long, date_nanos, wildcard, match_only_text |
| Routing | 9 files | Document/alias routing not supported |
| Versioning / compare-and-swap | 10 files | External versioning, if_seq_no |
| _source filtering | 7 files | Always returns full _source |
| Unsupported query types | 15 files | intervals, match_bool_prefix, combined_fields, distance_feature |
| Unsupported search features | 16 files | search_after, fetch_fields, doc_values, matched_queries, bitmap filtering |
| Unsupported aggregation types | 42 files | histogram, range, filter, avg, max, min, sum, cardinality, percentiles, composite, etc. |
| Cat APIs (text format) | 20 files | Cat APIs return text format not supported |
| Cluster/node management | varies | snapshot, suggest, explain, field_caps, scripts, PIT, etc. |
| Other | varies | refresh param, stored fields, error_trace, shard headers |

## Project Structure

```
src/
  main.rs               -- CLI entrypoint, server startup
  server.rs             -- axum Router with all route definitions
  config.rs             -- CLI args (port, data_dir, host)
  lib.rs                -- Module re-exports for integration tests
  error.rs              -- OpenSearch-compatible error JSON responses
  routes/
    index.rs            -- Index CRUD (create, delete, exists, mappings)
    document.rs         -- Document CRUD (index, get, delete, update)
    search.rs           -- _search, _count with query param support
    bulk.rs             -- _bulk NDJSON parsing and execution
    multi.rs            -- _mget multi-document retrieval
    cluster.rs          -- _cluster/health, _cat/indices, _refresh
  query/
    ast.rs              -- Query DSL AST types
    parser.rs           -- JSON Query DSL -> AST
    translator.rs       -- AST -> SQL + FTS5 MATCH expressions
  model/
    mapping.rs          -- Index mapping types, field type detection
    document.rs         -- Document response models
    search.rs           -- Search request/response models
    bulk.rs             -- Bulk request/response models
  storage/
    registry.rs         -- IndexRegistry: index lifecycle management
    connection.rs       -- SQLite connection setup (WAL, pragmas)
    schema.rs           -- DDL generation (FTS5 tables, triggers)
    reader.rs           -- Document get, search, count execution
    writer.rs           -- Document insert/update/delete
tests/
  opensearch_client.rs  -- Integration tests (opensearch crate + raw HTTP)
  comparison.rs         -- Side-by-side comparison tests vs real OpenSearch (Docker)
  yaml_runner.rs        -- OpenSearch YAML REST API spec runner
test-specs/             -- Vendored OpenSearch YAML test files (408 specs, 114 categories)
```

## License

MIT
