//! Integration tests for es-sqlite.
//!
//! These tests validate that es-sqlite correctly implements the
//! OpenSearch/Elasticsearch REST API, using both the official OpenSearch
//! Rust client and raw HTTP requests.
//!
//! The server is started automatically — just run:
//!   cargo test --test opensearch_client -- --test-threads=1

mod common;

use opensearch::{
    http::{
        request::JsonBody,
        transport::{SingleNodeConnectionPool, TransportBuilder},
    },
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts, IndicesRefreshParts},
    BulkParts, CountParts, DeleteParts, GetParts, IndexParts, MgetParts, OpenSearch, SearchParts,
    UpdateParts,
};
use reqwest::Client;
use serde_json::{json, Value};
use url::Url;

async fn make_client() -> OpenSearch {
    let base_url = common::ensure_server().await;
    let url = Url::parse(base_url).unwrap();
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool)
        .disable_proxy()
        .build()
        .expect("build transport");
    OpenSearch::new(transport)
}

async fn cleanup(client: &OpenSearch, indices: &[&str]) {
    for idx in indices {
        let _ = client
            .indices()
            .delete(IndicesDeleteParts::Index(&[idx]))
            .send()
            .await;
    }
}

// ─── Index Management ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_and_delete_index() {
    let client = make_client().await;
    let idx = "test-create-delete";
    cleanup(&client, &[idx]).await;

    // Create index with mappings
    let resp = client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .body(json!({
            "mappings": {
                "properties": {
                    "title": { "type": "text" },
                    "status": { "type": "keyword" },
                    "count": { "type": "integer" }
                }
            }
        }))
        .send()
        .await
        .expect("create index failed");

    assert!(
        resp.status_code().is_success(),
        "create index returned {}",
        resp.status_code()
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["index"], idx);

    // Check index exists
    let resp = client
        .indices()
        .exists(IndicesExistsParts::Index(&[idx]))
        .send()
        .await
        .expect("exists check failed");
    assert!(resp.status_code().is_success());

    // Delete index
    let resp = client
        .indices()
        .delete(IndicesDeleteParts::Index(&[idx]))
        .send()
        .await
        .expect("delete index failed");
    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["acknowledged"], true);

    // Verify it's gone
    let resp = client
        .indices()
        .exists(IndicesExistsParts::Index(&[idx]))
        .send()
        .await
        .expect("exists after delete failed");
    assert_eq!(resp.status_code().as_u16(), 404);
}

// ─── Document CRUD ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_index_get_delete_document() {
    let client = make_client().await;
    let idx = "test-doc-crud";
    cleanup(&client, &[idx]).await;

    // Create index
    client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .body(json!({
            "mappings": {
                "properties": {
                    "title": { "type": "text" },
                    "count": { "type": "integer" }
                }
            }
        }))
        .send()
        .await
        .unwrap();

    // Index a document with explicit ID
    let resp = client
        .index(IndexParts::IndexId(idx, "1"))
        .body(json!({
            "title": "hello world",
            "count": 42
        }))
        .send()
        .await
        .expect("index doc failed");

    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["_id"], "1");
    assert_eq!(body["result"], "created");
    assert_eq!(body["_version"], 1);

    // Get the document
    let resp = client
        .get(GetParts::IndexId(idx, "1"))
        .send()
        .await
        .expect("get doc failed");

    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["found"], true);
    assert_eq!(body["_id"], "1");
    assert_eq!(body["_source"]["title"], "hello world");
    assert_eq!(body["_source"]["count"], 42);

    // Update the document (re-index with same ID)
    let resp = client
        .index(IndexParts::IndexId(idx, "1"))
        .body(json!({
            "title": "updated title",
            "count": 100
        }))
        .send()
        .await
        .expect("update doc failed");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["result"], "updated");
    assert_eq!(body["_version"], 2);

    // Delete the document
    let resp = client
        .delete(DeleteParts::IndexId(idx, "1"))
        .send()
        .await
        .expect("delete doc failed");

    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["result"], "deleted");

    // Verify get returns found: false
    let resp = client
        .get(GetParts::IndexId(idx, "1"))
        .send()
        .await
        .expect("get after delete failed");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["found"], false);

    cleanup(&client, &[idx]).await;
}

// ─── Partial Update ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_partial_update() {
    let client = make_client().await;
    let idx = "test-partial-update";
    cleanup(&client, &[idx]).await;

    client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .body(json!({"mappings": {"properties": {"title": {"type": "text"}, "count": {"type": "integer"}}}}))
        .send()
        .await
        .unwrap();

    // Index initial document
    client
        .index(IndexParts::IndexId(idx, "1"))
        .body(json!({"title": "original", "count": 1}))
        .send()
        .await
        .unwrap();

    // Partial update — only change count
    let resp = client
        .update(UpdateParts::IndexId(idx, "1"))
        .body(json!({"doc": {"count": 99}}))
        .send()
        .await
        .expect("partial update failed");

    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["result"], "updated");

    // Verify the merge: title should remain, count should be updated
    let resp = client
        .get(GetParts::IndexId(idx, "1"))
        .send()
        .await
        .unwrap();

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["_source"]["title"], "original");
    assert_eq!(body["_source"]["count"], 99);

    cleanup(&client, &[idx]).await;
}

// ─── Search ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_search_match_query() {
    let client = make_client().await;
    let idx = "test-search";
    cleanup(&client, &[idx]).await;

    client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .body(json!({"mappings": {"properties": {"title": {"type": "text"}, "price": {"type": "float"}}}}))
        .send()
        .await
        .unwrap();

    // Index multiple documents
    client
        .index(IndexParts::IndexId(idx, "1"))
        .body(json!({"title": "rust programming language", "price": 29.99}))
        .send()
        .await
        .unwrap();
    client
        .index(IndexParts::IndexId(idx, "2"))
        .body(json!({"title": "python programming guide", "price": 19.99}))
        .send()
        .await
        .unwrap();
    client
        .index(IndexParts::IndexId(idx, "3"))
        .body(json!({"title": "rust cookbook recipes", "price": 39.99}))
        .send()
        .await
        .unwrap();

    // Refresh to make searchable
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[idx]))
        .send()
        .await
        .unwrap();

    // Search: match query
    let resp = client
        .search(SearchParts::Index(&[idx]))
        .body(json!({
            "query": {
                "match": {
                    "title": "rust"
                }
            }
        }))
        .send()
        .await
        .expect("search failed");

    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();

    // Should find 2 documents with "rust"
    let total = body["hits"]["total"]["value"].as_u64().unwrap();
    assert_eq!(total, 2, "Expected 2 hits, got {total}");
    assert_eq!(body["hits"]["total"]["relation"], "eq");
    assert_eq!(body["timed_out"], false);

    let hits = body["hits"]["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 2);

    // Each hit should have _id, _score, _source, _index
    for hit in hits {
        assert!(hit["_id"].is_string());
        assert!(hit["_score"].is_number());
        assert!(hit["_source"].is_object());
        assert_eq!(hit["_index"], idx);
    }

    // Search: match_all
    let resp = client
        .search(SearchParts::Index(&[idx]))
        .body(json!({"query": {"match_all": {}}}))
        .send()
        .await
        .unwrap();

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["hits"]["total"]["value"], 3);

    // Search: bool query
    let resp = client
        .search(SearchParts::Index(&[idx]))
        .body(json!({
            "query": {
                "bool": {
                    "must": [
                        {"match": {"title": "rust"}}
                    ],
                    "filter": [
                        {"range": {"price": {"gte": 30.0}}}
                    ]
                }
            }
        }))
        .send()
        .await
        .unwrap();

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["hits"]["total"]["value"], 1);
    assert_eq!(
        body["hits"]["hits"][0]["_source"]["title"],
        "rust cookbook recipes"
    );

    cleanup(&client, &[idx]).await;
}

// ─── Search with Pagination ─────────────────────────────────────────────────

#[tokio::test]
async fn test_search_pagination() {
    let client = make_client().await;
    let idx = "test-pagination";
    cleanup(&client, &[idx]).await;

    client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .body(json!({"mappings": {"properties": {"title": {"type": "text"}}}}))
        .send()
        .await
        .unwrap();

    for i in 1..=5 {
        client
            .index(IndexParts::IndexId(idx, &i.to_string()))
            .body(json!({"title": format!("document number {i}")}))
            .send()
            .await
            .unwrap();
    }

    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[idx]))
        .send()
        .await
        .unwrap();

    // Page 1: from=0, size=2
    let resp = client
        .search(SearchParts::Index(&[idx]))
        .from(0)
        .size(2)
        .body(json!({"query": {"match_all": {}}}))
        .send()
        .await
        .unwrap();

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["hits"]["total"]["value"], 5);
    assert_eq!(body["hits"]["hits"].as_array().unwrap().len(), 2);

    // Page 2: from=2, size=2
    let resp = client
        .search(SearchParts::Index(&[idx]))
        .from(2)
        .size(2)
        .body(json!({"query": {"match_all": {}}}))
        .send()
        .await
        .unwrap();

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["hits"]["hits"].as_array().unwrap().len(), 2);

    cleanup(&client, &[idx]).await;
}

// ─── Count ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_count() {
    let client = make_client().await;
    let idx = "test-count";
    cleanup(&client, &[idx]).await;

    client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .body(json!({"mappings": {"properties": {"title": {"type": "text"}}}}))
        .send()
        .await
        .unwrap();

    client
        .index(IndexParts::IndexId(idx, "1"))
        .body(json!({"title": "foo"}))
        .send()
        .await
        .unwrap();
    client
        .index(IndexParts::IndexId(idx, "2"))
        .body(json!({"title": "bar"}))
        .send()
        .await
        .unwrap();
    client
        .index(IndexParts::IndexId(idx, "3"))
        .body(json!({"title": "foo bar"}))
        .send()
        .await
        .unwrap();

    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[idx]))
        .send()
        .await
        .unwrap();

    // Count all
    let resp = client
        .count(CountParts::Index(&[idx]))
        .body(json!({"query": {"match_all": {}}}))
        .send()
        .await
        .expect("count failed");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["count"], 3);

    // Count with query
    let resp = client
        .count(CountParts::Index(&[idx]))
        .body(json!({"query": {"match": {"title": "foo"}}}))
        .send()
        .await
        .unwrap();

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["count"], 2); // "foo" and "foo bar"

    cleanup(&client, &[idx]).await;
}

// ─── Bulk ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_bulk_operations() {
    let client = make_client().await;
    let idx = "test-bulk";
    cleanup(&client, &[idx]).await;

    client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .body(json!({"mappings": {"properties": {"title": {"type": "text"}}}}))
        .send()
        .await
        .unwrap();

    // Bulk index using raw NDJSON body
    let bulk_body: Vec<JsonBody<Value>> = vec![
        json!({"index": {"_index": idx, "_id": "1"}}).into(),
        json!({"title": "first document"}).into(),
        json!({"index": {"_index": idx, "_id": "2"}}).into(),
        json!({"title": "second document"}).into(),
        json!({"index": {"_index": idx, "_id": "3"}}).into(),
        json!({"title": "third document"}).into(),
    ];

    let resp = client
        .bulk(BulkParts::None)
        .body(bulk_body)
        .send()
        .await
        .expect("bulk failed");

    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["errors"], false);
    assert_eq!(body["items"].as_array().unwrap().len(), 3);

    // Verify documents exist
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[idx]))
        .send()
        .await
        .unwrap();

    let resp = client
        .count(CountParts::Index(&[idx]))
        .body(json!({"query": {"match_all": {}}}))
        .send()
        .await
        .unwrap();

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["count"], 3);

    cleanup(&client, &[idx]).await;
}

// ─── Multi-Get ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_mget() {
    let client = make_client().await;
    let idx = "test-mget";
    cleanup(&client, &[idx]).await;

    client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .body(json!({"mappings": {"properties": {"title": {"type": "text"}}}}))
        .send()
        .await
        .unwrap();

    client
        .index(IndexParts::IndexId(idx, "1"))
        .body(json!({"title": "one"}))
        .send()
        .await
        .unwrap();
    client
        .index(IndexParts::IndexId(idx, "2"))
        .body(json!({"title": "two"}))
        .send()
        .await
        .unwrap();

    // Multi-get with index in path
    let resp = client
        .mget(MgetParts::Index(idx))
        .body(json!({
            "ids": ["1", "2", "999"]
        }))
        .send()
        .await
        .expect("mget failed");

    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();
    let docs = body["docs"].as_array().unwrap();
    assert_eq!(docs.len(), 3);
    assert_eq!(docs[0]["found"], true);
    assert_eq!(docs[0]["_source"]["title"], "one");
    assert_eq!(docs[1]["found"], true);
    assert_eq!(docs[1]["_source"]["title"], "two");
    assert_eq!(docs[2]["found"], false);

    // Multi-get without index in path (specify per-doc)
    let resp = client
        .mget(MgetParts::None)
        .body(json!({
            "docs": [
                {"_index": idx, "_id": "1"},
                {"_index": idx, "_id": "2"}
            ]
        }))
        .send()
        .await
        .expect("mget global failed");

    let body: Value = resp.json().await.unwrap();
    let docs = body["docs"].as_array().unwrap();
    assert_eq!(docs.len(), 2);
    assert!(docs.iter().all(|d| d["found"] == true));

    cleanup(&client, &[idx]).await;
}

// ─── Cluster Health ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cluster_health() {
    let client = make_client().await;

    let resp = client
        .cluster()
        .health(opensearch::cluster::ClusterHealthParts::None)
        .send()
        .await
        .expect("health failed");

    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "green");
    assert_eq!(body["cluster_name"], "opensearch-sqlite");
    assert_eq!(body["number_of_nodes"], 1);
}

// ─── Dynamic Mapping ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_dynamic_mapping() {
    let client = make_client().await;
    let idx = "test-dynamic";
    cleanup(&client, &[idx]).await;

    // Create index with no mappings
    client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .send()
        .await
        .unwrap();

    // Index a document — fields should be auto-detected
    client
        .index(IndexParts::IndexId(idx, "1"))
        .body(json!({
            "name": "test item",
            "price": 19.99,
            "active": true,
            "count": 5
        }))
        .send()
        .await
        .unwrap();

    // Retrieve it back
    let resp = client
        .get(GetParts::IndexId(idx, "1"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["found"], true);
    assert_eq!(body["_source"]["name"], "test item");
    assert_eq!(body["_source"]["price"], 19.99);
    assert_eq!(body["_source"]["active"], true);
    assert_eq!(body["_source"]["count"], 5);

    cleanup(&client, &[idx]).await;
}

// ─── Auto-generated ID ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_auto_generated_id() {
    let client = make_client().await;
    let idx = "test-autoid";
    cleanup(&client, &[idx]).await;

    client
        .indices()
        .create(IndicesCreateParts::Index(idx))
        .body(json!({"mappings": {"properties": {"title": {"type": "text"}}}}))
        .send()
        .await
        .unwrap();

    // Index without explicit ID
    let resp = client
        .index(IndexParts::Index(idx))
        .body(json!({"title": "auto id document"}))
        .send()
        .await
        .expect("auto-id index failed");

    assert!(resp.status_code().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["result"], "created");
    assert!(body["_id"].is_string());
    assert!(!body["_id"].as_str().unwrap().is_empty());

    cleanup(&client, &[idx]).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Raw HTTP integration tests
//
// The tests below exercise features that are easier to test via raw HTTP
// requests (typed_keys, aliases, delete-by-query, keyword stripping, etc.).
// ═══════════════════════════════════════════════════════════════════════════

async fn base_url() -> &'static str {
    common::ensure_server().await
}

fn http() -> Client {
    Client::new()
}

/// Helper: create an index via raw HTTP.
async fn create_index_raw(base: &str, index: &str, mappings: Value) {
    let client = http();
    let _ = client.delete(format!("{}/{}", base, index)).send().await;

    let resp = client
        .put(format!("{}/{}", base, index))
        .json(&json!({ "mappings": mappings }))
        .send()
        .await
        .expect("create index");
    assert!(
        resp.status().is_success(),
        "Failed to create index {}: {}",
        index,
        resp.status()
    );
}

/// Helper: index a document via raw HTTP.
async fn index_doc(base: &str, index: &str, id: &str, doc: Value) {
    let client = http();
    let resp = client
        .put(format!("{}/{}/_doc/{}", base, index, id))
        .json(&doc)
        .send()
        .await
        .expect("index doc");
    assert!(
        resp.status().is_success(),
        "Failed to index doc {}/{}: {}",
        index,
        id,
        resp.status()
    );
}

/// Helper: refresh an index via raw HTTP.
async fn refresh_raw(base: &str, index: &str) {
    let client = http();
    client
        .post(format!("{}/{}/_refresh", base, index))
        .send()
        .await
        .expect("refresh");
}

/// Helper: search and return response body via raw HTTP.
async fn search_raw(base: &str, index: &str, body: Value) -> Value {
    let client = http();
    let resp = client
        .post(format!("{}/{}/_search", base, index))
        .json(&body)
        .send()
        .await
        .expect("search");
    assert!(
        resp.status().is_success(),
        "Search failed on {}: {}",
        index,
        resp.status()
    );
    resp.json().await.expect("parse search response")
}

/// Helper: search with query params via raw HTTP.
async fn search_with_params(base: &str, index: &str, params: &str, body: Value) -> Value {
    let client = http();
    let resp = client
        .post(format!("{}/{}/_search?{}", base, index, params))
        .json(&body)
        .send()
        .await
        .expect("search with params");
    assert!(
        resp.status().is_success(),
        "Search failed on {}: {}",
        index,
        resp.status()
    );
    resp.json().await.expect("parse search response")
}

/// Helper: cleanup indices via raw HTTP.
async fn cleanup_raw(base: &str, indices: &[&str]) {
    let client = http();
    for idx in indices {
        let _ = client.delete(format!("{}/{}", base, idx)).send().await;
    }
}

// ─── Terms Aggregations ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_terms_aggregation_basic() {
    let base = base_url().await;
    let idx = "test-agg-basic";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "title": { "type": "text" },
                "category": { "type": "keyword" },
                "status": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(
        base,
        idx,
        "1",
        json!({"title": "doc one", "category": "books", "status": "active"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "2",
        json!({"title": "doc two", "category": "books", "status": "active"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "3",
        json!({"title": "doc three", "category": "electronics", "status": "active"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "4",
        json!({"title": "doc four", "category": "electronics", "status": "inactive"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "5",
        json!({"title": "doc five", "category": "clothing", "status": "inactive"}),
    )
    .await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "size": 0,
            "aggs": {
                "categories": {
                    "terms": { "field": "category", "size": 10 }
                }
            }
        }),
    )
    .await;

    assert_eq!(body["hits"]["total"]["value"], 5);
    let aggs = &body["aggregations"]["categories"];
    assert!(aggs["buckets"].is_array(), "Expected buckets array");
    let buckets = aggs["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 3, "Expected 3 category buckets");

    let first_count = buckets[0]["doc_count"].as_u64().unwrap();
    let last_count = buckets[buckets.len() - 1]["doc_count"].as_u64().unwrap();
    assert!(
        first_count >= last_count,
        "Buckets should be sorted by doc_count desc"
    );

    let books_bucket = buckets
        .iter()
        .find(|b| b["key"] == "books")
        .expect("books bucket");
    assert_eq!(books_bucket["doc_count"], 2);
    let electronics_bucket = buckets
        .iter()
        .find(|b| b["key"] == "electronics")
        .expect("electronics bucket");
    assert_eq!(electronics_bucket["doc_count"], 2);
    let clothing_bucket = buckets
        .iter()
        .find(|b| b["key"] == "clothing")
        .expect("clothing bucket");
    assert_eq!(clothing_bucket["doc_count"], 1);

    cleanup_raw(base, &[idx]).await;
}

#[tokio::test]
async fn test_terms_aggregation_multiple() {
    let base = base_url().await;
    let idx = "test-agg-multi";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "title": { "type": "text" },
                "category": { "type": "keyword" },
                "status": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(
        base,
        idx,
        "1",
        json!({"title": "a", "category": "books", "status": "active"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "2",
        json!({"title": "b", "category": "books", "status": "inactive"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "3",
        json!({"title": "c", "category": "toys", "status": "active"}),
    )
    .await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "size": 0,
            "aggs": {
                "by_category": {
                    "terms": { "field": "category" }
                },
                "by_status": {
                    "terms": { "field": "status" }
                }
            }
        }),
    )
    .await;

    let cat_buckets = body["aggregations"]["by_category"]["buckets"]
        .as_array()
        .unwrap();
    let status_buckets = body["aggregations"]["by_status"]["buckets"]
        .as_array()
        .unwrap();

    assert_eq!(cat_buckets.len(), 2);
    assert_eq!(status_buckets.len(), 2);

    cleanup_raw(base, &[idx]).await;
}

#[tokio::test]
async fn test_terms_aggregation_with_query_filter() {
    let base = base_url().await;
    let idx = "test-agg-filtered";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "title": { "type": "text" },
                "category": { "type": "keyword" },
                "price": { "type": "float" }
            }
        }),
    )
    .await;

    index_doc(
        base,
        idx,
        "1",
        json!({"title": "cheap book", "category": "books", "price": 5.0}),
    )
    .await;
    index_doc(
        base,
        idx,
        "2",
        json!({"title": "expensive book", "category": "books", "price": 50.0}),
    )
    .await;
    index_doc(
        base,
        idx,
        "3",
        json!({"title": "cheap toy", "category": "toys", "price": 3.0}),
    )
    .await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "size": 0,
            "query": {
                "range": { "price": { "lt": 10.0 } }
            },
            "aggs": {
                "categories": {
                    "terms": { "field": "category" }
                }
            }
        }),
    )
    .await;

    assert_eq!(body["hits"]["total"]["value"], 2);
    let buckets = body["aggregations"]["categories"]["buckets"]
        .as_array()
        .unwrap();
    assert_eq!(buckets.len(), 2);

    cleanup_raw(base, &[idx]).await;
}

// ─── typed_keys Support ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_typed_keys() {
    let base = base_url().await;
    let idx = "test-typed-keys";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "status": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(base, idx, "1", json!({"status": "active"})).await;
    index_doc(base, idx, "2", json!({"status": "inactive"})).await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "size": 0,
            "aggs": {
                "statuses": { "terms": { "field": "status" } }
            }
        }),
    )
    .await;
    assert!(
        body["aggregations"]["statuses"].is_object(),
        "Expected plain agg name"
    );

    let body = search_with_params(
        base,
        idx,
        "typed_keys=true",
        json!({
            "size": 0,
            "aggs": {
                "statuses": { "terms": { "field": "status" } }
            }
        }),
    )
    .await;
    assert!(
        body["aggregations"]["sterms#statuses"].is_object(),
        "Expected 'sterms#statuses' key, got: {:?}",
        body["aggregations"]
    );
    assert!(
        body["aggregations"]["statuses"].is_null(),
        "Plain name should not exist with typed_keys=true"
    );

    cleanup_raw(base, &[idx]).await;
}

// ─── Aggregations alias field ───────────────────────────────────────────────

#[tokio::test]
async fn test_aggregations_keyword_alias() {
    let base = base_url().await;
    let idx = "test-agg-alias";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "category": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(base, idx, "1", json!({"category": "A"})).await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "size": 0,
            "aggregations": {
                "cats": { "terms": { "field": "category" } }
            }
        }),
    )
    .await;

    let buckets = body["aggregations"]["cats"]["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["key"], "A");

    cleanup_raw(base, &[idx]).await;
}

// ─── Empty aggregations (no matching indices) ───────────────────────────────

#[tokio::test]
async fn test_aggregations_empty_index_returns_empty_buckets() {
    let base = base_url().await;
    let client = http();
    let resp = client
        .post(format!(
            "{}/nonexistent-index-pattern*/_search?typed_keys=true",
            base
        ))
        .json(&json!({
            "size": 0,
            "aggs": {
                "myagg": { "terms": { "field": "status" } }
            }
        }))
        .send()
        .await
        .expect("search non-existent pattern");

    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["hits"]["total"]["value"], 0);
    assert!(
        body["aggregations"]["sterms#myagg"]["buckets"].is_array(),
        "Expected empty buckets array, got: {:?}",
        body["aggregations"]
    );
    assert_eq!(
        body["aggregations"]["sterms#myagg"]["buckets"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

// ─── Index Aliases ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_alias_create_and_search() {
    let base = base_url().await;
    let idx = "test-alias-target";
    let alias = "test-alias-name";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "title": { "type": "text" },
                "category": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(
        base,
        idx,
        "1",
        json!({"title": "hello world", "category": "greetings"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "2",
        json!({"title": "goodbye world", "category": "farewells"}),
    )
    .await;
    refresh_raw(base, idx).await;

    let client = http();
    let resp = client
        .post(format!("{}/_aliases", base))
        .json(&json!({
            "actions": [
                { "add": { "index": idx, "alias": alias } }
            ]
        }))
        .send()
        .await
        .expect("create alias");
    assert!(resp.status().is_success());

    let body = search_raw(base, alias, json!({"query": {"match_all": {}}})).await;
    assert_eq!(body["hits"]["total"]["value"], 2);

    let body = search_raw(
        base,
        alias,
        json!({
            "query": { "match": { "title": "hello" } }
        }),
    )
    .await;
    assert_eq!(body["hits"]["total"]["value"], 1);

    let resp = client
        .get(format!("{}/{}/_alias", base, idx))
        .send()
        .await
        .expect("get aliases");
    let body: Value = resp.json().await.unwrap();
    assert!(
        body[idx]["aliases"][alias].is_object(),
        "Expected alias in response: {:?}",
        body
    );

    let resp = client
        .post(format!("{}/_aliases", base))
        .json(&json!({
            "actions": [
                { "remove": { "index": idx, "alias": alias } }
            ]
        }))
        .send()
        .await
        .expect("remove alias");
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/{}/_search", base, alias))
        .json(&json!({"query": {"match_all": {}}}))
        .send()
        .await
        .expect("search removed alias");
    if resp.status().is_success() {
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["hits"]["total"]["value"], 0,
            "After alias removal, search should return 0 results"
        );
    } else {
        assert_eq!(resp.status().as_u16(), 404);
    }

    cleanup_raw(base, &[idx]).await;
}

#[tokio::test]
async fn test_alias_multiple_indices() {
    let base = base_url().await;
    let idx1 = "test-multi-alias-1";
    let idx2 = "test-multi-alias-2";
    let alias = "test-multi-alias-all";
    cleanup_raw(base, &[idx1, idx2]).await;

    create_index_raw(
        base,
        idx1,
        json!({
            "properties": {
                "name": { "type": "text" },
                "type": { "type": "keyword" }
            }
        }),
    )
    .await;
    create_index_raw(
        base,
        idx2,
        json!({
            "properties": {
                "name": { "type": "text" },
                "type": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(base, idx1, "1", json!({"name": "alpha", "type": "first"})).await;
    index_doc(base, idx2, "2", json!({"name": "beta", "type": "second"})).await;
    refresh_raw(base, idx1).await;
    refresh_raw(base, idx2).await;

    let client = http();
    client
        .post(format!("{}/_aliases", base))
        .json(&json!({
            "actions": [
                { "add": { "index": idx1, "alias": alias } },
                { "add": { "index": idx2, "alias": alias } }
            ]
        }))
        .send()
        .await
        .expect("create multi alias");

    let body = search_raw(base, alias, json!({"query": {"match_all": {}}})).await;
    assert_eq!(body["hits"]["total"]["value"], 2);

    let body = search_raw(
        base,
        alias,
        json!({
            "size": 0,
            "aggs": {
                "types": { "terms": { "field": "type" } }
            }
        }),
    )
    .await;
    let buckets = body["aggregations"]["types"]["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 2);

    cleanup_raw(base, &[idx1, idx2]).await;
}

// ─── Sort Support ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_sort_by_field() {
    let base = base_url().await;
    let idx = "test-sort";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "name": { "type": "keyword" },
                "price": { "type": "float" },
                "created_at": { "type": "date" }
            }
        }),
    )
    .await;

    index_doc(base, idx, "1", json!({"name": "C-item", "price": 30.0})).await;
    index_doc(base, idx, "2", json!({"name": "A-item", "price": 10.0})).await;
    index_doc(base, idx, "3", json!({"name": "B-item", "price": 20.0})).await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "query": { "match_all": {} },
            "sort": [{ "price": { "order": "asc" } }]
        }),
    )
    .await;

    let hits = body["hits"]["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0]["_source"]["name"], "A-item");
    assert_eq!(hits[1]["_source"]["name"], "B-item");
    assert_eq!(hits[2]["_source"]["name"], "C-item");

    let body = search_raw(
        base,
        idx,
        json!({
            "query": { "match_all": {} },
            "sort": [{ "price": { "order": "desc" } }]
        }),
    )
    .await;

    let hits = body["hits"]["hits"].as_array().unwrap();
    assert_eq!(hits[0]["_source"]["name"], "C-item");
    assert_eq!(hits[2]["_source"]["name"], "A-item");

    cleanup_raw(base, &[idx]).await;
}

// ─── Multi-Index Search ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_multi_index_search() {
    let base = base_url().await;
    let idx1 = "test-multi-search-1";
    let idx2 = "test-multi-search-2";
    cleanup_raw(base, &[idx1, idx2]).await;

    create_index_raw(
        base,
        idx1,
        json!({
            "properties": { "title": { "type": "text" }, "source": { "type": "keyword" } }
        }),
    )
    .await;
    create_index_raw(
        base,
        idx2,
        json!({
            "properties": { "title": { "type": "text" }, "source": { "type": "keyword" } }
        }),
    )
    .await;

    index_doc(
        base,
        idx1,
        "1",
        json!({"title": "rust programming", "source": "idx1"}),
    )
    .await;
    index_doc(
        base,
        idx2,
        "2",
        json!({"title": "rust cookbook", "source": "idx2"}),
    )
    .await;
    refresh_raw(base, idx1).await;
    refresh_raw(base, idx2).await;

    let body = search_raw(
        base,
        &format!("{},{}", idx1, idx2),
        json!({
            "query": { "match": { "title": "rust" } }
        }),
    )
    .await;

    assert_eq!(body["hits"]["total"]["value"], 2);
    let hits = body["hits"]["hits"].as_array().unwrap();
    let indices: Vec<&str> = hits.iter().map(|h| h["_index"].as_str().unwrap()).collect();
    assert!(indices.contains(&idx1));
    assert!(indices.contains(&idx2));

    cleanup_raw(base, &[idx1, idx2]).await;
}

#[tokio::test]
async fn test_wildcard_index_search() {
    let base = base_url().await;
    let idx1 = "test-wild-aaa";
    let idx2 = "test-wild-bbb";
    cleanup_raw(base, &[idx1, idx2]).await;

    create_index_raw(
        base,
        idx1,
        json!({
            "properties": { "val": { "type": "keyword" } }
        }),
    )
    .await;
    create_index_raw(
        base,
        idx2,
        json!({
            "properties": { "val": { "type": "keyword" } }
        }),
    )
    .await;

    index_doc(base, idx1, "1", json!({"val": "from-aaa"})).await;
    index_doc(base, idx2, "2", json!({"val": "from-bbb"})).await;
    refresh_raw(base, idx1).await;
    refresh_raw(base, idx2).await;

    let body = search_raw(base, "test-wild-*", json!({"query": {"match_all": {}}})).await;
    assert_eq!(body["hits"]["total"]["value"], 2);

    cleanup_raw(base, &[idx1, idx2]).await;
}

// ─── Delete by Query ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_delete_by_query() {
    let base = base_url().await;
    let idx = "test-delete-by-query";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "title": { "type": "text" },
                "status": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(
        base,
        idx,
        "1",
        json!({"title": "keep me", "status": "active"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "2",
        json!({"title": "delete me", "status": "inactive"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "3",
        json!({"title": "delete me too", "status": "inactive"}),
    )
    .await;
    refresh_raw(base, idx).await;

    let client = http();
    let resp = client
        .post(format!("{}/{}/_delete_by_query", base, idx))
        .json(&json!({
            "query": {
                "term": { "status": "inactive" }
            }
        }))
        .send()
        .await
        .expect("delete by query");

    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["deleted"], 2);
    assert_eq!(body["total"], 2);

    refresh_raw(base, idx).await;
    let body = search_raw(base, idx, json!({"query": {"match_all": {}}})).await;
    assert_eq!(body["hits"]["total"]["value"], 1);
    assert_eq!(body["hits"]["hits"][0]["_source"]["status"], "active");

    cleanup_raw(base, &[idx]).await;
}

// ─── FTS Underscore/Hyphen Search ───────────────────────────────────────────

#[tokio::test]
async fn test_search_underscore_terms() {
    let base = base_url().await;
    let idx = "test-underscore-search";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "name": { "type": "text" },
                "description": { "type": "text" }
            }
        }),
    )
    .await;

    index_doc(
        base,
        idx,
        "1",
        json!({
            "name": "act_hi_entitylink",
            "description": "Historical entity link table"
        }),
    )
    .await;
    index_doc(
        base,
        idx,
        "2",
        json!({
            "name": "act_hi_taskinst",
            "description": "Historical task instance table"
        }),
    )
    .await;
    index_doc(
        base,
        idx,
        "3",
        json!({
            "name": "user_profile",
            "description": "User profiles"
        }),
    )
    .await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "query": { "match": { "name": "act_hi_entitylin" } }
        }),
    )
    .await;
    assert!(
        body["hits"]["total"]["value"].as_u64().unwrap() >= 1,
        "Should find act_hi_entitylink with partial search 'act_hi_entitylin'"
    );

    let body = search_raw(
        base,
        idx,
        json!({
            "query": { "match": { "name": "act_hi_taskinst" } }
        }),
    )
    .await;
    assert_eq!(body["hits"]["total"]["value"], 1);

    let body = search_raw(
        base,
        idx,
        json!({
            "query": {
                "multi_match": {
                    "query": "act_hi",
                    "fields": ["name", "description"]
                }
            }
        }),
    )
    .await;
    assert!(
        body["hits"]["total"]["value"].as_u64().unwrap() >= 2,
        "Should find multiple act_hi_* tables"
    );

    cleanup_raw(base, &[idx]).await;
}

// ─── Keyword Sub-field (.keyword suffix) ────────────────────────────────────

#[tokio::test]
async fn test_keyword_subfield_stripping() {
    let base = base_url().await;
    let idx = "test-keyword-strip";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "name": {
                    "type": "text",
                    "fields": { "keyword": { "type": "keyword" } }
                },
                "status": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(
        base,
        idx,
        "1",
        json!({"name": "hello_world", "status": "active"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "2",
        json!({"name": "goodbye_world", "status": "inactive"}),
    )
    .await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "query": {
                "term": { "name.keyword": "hello_world" }
            }
        }),
    )
    .await;
    assert_eq!(body["hits"]["total"]["value"], 1);
    assert_eq!(body["hits"]["hits"][0]["_source"]["name"], "hello_world");

    let body = search_raw(
        base,
        idx,
        json!({
            "size": 0,
            "aggs": {
                "names": { "terms": { "field": "name.keyword" } }
            }
        }),
    )
    .await;
    let buckets = body["aggregations"]["names"]["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 2);

    cleanup_raw(base, &[idx]).await;
}

// ─── Bool Should with Mixed FTS and Term Queries ────────────────────────────

#[tokio::test]
async fn test_bool_should_mixed_fts_and_term() {
    let base = base_url().await;
    let idx = "test-bool-should-mix";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "title": { "type": "text" },
                "name": { "type": "keyword" },
                "status": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(
        base,
        idx,
        "1",
        json!({"title": "rust programming", "name": "rust-book", "status": "published"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "2",
        json!({"title": "python guide", "name": "python-book", "status": "published"}),
    )
    .await;
    index_doc(
        base,
        idx,
        "3",
        json!({"title": "cooking recipes", "name": "cookbook", "status": "draft"}),
    )
    .await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": "rust",
                                "fields": ["title"]
                            }
                        },
                        {
                            "term": { "name": "rust-book" }
                        }
                    ]
                }
            }
        }),
    )
    .await;

    assert!(
        body["hits"]["total"]["value"].as_u64().unwrap() >= 1,
        "Should find at least 1 result for mixed should query"
    );

    cleanup_raw(base, &[idx]).await;
}

// ─── Count Endpoint (raw HTTP) ──────────────────────────────────────────────

#[tokio::test]
async fn test_count_with_query() {
    let base = base_url().await;
    let idx = "test-count-query";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "title": { "type": "text" },
                "status": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(base, idx, "1", json!({"title": "one", "status": "active"})).await;
    index_doc(base, idx, "2", json!({"title": "two", "status": "active"})).await;
    index_doc(
        base,
        idx,
        "3",
        json!({"title": "three", "status": "inactive"}),
    )
    .await;
    refresh_raw(base, idx).await;

    let client = http();

    let resp = client
        .post(format!("{}/{}/_count", base, idx))
        .json(&json!({"query": {"match_all": {}}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["count"], 3);

    let resp = client
        .post(format!("{}/{}/_count", base, idx))
        .json(&json!({
            "query": { "term": { "status": "active" } }
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["count"], 2);

    cleanup_raw(base, &[idx]).await;
}

// ─── Aggregation on JSON Array Fields ───────────────────────────────────────

#[tokio::test]
async fn test_aggregation_on_array_field() {
    let base = base_url().await;
    let idx = "test-agg-array";
    cleanup_raw(base, &[idx]).await;

    create_index_raw(
        base,
        idx,
        json!({
            "properties": {
                "title": { "type": "text" },
                "tags": { "type": "keyword" }
            }
        }),
    )
    .await;

    index_doc(
        base,
        idx,
        "1",
        json!({"title": "doc one", "tags": ["rust", "programming"]}),
    )
    .await;
    index_doc(
        base,
        idx,
        "2",
        json!({"title": "doc two", "tags": ["rust", "web"]}),
    )
    .await;
    index_doc(
        base,
        idx,
        "3",
        json!({"title": "doc three", "tags": ["python", "web"]}),
    )
    .await;
    refresh_raw(base, idx).await;

    let body = search_raw(
        base,
        idx,
        json!({
            "size": 0,
            "aggs": {
                "tag_counts": { "terms": { "field": "tags", "size": 10 } }
            }
        }),
    )
    .await;

    let buckets = body["aggregations"]["tag_counts"]["buckets"]
        .as_array()
        .unwrap();
    assert!(
        buckets.len() >= 3,
        "Expected at least 3 tag buckets, got {}",
        buckets.len()
    );

    let rust_bucket = buckets.iter().find(|b| b["key"] == "rust");
    assert!(rust_bucket.is_some(), "Expected 'rust' bucket");
    assert_eq!(rust_bucket.unwrap()["doc_count"], 2);

    let web_bucket = buckets.iter().find(|b| b["key"] == "web");
    assert!(web_bucket.is_some(), "Expected 'web' bucket");
    assert_eq!(web_bucket.unwrap()["doc_count"], 2);

    cleanup_raw(base, &[idx]).await;
}
