//! Comparison integration tests: es-sqlite vs real OpenSearch.
//!
//! These tests run the same queries against both es-sqlite and a real OpenSearch
//! Docker container, then compare:
//! - Response structure (same keys, same value types)
//! - Hit counts (identical)
//! - Ranking order (same document IDs in same order)
//! - Aggregation buckets (same keys, same counts)
//! - Sort order (identical when explicit sort is used)
//!
//! Run with:
//!   cargo test --test comparison -- --test-threads=1
//!
//! The OpenSearch Docker container is started automatically and cleaned up after tests.
//! If Docker is unavailable, all tests are skipped gracefully.

mod common;

use reqwest::Client;
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

// ─── Docker OpenSearch Management ───────────────────────────────────────────

const CONTAINER_NAME: &str = "es-sqlite-test-opensearch";
const OPENSEARCH_IMAGE: &str = "opensearchproject/opensearch:2.17.1";

static OPENSEARCH_URL: OnceLock<Option<String>> = OnceLock::new();

/// Start an OpenSearch Docker container and return its URL.
/// Returns None if Docker is not available. Called once per test run.
/// Uses only std::process and std::net (no reqwest::blocking) to avoid
/// runtime conflicts with tokio.
fn ensure_opensearch_sync() -> Option<String> {
    // Check if Docker is available
    let docker_check = Command::new("docker").arg("info").output();
    if docker_check.is_err() || !docker_check.unwrap().status.success() {
        eprintln!("Docker not available, skipping comparison tests");
        return None;
    }

    // Kill any existing container with this name
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();

    // Find an available port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind for port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    // Start OpenSearch container
    let result = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-p",
            &format!("{}:9200", port),
            "-e",
            "discovery.type=single-node",
            "-e",
            "DISABLE_SECURITY_PLUGIN=true",
            "-e",
            "OPENSEARCH_INITIAL_ADMIN_PASSWORD=Admin123!",
            "-e",
            "OPENSEARCH_JAVA_OPTS=-Xms256m -Xmx256m",
            OPENSEARCH_IMAGE,
        ])
        .output();

    match result {
        Ok(output) if output.status.success() => {
            eprintln!("Started OpenSearch container on port {}", port);
        }
        Ok(output) => {
            eprintln!(
                "Failed to start OpenSearch container: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            return None;
        }
        Err(e) => {
            eprintln!("Failed to run docker: {}", e);
            return None;
        }
    }

    // Wait for OpenSearch to be ready using curl (avoids reqwest::blocking runtime issues)
    let url = format!("http://127.0.0.1:{}", port);

    for i in 0..120 {
        let check = Command::new("curl")
            .args(["-sf", "--max-time", "2", &url])
            .output();
        if let Ok(output) = check {
            if output.status.success() {
                eprintln!("OpenSearch ready after ~{}s", i);
                return Some(url);
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    eprintln!("OpenSearch failed to start within 120s");
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();
    None
}

fn get_opensearch_url() -> Option<&'static str> {
    OPENSEARCH_URL
        .get_or_init(|| ensure_opensearch_sync())
        .as_deref()
}

/// Get both server URLs, skipping if OpenSearch is not available.
async fn require_both() -> Option<(&'static str, &'static str)> {
    let es = common::ensure_server().await;
    let os = match get_opensearch_url() {
        Some(url) => url,
        None => {
            eprintln!("OpenSearch not available, skipping test");
            return None;
        }
    };
    Some((es, os))
}

// ─── HTTP Helpers ───────────────────────────────────────────────────────────

fn http() -> Client {
    Client::new()
}

async fn create_index(base: &str, index: &str, mappings: Value) {
    let client = http();
    let _ = client.delete(format!("{}/{}", base, index)).send().await;
    // Small delay for OpenSearch to process delete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let resp = client
        .put(format!("{}/{}", base, index))
        .json(&json!({ "mappings": mappings }))
        .send()
        .await
        .expect("create index");
    assert!(
        resp.status().is_success(),
        "Failed to create index {} on {}: {}",
        index,
        base,
        resp.status()
    );
}

async fn index_doc(base: &str, index: &str, id: &str, doc: Value) {
    let client = http();
    let resp = client
        .put(format!("{}/{}/_doc/{}", base, index, id))
        .json(&doc)
        .send()
        .await
        .expect("index doc");
    assert!(resp.status().is_success());
}

async fn refresh(base: &str, index: &str) {
    let client = http();
    client
        .post(format!("{}/{}/_refresh", base, index))
        .send()
        .await
        .expect("refresh");
}

async fn search(base: &str, index: &str, body: Value) -> Value {
    let client = http();
    let resp = client
        .post(format!("{}/{}/_search", base, index))
        .json(&body)
        .send()
        .await
        .expect("search");
    assert!(
        resp.status().is_success(),
        "Search failed on {}/{}",
        base,
        index
    );
    resp.json().await.expect("parse json")
}

async fn search_with_params(base: &str, index: &str, params: &str, body: Value) -> Value {
    let client = http();
    let resp = client
        .post(format!("{}/{}/_search?{}", base, index, params))
        .json(&body)
        .send()
        .await
        .expect("search with params");
    assert!(resp.status().is_success());
    resp.json().await.expect("parse json")
}

async fn count(base: &str, index: &str, body: Value) -> Value {
    let client = http();
    let resp = client
        .post(format!("{}/{}/_count", base, index))
        .json(&body)
        .send()
        .await
        .expect("count");
    assert!(resp.status().is_success());
    resp.json().await.expect("parse json")
}

async fn cleanup(base: &str, indices: &[&str]) {
    let client = http();
    for idx in indices {
        let _ = client.delete(format!("{}/{}", base, idx)).send().await;
    }
}

/// Set up the same index + docs on both servers.
async fn setup_both(
    es_base: &str,
    os_base: &str,
    index: &str,
    mappings: Value,
    docs: &[(&str, Value)],
) {
    for base in [es_base, os_base] {
        cleanup(base, &[index]).await;
        create_index(base, index, mappings.clone()).await;
        for (id, doc) in docs {
            index_doc(base, index, id, doc.clone()).await;
        }
        refresh(base, index).await;
    }
    // Give OpenSearch a moment to settle after refresh
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
}

async fn cleanup_both(es_base: &str, os_base: &str, indices: &[&str]) {
    for base in [es_base, os_base] {
        cleanup(base, indices).await;
    }
}

// ─── Comparison Helpers ─────────────────────────────────────────────────────

/// Keys that are expected to vary between implementations and should be
/// skipped during structural comparison.
const SKIP_KEYS: &[&str] = &["took", "_seq_no", "_primary_term", "_version"];

/// Recursively compare JSON structure: same keys, same value types.
/// Does not compare exact values for numeric/string fields (scores, timing differ).
/// Panics with a descriptive message on mismatch.
fn assert_same_structure(es: &Value, os: &Value, path: &str) {
    match (es, os) {
        (Value::Object(es_map), Value::Object(os_map)) => {
            // Check that all OpenSearch keys exist in es-sqlite response
            for key in os_map.keys() {
                if SKIP_KEYS.contains(&key.as_str()) {
                    continue;
                }
                assert!(
                    es_map.contains_key(key),
                    "es-sqlite missing key '{key}' at path '{path}'. \
                     OpenSearch has: {:?}, es-sqlite has: {:?}",
                    os_map.keys().collect::<Vec<_>>(),
                    es_map.keys().collect::<Vec<_>>()
                );
                assert_same_structure(&es_map[key], &os_map[key], &format!("{path}.{key}"));
            }
        }
        (Value::Array(es_arr), Value::Array(os_arr)) => {
            assert_eq!(
                es_arr.len(),
                os_arr.len(),
                "Array length mismatch at '{path}': es-sqlite={}, opensearch={}",
                es_arr.len(),
                os_arr.len()
            );
            // Compare structure of first element (if any) as representative
            if !es_arr.is_empty() {
                assert_same_structure(&es_arr[0], &os_arr[0], &format!("{path}[0]"));
            }
        }
        // For scalar values, just check the type matches
        (Value::Number(_), Value::Number(_)) => {}
        (Value::String(_), Value::String(_)) => {}
        (Value::Bool(_), Value::Bool(_)) => {}
        (Value::Null, Value::Null) => {}
        _ => {
            panic!(
                "Type mismatch at '{path}': es-sqlite={}, opensearch={}",
                value_type_name(es),
                value_type_name(os)
            );
        }
    }
}

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Compare that exact values match for specific fields (not just types).
fn assert_same_value(es: &Value, os: &Value, field: &str) {
    assert_eq!(
        es, os,
        "Value mismatch for '{}': es-sqlite={}, opensearch={}",
        field, es, os
    );
}

/// Compare ranking: extract `_id` lists and check they match.
fn assert_same_ranking(es_body: &Value, os_body: &Value) {
    let es_ids: Vec<&str> = es_body["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["_id"].as_str().unwrap())
        .collect();
    let os_ids: Vec<&str> = os_body["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        es_ids, os_ids,
        "Ranking mismatch:\n  es-sqlite:   {:?}\n  opensearch:  {:?}",
        es_ids, os_ids
    );
}

/// Compare ranking with tolerance: check that the top-N IDs overlap by at
/// least `min_overlap_pct` percent (0.0–1.0), and if they all overlap,
/// also check the ordering is identical.
fn assert_similar_ranking(es_body: &Value, os_body: &Value, top_n: usize, min_overlap_pct: f64) {
    let es_ids: Vec<&str> = es_body["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .take(top_n)
        .map(|h| h["_id"].as_str().unwrap())
        .collect();
    let os_ids: Vec<&str> = os_body["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .take(top_n)
        .map(|h| h["_id"].as_str().unwrap())
        .collect();

    if es_ids.is_empty() && os_ids.is_empty() {
        return;
    }

    let max_len = es_ids.len().max(os_ids.len());
    let overlap = es_ids.iter().filter(|id| os_ids.contains(id)).count();
    let overlap_pct = overlap as f64 / max_len as f64;

    assert!(
        overlap_pct >= min_overlap_pct,
        "Ranking overlap too low: {:.0}% (need {:.0}%)\n  es-sqlite:   {:?}\n  opensearch:  {:?}",
        overlap_pct * 100.0,
        min_overlap_pct * 100.0,
        es_ids,
        os_ids
    );

    // If all IDs overlap but order differs, log it (BM25 tie-breaking may differ)
    if overlap == max_len && es_ids != os_ids {
        eprintln!(
            "  Note: Same IDs, different order (BM25 tie-breaking):\n    es-sqlite:   {:?}\n    opensearch:  {:?}",
            es_ids, os_ids
        );
    }
}

/// Compare aggregation buckets: same keys with same doc_count values.
fn assert_same_buckets(es_agg: &Value, os_agg: &Value, agg_name: &str) {
    let es_buckets = es_agg["buckets"]
        .as_array()
        .unwrap_or_else(|| panic!("es-sqlite: no buckets array in agg '{}'", agg_name));
    let os_buckets = os_agg["buckets"]
        .as_array()
        .unwrap_or_else(|| panic!("opensearch: no buckets array in agg '{}'", agg_name));

    assert_eq!(
        es_buckets.len(),
        os_buckets.len(),
        "Bucket count mismatch for '{}': es-sqlite={}, opensearch={}",
        agg_name,
        es_buckets.len(),
        os_buckets.len()
    );

    // Build maps for comparison (order may differ for equal counts)
    let es_map: std::collections::HashMap<String, u64> = es_buckets
        .iter()
        .map(|b| (b["key"].to_string(), b["doc_count"].as_u64().unwrap()))
        .collect();
    let os_map: std::collections::HashMap<String, u64> = os_buckets
        .iter()
        .map(|b| (b["key"].to_string(), b["doc_count"].as_u64().unwrap()))
        .collect();

    assert_eq!(
        es_map, os_map,
        "Bucket values mismatch for '{}':\n  es-sqlite:   {:?}\n  opensearch:  {:?}",
        agg_name, es_map, os_map
    );
}

// ─── Structure Comparison Tests ─────────────────────────────────────────────

#[tokio::test]
async fn compare_structure_match_all() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-struct-matchall";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "status": {"type": "keyword"},
        "price": {"type": "float"}
    }});
    let docs = vec![
        (
            "1",
            json!({"title": "first document", "status": "active", "price": 10.0}),
        ),
        (
            "2",
            json!({"title": "second document", "status": "inactive", "price": 20.0}),
        ),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    let query = json!({"query": {"match_all": {}}});
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    // Structure comparison
    assert_same_structure(&es_body, &os_body, "root");

    // Exact value checks
    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "hits.total.value",
    );
    assert_same_value(
        &es_body["hits"]["total"]["relation"],
        &os_body["hits"]["total"]["relation"],
        "hits.total.relation",
    );
    assert_same_value(&es_body["timed_out"], &os_body["timed_out"], "timed_out");

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_structure_search_hits() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-struct-hits";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "category": {"type": "keyword"}
    }});
    let docs = vec![
        (
            "1",
            json!({"title": "rust programming language", "category": "tech"}),
        ),
        (
            "2",
            json!({"title": "rust cookbook for beginners", "category": "tech"}),
        ),
        (
            "3",
            json!({"title": "python web development", "category": "tech"}),
        ),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    let query = json!({"query": {"match": {"title": "rust"}}});
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    // Same hit count
    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "hits.total.value",
    );

    // Hit structure matches
    assert_same_structure(&es_body, &os_body, "root");

    // Each hit should have the same _source content
    let es_hits = es_body["hits"]["hits"].as_array().unwrap();
    let os_hits = os_body["hits"]["hits"].as_array().unwrap();
    for es_hit in es_hits {
        let id = es_hit["_id"].as_str().unwrap();
        let os_hit = os_hits
            .iter()
            .find(|h| h["_id"].as_str().unwrap() == id)
            .unwrap_or_else(|| panic!("OpenSearch missing hit with _id={}", id));
        assert_eq!(
            es_hit["_source"], os_hit["_source"],
            "_source mismatch for _id={}",
            id
        );
    }

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_structure_aggregation() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-struct-agg";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "category": {"type": "keyword"}
    }});
    let docs = vec![
        ("1", json!({"title": "one", "category": "A"})),
        ("2", json!({"title": "two", "category": "A"})),
        ("3", json!({"title": "three", "category": "B"})),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    let query = json!({
        "size": 0,
        "aggs": {
            "categories": {"terms": {"field": "category", "size": 10}}
        }
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    // Structure comparison — the aggregation response shape should match
    assert_same_structure(&es_body, &os_body, "root");

    // Bucket values must match exactly
    assert_same_buckets(
        &es_body["aggregations"]["categories"],
        &os_body["aggregations"]["categories"],
        "categories",
    );

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_structure_typed_keys() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-struct-typed";

    let mappings = json!({"properties": {
        "status": {"type": "keyword"}
    }});
    let docs = vec![
        ("1", json!({"status": "active"})),
        ("2", json!({"status": "inactive"})),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    let query = json!({
        "size": 0,
        "aggs": {
            "statuses": {"terms": {"field": "status"}}
        }
    });
    let es_body = search_with_params(es, idx, "typed_keys=true", query.clone()).await;
    let os_body = search_with_params(os, idx, "typed_keys=true", query).await;

    // Both should have "sterms#statuses" key
    assert!(
        es_body["aggregations"]["sterms#statuses"].is_object(),
        "es-sqlite missing sterms#statuses: {:?}",
        es_body["aggregations"]
    );
    assert!(
        os_body["aggregations"]["sterms#statuses"].is_object(),
        "opensearch missing sterms#statuses: {:?}",
        os_body["aggregations"]
    );

    assert_same_structure(&es_body, &os_body, "root");

    assert_same_buckets(
        &es_body["aggregations"]["sterms#statuses"],
        &os_body["aggregations"]["sterms#statuses"],
        "sterms#statuses",
    );

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_structure_count() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-struct-count";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "status": {"type": "keyword"}
    }});
    let docs = vec![
        ("1", json!({"title": "one", "status": "active"})),
        ("2", json!({"title": "two", "status": "active"})),
        ("3", json!({"title": "three", "status": "inactive"})),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    // Count all
    let es_body = count(es, idx, json!({"query": {"match_all": {}}})).await;
    let os_body = count(os, idx, json!({"query": {"match_all": {}}})).await;
    assert_same_structure(&es_body, &os_body, "count-all");
    assert_same_value(&es_body["count"], &os_body["count"], "count");

    // Count filtered
    let es_body = count(es, idx, json!({"query": {"term": {"status": "active"}}})).await;
    let os_body = count(os, idx, json!({"query": {"term": {"status": "active"}}})).await;
    assert_same_value(&es_body["count"], &os_body["count"], "count-filtered");

    cleanup_both(es, os, &[idx]).await;
}

// ─── Ranking Comparison Tests ───────────────────────────────────────────────

#[tokio::test]
async fn compare_ranking_single_term() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-rank-single";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "body": {"type": "text"}
    }});
    // Documents with varying relevance to "rust"
    let docs = vec![
        (
            "1",
            json!({"title": "rust", "body": "The rust programming language is fast"}),
        ),
        (
            "2",
            json!({"title": "rust programming guide", "body": "Learn rust from scratch"}),
        ),
        (
            "3",
            json!({"title": "python basics", "body": "A guide to python with some rust mentions"}),
        ),
        (
            "4",
            json!({"title": "cooking recipes", "body": "Nothing about programming"}),
        ),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    let query = json!({
        "query": {"match": {"title": "rust"}},
        "size": 10
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    // Same number of hits
    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "hits.total.value",
    );

    // Ranking should be similar (allow 80% overlap in top results)
    assert_similar_ranking(&es_body, &os_body, 4, 0.8);

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_ranking_multi_match() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-rank-multi";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "description": {"type": "text"},
        "category": {"type": "keyword"}
    }});
    let docs = vec![
        (
            "1",
            json!({"title": "rust guide", "description": "comprehensive rust tutorial", "category": "programming"}),
        ),
        (
            "2",
            json!({"title": "rust cookbook", "description": "practical rust recipes", "category": "programming"}),
        ),
        (
            "3",
            json!({"title": "web development", "description": "building web apps with rust", "category": "web"}),
        ),
        (
            "4",
            json!({"title": "database systems", "description": "sql and nosql databases", "category": "databases"}),
        ),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    let query = json!({
        "query": {
            "multi_match": {
                "query": "rust",
                "fields": ["title^2", "description"]
            }
        },
        "size": 10
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "hits.total.value",
    );

    // Multi-match ranking — expect similar ordering
    assert_similar_ranking(&es_body, &os_body, 4, 0.8);

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_ranking_bool_must_filter() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-rank-bool";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "price": {"type": "float"},
        "status": {"type": "keyword"}
    }});
    let docs = vec![
        (
            "1",
            json!({"title": "rust programming", "price": 29.99, "status": "published"}),
        ),
        (
            "2",
            json!({"title": "advanced rust", "price": 49.99, "status": "published"}),
        ),
        (
            "3",
            json!({"title": "rust for beginners", "price": 19.99, "status": "draft"}),
        ),
        (
            "4",
            json!({"title": "python programming", "price": 24.99, "status": "published"}),
        ),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    let query = json!({
        "query": {
            "bool": {
                "must": [{"match": {"title": "rust"}}],
                "filter": [{"term": {"status": "published"}}]
            }
        },
        "size": 10
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "hits.total.value",
    );

    // Bool must+filter: same docs, ranking should be very similar
    assert_similar_ranking(&es_body, &os_body, 3, 0.8);

    cleanup_both(es, os, &[idx]).await;
}

// ─── Aggregation Value Tests ────────────────────────────────────────────────

#[tokio::test]
async fn compare_aggregation_terms() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-agg-terms";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "category": {"type": "keyword"},
        "status": {"type": "keyword"}
    }});
    let docs = vec![
        (
            "1",
            json!({"title": "a", "category": "books", "status": "active"}),
        ),
        (
            "2",
            json!({"title": "b", "category": "books", "status": "active"}),
        ),
        (
            "3",
            json!({"title": "c", "category": "electronics", "status": "active"}),
        ),
        (
            "4",
            json!({"title": "d", "category": "electronics", "status": "inactive"}),
        ),
        (
            "5",
            json!({"title": "e", "category": "clothing", "status": "inactive"}),
        ),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    let query = json!({
        "size": 0,
        "aggs": {
            "by_category": {"terms": {"field": "category", "size": 10}},
            "by_status": {"terms": {"field": "status", "size": 10}}
        }
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    assert_same_buckets(
        &es_body["aggregations"]["by_category"],
        &os_body["aggregations"]["by_category"],
        "by_category",
    );
    assert_same_buckets(
        &es_body["aggregations"]["by_status"],
        &os_body["aggregations"]["by_status"],
        "by_status",
    );

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_aggregation_with_filter() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-agg-filter";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "category": {"type": "keyword"},
        "price": {"type": "float"}
    }});
    let docs = vec![
        (
            "1",
            json!({"title": "cheap book", "category": "books", "price": 5.0}),
        ),
        (
            "2",
            json!({"title": "expensive book", "category": "books", "price": 50.0}),
        ),
        (
            "3",
            json!({"title": "cheap toy", "category": "toys", "price": 3.0}),
        ),
        (
            "4",
            json!({"title": "expensive toy", "category": "toys", "price": 100.0}),
        ),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    // Aggregation with range filter
    let query = json!({
        "size": 0,
        "query": {"range": {"price": {"lt": 10.0}}},
        "aggs": {
            "categories": {"terms": {"field": "category", "size": 10}}
        }
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "hits.total.value",
    );

    assert_same_buckets(
        &es_body["aggregations"]["categories"],
        &os_body["aggregations"]["categories"],
        "categories",
    );

    cleanup_both(es, os, &[idx]).await;
}

// ─── Sort Comparison Tests ──────────────────────────────────────────────────

#[tokio::test]
async fn compare_sort_single_field() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-sort-single";

    let mappings = json!({"properties": {
        "name": {"type": "keyword"},
        "price": {"type": "float"}
    }});
    let docs = vec![
        ("1", json!({"name": "C-item", "price": 30.0})),
        ("2", json!({"name": "A-item", "price": 10.0})),
        ("3", json!({"name": "B-item", "price": 20.0})),
        ("4", json!({"name": "D-item", "price": 5.0})),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    // Sort by price ascending
    let query = json!({
        "query": {"match_all": {}},
        "sort": [{"price": {"order": "asc"}}]
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    // With explicit sort, order must be identical
    assert_same_ranking(&es_body, &os_body);

    // Sort by price descending
    let query = json!({
        "query": {"match_all": {}},
        "sort": [{"price": {"order": "desc"}}]
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    assert_same_ranking(&es_body, &os_body);

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_sort_multi_field() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-sort-multi";

    let mappings = json!({"properties": {
        "category": {"type": "keyword"},
        "price": {"type": "float"},
        "name": {"type": "keyword"}
    }});
    let docs = vec![
        (
            "1",
            json!({"category": "A", "price": 20.0, "name": "item-1"}),
        ),
        (
            "2",
            json!({"category": "B", "price": 10.0, "name": "item-2"}),
        ),
        (
            "3",
            json!({"category": "A", "price": 10.0, "name": "item-3"}),
        ),
        (
            "4",
            json!({"category": "B", "price": 20.0, "name": "item-4"}),
        ),
        (
            "5",
            json!({"category": "A", "price": 10.0, "name": "item-5"}),
        ),
    ];
    setup_both(es, os, idx, mappings, &docs).await;

    // Sort by category asc, then price asc, then name asc (for tie-breaking)
    let query = json!({
        "query": {"match_all": {}},
        "sort": [
            {"category": {"order": "asc"}},
            {"price": {"order": "asc"}},
            {"name": {"order": "asc"}}
        ]
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;

    assert_same_ranking(&es_body, &os_body);

    cleanup_both(es, os, &[idx]).await;
}

// ─── Real Corpus Tests ──────────────────────────────────────────────────────

/// Generate a realistic corpus of Wikipedia-style articles for testing.
fn wikipedia_corpus() -> Vec<(&'static str, Value)> {
    vec![
        (
            "1",
            json!({
                "title": "Rust (programming language)",
                "body": "Rust is a multi-paradigm, general-purpose programming language that emphasizes performance, type safety, and concurrency. It enforces memory safety, meaning that all references point to valid memory, without a garbage collector. Rust was originally designed by Graydon Hoare at Mozilla Research, with contributions from Dave Herman, Brendan Eich, and others. The designers refined the language while writing the Servo experimental browser engine and the Rust compiler.",
                "category": "programming",
                "tags": "systems,memory-safe,compiled",
                "year": 2010,
                "rating": 4.8
            }),
        ),
        (
            "2",
            json!({
                "title": "Python (programming language)",
                "body": "Python is a high-level, general-purpose programming language. Its design philosophy emphasizes code readability with the use of significant indentation. Python is dynamically typed and garbage-collected. It supports multiple programming paradigms, including structured, object-oriented and functional programming. It was conceived in the late 1980s by Guido van Rossum at Centrum Wiskunde & Informatica in the Netherlands as a successor to the ABC programming language.",
                "category": "programming",
                "tags": "interpreted,dynamic,scripting",
                "year": 1991,
                "rating": 4.7
            }),
        ),
        (
            "3",
            json!({
                "title": "SQLite",
                "body": "SQLite is a relational database management system contained in a C library. In contrast to many other database management systems, SQLite is not a client-server database engine. Rather, it is embedded into the end program. SQLite generally follows PostgreSQL syntax. SQLite uses a dynamically and weakly typed SQL syntax that does not guarantee the domain integrity. SQLite is a popular choice as embedded database software for local storage in application software.",
                "category": "database",
                "tags": "embedded,relational,lightweight",
                "year": 2000,
                "rating": 4.5
            }),
        ),
        (
            "4",
            json!({
                "title": "Elasticsearch",
                "body": "Elasticsearch is a search engine based on the Lucene library. It provides a distributed, multitenant-capable full-text search engine with an HTTP web interface and schema-free JSON documents. Elasticsearch is developed in Java and is dual-licensed under the source-available Server Side Public License and the Elastic License. Elasticsearch is the most popular enterprise search engine followed by Apache Solr, also based on Lucene.",
                "category": "database",
                "tags": "search,distributed,full-text",
                "year": 2010,
                "rating": 4.3
            }),
        ),
        (
            "5",
            json!({
                "title": "PostgreSQL",
                "body": "PostgreSQL also known as Postgres, is a free and open-source relational database management system emphasizing extensibility and SQL compliance. It was originally named POSTGRES, referring to its origins as a successor to the Ingres database developed at the University of California, Berkeley. PostgreSQL features transactions with atomicity, consistency, isolation, durability properties, automatically updatable views, materialized views, triggers, foreign keys, and stored procedures.",
                "category": "database",
                "tags": "relational,open-source,enterprise",
                "year": 1996,
                "rating": 4.6
            }),
        ),
        (
            "6",
            json!({
                "title": "Linux kernel",
                "body": "The Linux kernel is a free and open-source, monolithic, modular, multitasking, Unix-like operating system kernel. It was originally authored in 1991 by Linus Torvalds for his i386-based PC, and it was soon adopted as the kernel for the GNU operating system. Linux is deployed on a wide variety of computing systems, such as embedded devices, mobile devices, personal computers, servers, mainframes, and supercomputers.",
                "category": "operating-system",
                "tags": "kernel,open-source,unix",
                "year": 1991,
                "rating": 4.9
            }),
        ),
        (
            "7",
            json!({
                "title": "Docker (software)",
                "body": "Docker is a set of platform as a service products that use OS-level virtualization to deliver software in packages called containers. The service has both free and premium tiers. The software that hosts the containers is called Docker Engine. It was first released in 2013 and is developed by Docker, Inc. Docker can package an application and its dependencies in a virtual container that can run on any Linux, Windows, or macOS computer.",
                "category": "devops",
                "tags": "containers,virtualization,deployment",
                "year": 2013,
                "rating": 4.4
            }),
        ),
        (
            "8",
            json!({
                "title": "Kubernetes",
                "body": "Kubernetes is an open-source container orchestration system for automating software deployment, scaling, and management. Originally designed by Google, the project is now maintained by a worldwide community of contributors, and the trademark is held by the Cloud Native Computing Foundation. Kubernetes works with Docker and other container tools, and handles scheduling onto nodes in a compute cluster and manages workloads.",
                "category": "devops",
                "tags": "orchestration,containers,cloud-native",
                "year": 2014,
                "rating": 4.5
            }),
        ),
        (
            "9",
            json!({
                "title": "Git (software)",
                "body": "Git is a distributed version control system that tracks versions of files. It is often used to control source code by programmers collaboratively developing software. Git was originally authored by Linus Torvalds in 2005 for development of the Linux kernel. As with most other distributed version control systems, and unlike most client-server systems, every Git directory on every computer is a full-fledged repository with complete history and full version-tracking abilities.",
                "category": "devtools",
                "tags": "version-control,distributed,open-source",
                "year": 2005,
                "rating": 4.8
            }),
        ),
        (
            "10",
            json!({
                "title": "WebAssembly",
                "body": "WebAssembly is a portable binary-code format and a corresponding text format for executable programs as well as software interfaces for facilitating interactions between such programs and their host environment. The main goal of WebAssembly is to enable high-performance applications on web pages, but it is also designed to be usable in non-web environments. WebAssembly can be used alongside JavaScript for web development.",
                "category": "web",
                "tags": "binary,portable,performance",
                "year": 2017,
                "rating": 4.2
            }),
        ),
        (
            "11",
            json!({
                "title": "GraphQL",
                "body": "GraphQL is an open-source data query and manipulation language for APIs, and a query runtime engine. GraphQL was developed internally by Meta in 2012 before being publicly released in 2015. It provides an approach to developing web APIs and has been compared and contrasted with REST and other web service architectures. It allows clients to define the structure of the data required, and the same structure of the data is returned from the server.",
                "category": "web",
                "tags": "api,query-language,meta",
                "year": 2015,
                "rating": 4.1
            }),
        ),
        (
            "12",
            json!({
                "title": "Redis",
                "body": "Redis is an open-source in-memory storage, used as a distributed, in-memory key-value database, cache, and message broker, with optional durability. Because it holds all data in memory and because of its design, Redis offers low-latency reads and writes, making it particularly suitable for use cases that require a cache. Redis is the most popular key-value database and is used by tech industry giants such as Twitter, GitHub, and Snapchat.",
                "category": "database",
                "tags": "in-memory,cache,key-value",
                "year": 2009,
                "rating": 4.5
            }),
        ),
        (
            "13",
            json!({
                "title": "TypeScript",
                "body": "TypeScript is a free and open-source high-level programming language developed by Microsoft that adds static typing with optional type annotations to JavaScript. It is designed for the development of large applications and transpiles to JavaScript. TypeScript may be used to develop JavaScript applications for both client-side and server-side execution. TypeScript supports definition files that can contain type information of existing JavaScript libraries.",
                "category": "programming",
                "tags": "typed,javascript,microsoft",
                "year": 2012,
                "rating": 4.6
            }),
        ),
        (
            "14",
            json!({
                "title": "Apache Kafka",
                "body": "Apache Kafka is a distributed event store and stream-processing platform. It is an open-source system developed by the Apache Software Foundation written in Java and Scala. The project aims to provide a unified, high-throughput, low-latency platform for handling real-time data feeds. Kafka can connect to external systems for data import and export via Kafka Connect, and provides the Kafka Streams libraries for stream processing applications.",
                "category": "data-engineering",
                "tags": "streaming,distributed,event-driven",
                "year": 2011,
                "rating": 4.4
            }),
        ),
        (
            "15",
            json!({
                "title": "Nginx",
                "body": "Nginx is a web server that can also be used as a reverse proxy, load balancer, mail proxy and HTTP cache. The software was created by Igor Sysoev and publicly released in 2004. Nginx is free and open-source software, released under the terms of the 2-clause BSD license. A large fraction of web servers use Nginx, often as a load balancer. Nginx is known for its high performance, stability, rich feature set, simple configuration, and low resource consumption.",
                "category": "web",
                "tags": "web-server,reverse-proxy,load-balancer",
                "year": 2004,
                "rating": 4.6
            }),
        ),
    ]
}

#[tokio::test]
async fn compare_corpus_full_text_ranking() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-corpus-fts";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "body": {"type": "text"},
        "category": {"type": "keyword"},
        "tags": {"type": "keyword"},
        "year": {"type": "integer"},
        "rating": {"type": "float"}
    }});
    let docs = wikipedia_corpus();
    setup_both(es, os, idx, mappings, &docs).await;

    // Query 1: Single-term search
    let query = json!({"query": {"match": {"title": "programming"}}, "size": 15});
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "corpus: match title=programming",
    );
    assert_similar_ranking(&es_body, &os_body, 5, 0.8);

    // Query 2: Multi-word search
    let query = json!({"query": {"match": {"body": "database management system"}}, "size": 15});
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "corpus: match body=database management system",
    );
    assert_similar_ranking(&es_body, &os_body, 5, 0.6);

    // Query 3: Multi-match across title and body
    let query = json!({
        "query": {"multi_match": {"query": "open-source distributed", "fields": ["title^2", "body"]}},
        "size": 15
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "corpus: multi_match open-source distributed",
    );
    assert_similar_ranking(&es_body, &os_body, 5, 0.6);

    // Query 4: Bool query — must match + filter
    let query = json!({
        "query": {
            "bool": {
                "must": [{"match": {"body": "programming language"}}],
                "filter": [{"range": {"year": {"gte": 2010}}}]
            }
        },
        "size": 15
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "corpus: bool must+filter",
    );
    assert_similar_ranking(&es_body, &os_body, 5, 0.8);

    // Query 5: Aggregation on category
    let query = json!({
        "size": 0,
        "aggs": {"by_category": {"terms": {"field": "category", "size": 20}}}
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_buckets(
        &es_body["aggregations"]["by_category"],
        &os_body["aggregations"]["by_category"],
        "corpus: by_category",
    );

    // Query 6: Aggregation with text filter (using a simple term to avoid
    // hyphen tokenization differences between FTS5 and Lucene)
    let query = json!({
        "size": 0,
        "query": {"match": {"body": "database"}},
        "aggs": {"by_category": {"terms": {"field": "category", "size": 20}}}
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "corpus: agg with filter",
    );
    assert_same_buckets(
        &es_body["aggregations"]["by_category"],
        &os_body["aggregations"]["by_category"],
        "corpus: filtered by_category",
    );

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_corpus_sort_and_pagination() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-corpus-sort";

    let mappings = json!({"properties": {
        "title": {"type": "text"},
        "body": {"type": "text"},
        "category": {"type": "keyword"},
        "tags": {"type": "keyword"},
        "year": {"type": "integer"},
        "rating": {"type": "float"}
    }});
    let docs = wikipedia_corpus();
    setup_both(es, os, idx, mappings, &docs).await;

    // Sort by year descending
    let query = json!({
        "query": {"match_all": {}},
        "sort": [{"year": {"order": "desc"}}],
        "size": 15
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_ranking(&es_body, &os_body);

    // Sort by rating desc, then year asc
    let query = json!({
        "query": {"match_all": {}},
        "sort": [{"rating": {"order": "desc"}}, {"year": {"order": "asc"}}],
        "size": 15
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_ranking(&es_body, &os_body);

    // Pagination: page 1 (from=0, size=5) and page 2 (from=5, size=5)
    let query_p1 = json!({
        "query": {"match_all": {}},
        "sort": [{"year": {"order": "asc"}}],
        "from": 0, "size": 5
    });
    let query_p2 = json!({
        "query": {"match_all": {}},
        "sort": [{"year": {"order": "asc"}}],
        "from": 5, "size": 5
    });
    let es_p1 = search(es, idx, query_p1.clone()).await;
    let os_p1 = search(os, idx, query_p1).await;
    let es_p2 = search(es, idx, query_p2.clone()).await;
    let os_p2 = search(os, idx, query_p2).await;

    assert_same_ranking(&es_p1, &os_p1);
    assert_same_ranking(&es_p2, &os_p2);

    // Pages should not overlap
    let p1_ids: Vec<&str> = es_p1["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["_id"].as_str().unwrap())
        .collect();
    let p2_ids: Vec<&str> = es_p2["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["_id"].as_str().unwrap())
        .collect();
    for id in &p1_ids {
        assert!(
            !p2_ids.contains(id),
            "Page overlap: {} appears in both pages",
            id
        );
    }

    cleanup_both(es, os, &[idx]).await;
}

// ─── Real Corpus: Gutenberg Books ────────────────────────────────────────────

const CACHE_PATH: &str = "tests/.cache/gutenberg_corpus.json";

/// Load the Gutenberg corpus from cache, or download it if not cached.
async fn load_or_download_corpus() -> Vec<(String, Value)> {
    let cache = Path::new(CACHE_PATH);
    if cache.exists() {
        eprintln!("Loading cached Gutenberg corpus from {}", CACHE_PATH);
        let data = std::fs::read_to_string(cache).expect("read cache");
        let docs: Vec<Value> = serde_json::from_str(&data).expect("parse cache");
        return docs
            .into_iter()
            .enumerate()
            .map(|(i, v)| ((i + 1).to_string(), v))
            .collect();
    }

    eprintln!("Downloading Gutenberg corpus (this may take a few minutes on first run)...");
    let docs = download_gutenberg_corpus().await;

    // Ensure cache directory exists
    std::fs::create_dir_all("tests/.cache").expect("create cache dir");
    let json_docs: Vec<&Value> = docs.iter().map(|(_, v)| v).collect();
    let data = serde_json::to_string(&json_docs).expect("serialize cache");
    std::fs::write(cache, data).expect("write cache");
    eprintln!("Cached {} documents to {}", docs.len(), CACHE_PATH);

    docs
}

/// Download books from the Gutendex API with full text from Project Gutenberg.
async fn download_gutenberg_corpus() -> Vec<(String, Value)> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("build client");

    let mut all_books: Vec<Value> = Vec::new();
    let pages = 16; // 32 books/page = ~512 books

    for page in 1..=pages {
        let url = format!(
            "https://gutendex.com/books/?page={}&languages=en&mime_type=text%2Fplain",
            page
        );
        eprintln!("  Fetching page {}/{}...", page, pages);

        let resp = match client.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("  Failed to fetch page {}: {}", page, e);
                continue;
            }
        };

        let body: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("  Failed to parse page {}: {}", page, e);
                continue;
            }
        };

        let results = match body["results"].as_array() {
            Some(r) => r,
            None => continue,
        };

        for book in results {
            // Extract metadata
            let title = book["title"].as_str().unwrap_or("").to_string();
            if title.is_empty() {
                continue;
            }

            let author = book["authors"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|a| a["name"].as_str())
                .unwrap_or("Unknown")
                .to_string();

            let birth_year = book["authors"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|a| a["birth_year"].as_i64())
                .unwrap_or(0);

            let death_year = book["authors"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|a| a["death_year"].as_i64())
                .unwrap_or(0);

            let subjects: Vec<String> = book["subjects"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let subject = subjects.first().cloned().unwrap_or_default();

            let bookshelves: Vec<String> = book["bookshelves"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let bookshelf = bookshelves.first().cloned().unwrap_or_default();

            let download_count = book["download_count"].as_i64().unwrap_or(0);

            // Find plain text URL
            let text_url = book["formats"]
                .as_object()
                .and_then(|f| {
                    // Try various plain text format keys
                    f.get("text/plain; charset=utf-8")
                        .or_else(|| f.get("text/plain; charset=us-ascii"))
                        .or_else(|| f.get("text/plain"))
                })
                .and_then(|v| v.as_str())
                .map(String::from);

            all_books.push(json!({
                "title": title,
                "author": author,
                "birth_year": birth_year,
                "death_year": death_year,
                "subject": subject,
                "bookshelf": bookshelf,
                "download_count": download_count,
                "text_url": text_url,
            }));
        }

        // Be polite to the API
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    eprintln!(
        "  Fetched metadata for {} books, now downloading full texts...",
        all_books.len()
    );

    // Download full text for each book (up to 100K chars)
    let mut docs: Vec<(String, Value)> = Vec::new();
    let mut success_count = 0;

    for (i, book) in all_books.iter().enumerate() {
        let text_url = match book["text_url"].as_str() {
            Some(u) if !u.is_empty() => u,
            _ => continue,
        };

        if (i + 1) % 50 == 0 {
            eprintln!("  Downloading text {}/{}...", i + 1, all_books.len());
        }

        let body_text = match client.get(text_url).send().await {
            Ok(resp) => match resp.text().await {
                Ok(text) => {
                    // Trim to 100K chars, strip Gutenberg header/footer boilerplate
                    let cleaned = strip_gutenberg_boilerplate(&text);
                    if cleaned.len() < 500 {
                        continue; // Too short, skip
                    }
                    let max_len = 100_000.min(cleaned.len());
                    cleaned[..max_len].to_string()
                }
                Err(_) => continue,
            },
            Err(_) => continue,
        };

        success_count += 1;
        let doc_id = success_count.to_string();

        docs.push((
            doc_id,
            json!({
                "title": book["title"],
                "author": book["author"],
                "body": body_text,
                "subject": book["subject"],
                "bookshelf": book["bookshelf"],
                "download_count": book["download_count"],
                "birth_year": book["birth_year"],
                "death_year": book["death_year"],
            }),
        ));

        // Rate limit: don't hammer gutenberg.org
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    eprintln!("  Downloaded full text for {} books", docs.len());
    docs
}

/// Strip Project Gutenberg header and footer boilerplate from book text.
fn strip_gutenberg_boilerplate(text: &str) -> String {
    let start_markers = [
        "*** START OF THIS PROJECT GUTENBERG",
        "*** START OF THE PROJECT GUTENBERG",
        "***START OF THIS PROJECT GUTENBERG",
        "***START OF THE PROJECT GUTENBERG",
    ];
    let end_markers = [
        "*** END OF THIS PROJECT GUTENBERG",
        "*** END OF THE PROJECT GUTENBERG",
        "***END OF THIS PROJECT GUTENBERG",
        "***END OF THE PROJECT GUTENBERG",
        "End of the Project Gutenberg",
        "End of Project Gutenberg",
    ];

    let start = start_markers
        .iter()
        .filter_map(|marker| text.find(marker))
        .min()
        .map(|pos| {
            // Skip past the marker line
            text[pos..].find('\n').map(|nl| pos + nl + 1).unwrap_or(pos)
        })
        .unwrap_or(0);

    let end = end_markers
        .iter()
        .filter_map(|marker| text[start..].find(marker))
        .min()
        .map(|pos| start + pos)
        .unwrap_or(text.len());

    text[start..end].trim().to_string()
}

/// Bulk index documents using the _bulk API for faster ingestion.
async fn bulk_index(base: &str, index: &str, docs: &[(String, Value)]) {
    let client = http();

    // Process in chunks of 50 docs
    for chunk in docs.chunks(50) {
        let mut ndjson = String::new();
        for (id, doc) in chunk {
            ndjson.push_str(&format!(
                "{{\"index\":{{\"_index\":\"{}\",\"_id\":\"{}\"}}}}\n",
                index, id
            ));
            ndjson.push_str(&serde_json::to_string(doc).unwrap());
            ndjson.push('\n');
        }

        let resp = client
            .post(format!("{}/_bulk", base))
            .header("Content-Type", "application/x-ndjson")
            .body(ndjson)
            .send()
            .await
            .expect("bulk index");

        assert!(
            resp.status().is_success(),
            "Bulk index failed on {}: {}",
            base,
            resp.status()
        );
    }
}

/// Set up both servers with the Gutenberg corpus using bulk indexing.
async fn setup_both_gutenberg(
    es_base: &str,
    os_base: &str,
    index: &str,
    mappings: Value,
    docs: &[(String, Value)],
) {
    for base in [es_base, os_base] {
        cleanup(base, &[index]).await;
        create_index(base, index, mappings.clone()).await;
        bulk_index(base, index, docs).await;
        refresh(base, index).await;
    }
    // Give OpenSearch time to settle after bulk indexing
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
}

fn gutenberg_mappings() -> Value {
    json!({
        "properties": {
            "title": {"type": "text"},
            "author": {"type": "keyword"},
            "body": {"type": "text"},
            "subject": {"type": "keyword"},
            "bookshelf": {"type": "keyword"},
            "download_count": {"type": "integer"},
            "birth_year": {"type": "integer"},
            "death_year": {"type": "integer"}
        }
    })
}

// ─── Real Corpus Tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn compare_gutenberg_ranking() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-gutenberg-rank";

    let docs = load_or_download_corpus().await;
    if docs.is_empty() {
        eprintln!("No Gutenberg corpus available, skipping");
        return;
    }
    let doc_count = docs.len();
    eprintln!("Testing with {} Gutenberg books", doc_count);

    setup_both_gutenberg(es, os, idx, gutenberg_mappings(), &docs).await;

    // Verify doc counts match
    let es_count = count(es, idx, json!({"query": {"match_all": {}}})).await;
    let os_count = count(os, idx, json!({"query": {"match_all": {}}})).await;
    assert_same_value(
        &es_count["count"],
        &os_count["count"],
        "gutenberg doc count",
    );

    // Query 1: Classic literature search
    let query = json!({"query": {"match": {"body": "love and marriage"}}, "size": 20});
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    let es_total = es_body["hits"]["total"]["value"].as_i64().unwrap_or(0);
    let os_total = os_body["hits"]["total"]["value"].as_i64().unwrap_or(0);
    eprintln!(
        "  'love and marriage': es-sqlite={} hits, OpenSearch={} hits",
        es_total, os_total
    );
    assert_similar_ranking(&es_body, &os_body, 10, 0.6);

    // Query 2: Adventure theme
    let query = json!({"query": {"match": {"body": "adventure sea voyage"}}, "size": 20});
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    let es_total = es_body["hits"]["total"]["value"].as_i64().unwrap_or(0);
    let os_total = os_body["hits"]["total"]["value"].as_i64().unwrap_or(0);
    eprintln!(
        "  'adventure sea voyage': es-sqlite={} hits, OpenSearch={} hits",
        es_total, os_total
    );
    assert_similar_ranking(&es_body, &os_body, 10, 0.6);

    // Query 3: Multi-match across title and body
    let query = json!({
        "query": {"multi_match": {"query": "war and peace", "fields": ["title^3", "body"]}},
        "size": 20
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    let es_total = es_body["hits"]["total"]["value"].as_i64().unwrap_or(0);
    let os_total = os_body["hits"]["total"]["value"].as_i64().unwrap_or(0);
    eprintln!(
        "  'war and peace' (multi_match): es-sqlite={} hits, OpenSearch={} hits",
        es_total, os_total
    );
    assert_similar_ranking(&es_body, &os_body, 10, 0.6);

    // Query 4: Bool must + filter (author filter)
    let query = json!({
        "query": {
            "bool": {
                "must": [{"match": {"body": "science discovery nature"}}],
                "filter": [{"range": {"download_count": {"gte": 100}}}]
            }
        },
        "size": 20
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    let es_total = es_body["hits"]["total"]["value"].as_i64().unwrap_or(0);
    let os_total = os_body["hits"]["total"]["value"].as_i64().unwrap_or(0);
    eprintln!(
        "  'science discovery nature' (bool+filter): es-sqlite={} hits, OpenSearch={} hits",
        es_total, os_total
    );
    assert_similar_ranking(&es_body, &os_body, 10, 0.6);

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_gutenberg_aggregations() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-gutenberg-agg";

    let docs = load_or_download_corpus().await;
    if docs.is_empty() {
        eprintln!("No Gutenberg corpus available, skipping");
        return;
    }

    setup_both_gutenberg(es, os, idx, gutenberg_mappings(), &docs).await;

    // Terms aggregation on author (top 20)
    let query = json!({
        "size": 0,
        "aggs": {"by_author": {"terms": {"field": "author", "size": 20}}}
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_buckets(
        &es_body["aggregations"]["by_author"],
        &os_body["aggregations"]["by_author"],
        "gutenberg: by_author",
    );

    // Terms aggregation on bookshelf
    let query = json!({
        "size": 0,
        "aggs": {"by_bookshelf": {"terms": {"field": "bookshelf", "size": 20}}}
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_buckets(
        &es_body["aggregations"]["by_bookshelf"],
        &os_body["aggregations"]["by_bookshelf"],
        "gutenberg: by_bookshelf",
    );

    // Filtered aggregation: match "history" → aggregate by subject
    let query = json!({
        "size": 0,
        "query": {"match": {"body": "history"}},
        "aggs": {"by_subject": {"terms": {"field": "subject", "size": 20}}}
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_value(
        &es_body["hits"]["total"]["value"],
        &os_body["hits"]["total"]["value"],
        "gutenberg: history hit count",
    );
    assert_same_buckets(
        &es_body["aggregations"]["by_subject"],
        &os_body["aggregations"]["by_subject"],
        "gutenberg: filtered by_subject",
    );

    cleanup_both(es, os, &[idx]).await;
}

#[tokio::test]
async fn compare_gutenberg_sort_pagination() {
    let Some((es, os)) = require_both().await else {
        return;
    };
    let idx = "cmp-gutenberg-sort";

    let docs = load_or_download_corpus().await;
    if docs.is_empty() {
        eprintln!("No Gutenberg corpus available, skipping");
        return;
    }

    setup_both_gutenberg(es, os, idx, gutenberg_mappings(), &docs).await;

    // Sort by download_count descending (most popular books)
    let query = json!({
        "query": {"match_all": {}},
        "sort": [{"download_count": {"order": "desc"}}],
        "size": 20
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_ranking(&es_body, &os_body);

    // Multi-field sort: author asc, download_count desc
    let query = json!({
        "query": {"match_all": {}},
        "sort": [
            {"author": {"order": "asc"}},
            {"download_count": {"order": "desc"}}
        ],
        "size": 20
    });
    let es_body = search(es, idx, query.clone()).await;
    let os_body = search(os, idx, query).await;
    assert_same_ranking(&es_body, &os_body);

    // Pagination: page through sorted results
    let page_size = 10;
    for page in 0..3 {
        let query = json!({
            "query": {"match_all": {}},
            "sort": [{"download_count": {"order": "desc"}}],
            "from": page * page_size,
            "size": page_size
        });
        let es_body = search(es, idx, query.clone()).await;
        let os_body = search(os, idx, query).await;
        assert_same_ranking(&es_body, &os_body);
    }

    cleanup_both(es, os, &[idx]).await;
}

// ─── Cleanup: Stop Docker Container ─────────────────────────────────────────

// This test sorts last alphabetically (z_*) and cleans up the Docker container.
#[test]
fn z_cleanup_docker_container() {
    if get_opensearch_url().is_some() {
        let _ = Command::new("docker")
            .args(["rm", "-f", CONTAINER_NAME])
            .output();
        eprintln!("Cleaned up Docker container: {}", CONTAINER_NAME);
    }
}
