//! Jaeger compatibility test: verifies es-qlite can serve as Jaeger's
//! Elasticsearch storage backend.
//!
//! Sends OTLP traces to Jaeger (backed by es-qlite), then queries them
//! through both Jaeger's Query API and directly against es-qlite's ES API.
//!
//! Run with:
//!   cargo test --test jaeger_compat -- --test-threads=1
//!
//! Requires Docker. Tests are skipped gracefully if Docker is unavailable.

mod common;

use reqwest::Client;
use serde_json::{json, Value};
use std::process::Command;
use std::sync::OnceLock;

const CONTAINER_NAME: &str = "es-qlite-test-jaeger";
const JAEGER_IMAGE: &str = "jaegertracing/all-in-one:1.60";

struct JaegerPorts {
    query: u16, // 16686 — UI / Query API
    otlp: u16,  // 4318  — OTLP HTTP receiver
}

static JAEGER_PORTS: OnceLock<Option<JaegerPorts>> = OnceLock::new();

// ─── Docker Jaeger Management ────────────────────────────────────────────

fn find_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind for port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Start a Jaeger all-in-one Docker container backed by es-qlite.
/// Returns None if Docker is not available.
fn ensure_jaeger_sync(es_qlite_url: &str) -> Option<JaegerPorts> {
    let docker_check = Command::new("docker").arg("info").output();
    if docker_check.is_err() || !docker_check.unwrap().status.success() {
        eprintln!("Docker not available, skipping Jaeger compat tests");
        return None;
    }

    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();

    let query_port = find_available_port();
    let otlp_port = find_available_port();

    let es_url_for_docker = es_qlite_url.replace("127.0.0.1", "host.docker.internal");

    let result = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            &format!("{query_port}:16686"),
            "-p",
            &format!("{otlp_port}:4318"),
            "-e",
            "SPAN_STORAGE_TYPE=elasticsearch",
            "-e",
            &format!("ES_SERVER_URLS={es_url_for_docker}"),
            "-e",
            "ES_TAGS_AS_FIELDS_ALL=true",
            "-e",
            "ES_CREATE_INDEX_TEMPLATES=false",
            JAEGER_IMAGE,
        ])
        .output();

    match result {
        Ok(output) if output.status.success() => {
            eprintln!("Started Jaeger container (query={query_port}, otlp={otlp_port})");
        }
        Ok(output) => {
            eprintln!(
                "Failed to start Jaeger container: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            return None;
        }
        Err(e) => {
            eprintln!("Failed to run docker: {e}");
            return None;
        }
    }

    let query_url = format!("http://127.0.0.1:{query_port}");
    for i in 0..60 {
        let check = Command::new("curl")
            .args(["-sf", "--max-time", "2", &query_url])
            .output();
        if let Ok(output) = check {
            if output.status.success() {
                eprintln!("Jaeger ready after ~{i}s");
                return Some(JaegerPorts {
                    query: query_port,
                    otlp: otlp_port,
                });
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    eprintln!("Jaeger failed to start within 60s");
    let logs = Command::new("docker")
        .args(["logs", "--tail", "50", CONTAINER_NAME])
        .output();
    if let Ok(output) = logs {
        eprintln!(
            "Jaeger logs:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();
    None
}

// ─── Test Setup ──────────────────────────────────────────────────────────

async fn setup() -> Option<(String, &'static JaegerPorts)> {
    let es_url = common::ensure_server().await;
    let ports = JAEGER_PORTS.get_or_init(|| ensure_jaeger_sync(es_url));
    _CLEANUP.get_or_init(|| cleanup::JaegerCleanup);

    match ports.as_ref() {
        Some(p) => Some((es_url.to_string(), p)),
        None => {
            eprintln!("Skipping Jaeger compat test (Docker unavailable)");
            None
        }
    }
}

// ─── OTLP Trace Helpers ─────────────────────────────────────────────────

fn make_trace_payload(service_name: &str, spans: Vec<Value>) -> Value {
    json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeSpans": [{
                "scope": {
                    "name": "test-library",
                    "version": "1.0.0"
                },
                "spans": spans
            }]
        }]
    })
}

fn make_span(
    trace_id: &str,
    span_id: &str,
    parent_span_id: &str,
    name: &str,
    kind: u32,
    start_ns: &str,
    end_ns: &str,
) -> Value {
    let mut span = json!({
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": kind,
        "startTimeUnixNano": start_ns,
        "endTimeUnixNano": end_ns,
        "attributes": [],
        "status": {}
    });
    if !parent_span_id.is_empty() {
        span["parentSpanId"] = json!(parent_span_id);
    }
    span
}

// ─── Cleanup ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod cleanup {
    use std::process::Command;

    pub struct JaegerCleanup;

    impl Drop for JaegerCleanup {
        fn drop(&mut self) {
            eprintln!("Cleaning up Jaeger container...");
            let _ = Command::new("docker")
                .args(["rm", "-f", super::CONTAINER_NAME])
                .output();
        }
    }
}

static _CLEANUP: std::sync::OnceLock<cleanup::JaegerCleanup> = std::sync::OnceLock::new();

// ─── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_jaeger_receives_and_queries_traces() {
    let (es_url, ports) = match setup().await {
        Some(v) => v,
        None => return,
    };

    let client = Client::new();
    let otlp_url = format!("http://127.0.0.1:{}/v1/traces", ports.otlp);
    let query_base = format!("http://127.0.0.1:{}", ports.query);

    // ── 1. Send traces for two services ──────────────────────────────

    let trace_id = "aaaabbbbccccdddd1111222233334444";

    // Use current time so Jaeger's time-based index lookup finds them
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let start_ns = now_ns.to_string();
    let end_ns = (now_ns + 1_000_000_000).to_string();

    // Service A: gateway — root span + child span
    let payload_a = make_trace_payload(
        "gateway-service",
        vec![
            make_span(
                trace_id,
                "1111111111111111",
                "",
                "HTTP GET /api/users",
                2, // SERVER
                &start_ns,
                &end_ns,
            ),
            make_span(
                trace_id,
                "2222222222222222",
                "1111111111111111",
                "auth-check",
                1, // INTERNAL
                &start_ns,
                &end_ns,
            ),
        ],
    );

    // Service B: user-service — downstream call
    let payload_b = make_trace_payload(
        "user-service",
        vec![make_span(
            trace_id,
            "3333333333333333",
            "1111111111111111",
            "DB query users",
            3, // CLIENT
            &start_ns,
            &end_ns,
        )],
    );

    for (label, payload) in [("gateway", &payload_a), ("user-svc", &payload_b)] {
        let resp = client
            .post(&otlp_url)
            .header("Content-Type", "application/json")
            .json(payload)
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to send OTLP traces for {label}: {e}"));

        assert!(
            resp.status().is_success(),
            "OTLP POST for {label} failed: {}",
            resp.status(),
        );
    }

    // Wait for Jaeger's BulkProcessor to flush spans to ES
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // ── 2. Query Jaeger: list services ───────────────────────────────

    let services_resp = client
        .get(format!("{query_base}/api/services"))
        .send()
        .await
        .expect("GET /api/services");

    assert!(
        services_resp.status().is_success(),
        "GET /api/services returned {}",
        services_resp.status()
    );

    let services_body: Value = services_resp.json().await.expect("parse services JSON");
    let services = services_body["data"]
        .as_array()
        .expect("services.data should be an array");

    assert!(
        services
            .iter()
            .any(|s| s.as_str() == Some("gateway-service")),
        "Expected gateway-service in Jaeger services, got: {services:?}"
    );
    assert!(
        services.iter().any(|s| s.as_str() == Some("user-service")),
        "Expected user-service in Jaeger services, got: {services:?}"
    );

    // ── 3. Query Jaeger: find traces for gateway-service ─────────────

    let traces_resp = client
        .get(format!(
            "{query_base}/api/traces?service=gateway-service&limit=10&lookback=1h"
        ))
        .send()
        .await
        .expect("GET /api/traces");

    assert!(
        traces_resp.status().is_success(),
        "GET /api/traces returned {}",
        traces_resp.status()
    );

    let traces_body: Value = traces_resp.json().await.expect("parse traces JSON");
    let traces = traces_body["data"]
        .as_array()
        .expect("traces.data should be an array");

    assert!(
        !traces.is_empty(),
        "Expected at least one trace, got none. Response: {traces_body}"
    );

    let first_trace = &traces[0];
    let spans = first_trace["spans"]
        .as_array()
        .expect("trace should have spans array");

    assert!(
        spans.len() >= 2,
        "Expected at least 2 spans in trace, got {}",
        spans.len()
    );

    // ── 4. Verify ES indices on es-qlite directly ────────────────────

    let cat_resp = client
        .get(format!("{es_url}/_cat/indices?format=json"))
        .send()
        .await
        .expect("GET _cat/indices");

    let indices: Value = cat_resp.json().await.expect("parse _cat/indices");
    let indices_arr = indices.as_array().expect("_cat/indices should be array");
    let index_names: Vec<&str> = indices_arr
        .iter()
        .filter_map(|idx| idx["index"].as_str())
        .collect();

    assert!(
        index_names.iter().any(|n| n.contains("jaeger-span")),
        "Expected jaeger-span-* index, found: {index_names:?}"
    );
    assert!(
        index_names.iter().any(|n| n.contains("jaeger-service")),
        "Expected jaeger-service-* index, found: {index_names:?}"
    );

    // Search span index directly to verify document count
    let span_index = index_names
        .iter()
        .find(|n| n.contains("jaeger-span"))
        .unwrap();

    let search_resp = client
        .post(format!("{es_url}/{span_index}/_search"))
        .json(&json!({"query": {"match_all": {}}, "size": 100}))
        .send()
        .await
        .expect("search jaeger-span index");

    let search_body: Value = search_resp.json().await.expect("parse search response");
    let total_hits = search_body["hits"]["total"]["value"].as_u64().unwrap_or(0);

    assert!(
        total_hits >= 3,
        "Expected at least 3 span docs in ES (sent 3 spans), got {total_hits}"
    );

    eprintln!("All Jaeger compatibility checks passed!");
}
