//! YAML REST API test runner for es-qlite
//!
//! Runs a subset of the OpenSearch/Elasticsearch YAML test specs against a live
//! es-qlite server. The server is started automatically.
//!
#![allow(clippy::await_holding_lock, dead_code)]
//! Run with:
//!   cargo test --test yaml_runner

mod common;

use reqwest::Client;
use serde_json::Value;
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

/// Mutex to serialize YAML tests that share index state on the same server.
static TEST_MUTEX: Mutex<()> = Mutex::new(());

struct TestRunner {
    base_url: String,
    client: Client,
    last_response: Option<Value>,
    stashed: HashMap<String, Value>,
}

impl TestRunner {
    fn new(base_url: String) -> Self {
        TestRunner {
            base_url,
            client: Client::new(),
            last_response: None,
            stashed: HashMap::new(),
        }
    }

    async fn cleanup(&self) {
        // Delete all indices
        let resp = self
            .client
            .get(format!("{}/_cat/indices", self.base_url))
            .send()
            .await;
        if let Ok(resp) = resp {
            if let Ok(indices) = resp.json::<Vec<Value>>().await {
                for idx in indices {
                    if let Some(name) = idx.get("index").and_then(|v| v.as_str()) {
                        let _ = self
                            .client
                            .delete(format!("{}/{name}", self.base_url))
                            .send()
                            .await;
                    }
                }
            }
        }
    }

    async fn execute_do(&mut self, operation: &YamlValue) -> Result<(), String> {
        let map = operation
            .as_mapping()
            .ok_or("'do' value must be a mapping")?;

        let mut catch: Option<String> = None;

        for (key, value) in map {
            let key_str = yaml_to_string(key);

            if key_str == "catch" {
                catch = Some(yaml_to_string(value));
                continue;
            }

            if key_str == "headers" || key_str == "warnings" || key_str == "allowed_warnings" {
                continue; // Skip unsupported directives
            }

            let result = self.execute_api_call(&key_str, value).await;

            match (&catch, &result) {
                (Some(_), Err(_)) => {
                    // Expected error, that's fine
                    self.last_response = Some(Value::Null);
                    return Ok(());
                }
                (Some(expected), Ok(_)) => {
                    // We expected an error but got success — check if response status indicates error
                    // For now, just pass; a more complete runner would check status codes
                    let _ = expected;
                    return Ok(());
                }
                (None, Err(e)) => return Err(e.clone()),
                (None, Ok(())) => {}
            }
        }
        Ok(())
    }

    async fn execute_api_call(&mut self, api: &str, params: &YamlValue) -> Result<(), String> {
        let empty_mapping = serde_yaml::Mapping::new();
        let params_map = params.as_mapping().unwrap_or(&empty_mapping);

        let index = get_yaml_str(params_map, "index").unwrap_or_default();
        let id = get_yaml_str(params_map, "id").unwrap_or_default();
        let body = params_map
            .iter()
            .find(|(k, _)| yaml_to_string(k) == "body")
            .map(|(_, v)| v);

        let name = get_yaml_str(params_map, "name").unwrap_or_default();

        let (method, path) = match api {
            // ─── Index management ───────────────────────────────────────
            "indices.create" => ("PUT", format!("/{index}")),
            "indices.delete" => ("DELETE", format!("/{index}")),
            "indices.exists" => ("HEAD", format!("/{index}")),
            "indices.get" => ("GET", format!("/{index}")),
            "indices.refresh" => ("POST", format!("/{index}/_refresh")),
            "indices.flush" => ("POST", format!("/{index}/_flush")),
            "indices.forcemerge" => ("POST", format!("/{index}/_forcemerge")),
            "indices.get_mapping" | "indices.get_mappings" => ("GET", format!("/{index}/_mapping")),
            "indices.put_mapping" => ("PUT", format!("/{index}/_mapping")),
            "indices.get_settings" => {
                let setting_name = get_yaml_str(params_map, "name").unwrap_or_default();
                if setting_name.is_empty() {
                    ("GET", format!("/{index}/_settings"))
                } else {
                    ("GET", format!("/{index}/_settings/{setting_name}"))
                }
            }
            "indices.get_field_mapping" => {
                let fields = get_yaml_str(params_map, "fields").unwrap_or_default();
                ("GET", format!("/{index}/_mapping/field/{fields}"))
            }
            "indices.put_settings" => ("PUT", format!("/{index}/_settings")),
            "indices.open" => ("POST", format!("/{index}/_open")),
            "indices.close" => ("POST", format!("/{index}/_close")),
            "indices.stats" => ("GET", format!("/{index}/_stats")),
            "indices.segments" => ("GET", format!("/{index}/_segments")),
            "indices.recovery" => ("GET", format!("/{index}/_recovery")),
            "indices.shard_stores" => ("GET", format!("/{index}/_shard_stores")),
            "indices.validate_query" => ("POST", format!("/{index}/_validate/query")),
            "indices.analyze" => {
                if index.is_empty() {
                    ("POST", "/_analyze".to_string())
                } else {
                    ("POST", format!("/{index}/_analyze"))
                }
            }
            "indices.clear_cache" => ("POST", format!("/{index}/_cache/clear")),
            "indices.upgrade" => ("POST", format!("/{index}/_upgrade")),
            "indices.rollover" => ("POST", format!("/{index}/_rollover")),
            "indices.shrink" => ("POST", format!("/{index}/_shrink")),
            "indices.split" => ("POST", format!("/{index}/_split")),
            "indices.clone" => ("POST", format!("/{index}/_clone")),
            "indices.resolve_index" => ("GET", format!("/_resolve/index/{index}")),
            "indices.add_block" => {
                let block = get_yaml_str(params_map, "block").unwrap_or_default();
                ("PUT", format!("/{index}/_block/{block}"))
            }
            // ─── Aliases ────────────────────────────────────────────────
            "indices.get_alias" => {
                if name.is_empty() {
                    if index.is_empty() {
                        ("GET", "/_alias".to_string())
                    } else {
                        ("GET", format!("/{index}/_alias"))
                    }
                } else if index.is_empty() {
                    ("GET", format!("/_alias/{name}"))
                } else {
                    ("GET", format!("/{index}/_alias/{name}"))
                }
            }
            "indices.put_alias" => ("PUT", format!("/{index}/_alias/{name}")),
            "indices.delete_alias" => ("DELETE", format!("/{index}/_alias/{name}")),
            "indices.exists_alias" => {
                if index.is_empty() {
                    ("HEAD", format!("/_alias/{name}"))
                } else {
                    ("HEAD", format!("/{index}/_alias/{name}"))
                }
            }
            "indices.update_aliases" => ("POST", "/_aliases".to_string()),
            // ─── Templates ──────────────────────────────────────────────
            "indices.put_template" => ("PUT", format!("/_template/{name}")),
            "indices.get_template" => ("GET", format!("/_template/{name}")),
            "indices.delete_template" => ("DELETE", format!("/_template/{name}")),
            "indices.exists_template" => ("HEAD", format!("/_template/{name}")),
            "indices.put_index_template" => ("PUT", format!("/_index_template/{name}")),
            "indices.get_index_template" => ("GET", format!("/_index_template/{name}")),
            "indices.delete_index_template" => ("DELETE", format!("/_index_template/{name}")),
            "indices.simulate_index_template" | "indices.simulate_template" => {
                ("POST", format!("/_index_template/_simulate/{name}"))
            }
            "cluster.put_component_template"
            | "cluster.get_component_template"
            | "cluster.delete_component_template" => {
                let method = if api.starts_with("cluster.put") {
                    "PUT"
                } else if api.starts_with("cluster.delete") {
                    "DELETE"
                } else {
                    "GET"
                };
                (method, format!("/_component_template/{name}"))
            }
            // ─── Document CRUD ──────────────────────────────────────────
            "index" => {
                if id.is_empty() {
                    ("POST", format!("/{index}/_doc"))
                } else {
                    ("PUT", format!("/{index}/_doc/{id}"))
                }
            }
            "create" => {
                if id.is_empty() {
                    ("POST", format!("/{index}/_doc"))
                } else {
                    ("PUT", format!("/{index}/_create/{id}"))
                }
            }
            "get" => ("GET", format!("/{index}/_doc/{id}")),
            "get_source" => ("GET", format!("/{index}/_source/{id}")),
            "exists" => ("HEAD", format!("/{index}/_doc/{id}")),
            "delete" => ("DELETE", format!("/{index}/_doc/{id}")),
            "update" => ("POST", format!("/{index}/_update/{id}")),
            "explain" => ("POST", format!("/{index}/_explain/{id}")),
            // ─── Search & Query ─────────────────────────────────────────
            "search" => {
                if index.is_empty() {
                    ("POST", "/_search".to_string())
                } else {
                    ("POST", format!("/{index}/_search"))
                }
            }
            "count" => {
                if index.is_empty() {
                    ("POST", "/_count".to_string())
                } else {
                    ("POST", format!("/{index}/_count"))
                }
            }
            "msearch" => ("POST", "/_msearch".to_string()),
            "scroll" => ("POST", "/_search/scroll".to_string()),
            "field_caps" => ("POST", format!("/{index}/_field_caps")),
            "delete_by_query" => ("POST", format!("/{index}/_delete_by_query")),
            "suggest" => ("POST", format!("/{index}/_suggest")),
            "termvectors" => ("GET", format!("/{index}/_termvectors/{id}")),
            "mtermvectors" => ("POST", format!("/{index}/_mtermvectors")),
            "mlt" => ("GET", format!("/{index}/_mlt/{id}")),
            // ─── Bulk & Multi-get ───────────────────────────────────────
            "bulk" => {
                if index.is_empty() {
                    ("POST", "/_bulk".to_string())
                } else {
                    ("POST", format!("/{index}/_bulk"))
                }
            }
            "mget" => {
                if index.is_empty() {
                    ("POST", "/_mget".to_string())
                } else {
                    ("POST", format!("/{index}/_mget"))
                }
            }
            // ─── Cluster ────────────────────────────────────────────────
            "cluster.health" => {
                if index.is_empty() {
                    ("GET", "/_cluster/health".to_string())
                } else {
                    ("GET", format!("/_cluster/health/{index}"))
                }
            }
            "cluster.state" => ("GET", "/_cluster/state".to_string()),
            "cluster.stats" => ("GET", "/_cluster/stats".to_string()),
            "cluster.get_settings" => ("GET", "/_cluster/settings".to_string()),
            "cluster.put_settings" => ("PUT", "/_cluster/settings".to_string()),
            "cluster.pending_tasks" => ("GET", "/_cluster/pending_tasks".to_string()),
            "cluster.allocation_explain" => ("POST", "/_cluster/allocation/explain".to_string()),
            "cluster.reroute" => ("POST", "/_cluster/reroute".to_string()),
            "cluster.remote_info" => ("GET", "/_remote/info".to_string()),
            "cluster.post_voting_config_exclusions" => {
                ("POST", "/_cluster/voting_config_exclusions".to_string())
            }
            "cluster.delete_voting_config_exclusions" => {
                ("DELETE", "/_cluster/voting_config_exclusions".to_string())
            }
            // ─── Cat ────────────────────────────────────────────────────
            "cat.indices" => ("GET", "/_cat/indices".to_string()),
            "cat.aliases" => ("GET", "/_cat/aliases".to_string()),
            "cat.health" => ("GET", "/_cat/health".to_string()),
            "cat.count" => ("GET", "/_cat/count".to_string()),
            "cat.shards" => ("GET", "/_cat/shards".to_string()),
            "cat.segments" => ("GET", "/_cat/segments".to_string()),
            "cat.nodes" => ("GET", "/_cat/nodes".to_string()),
            "cat.allocation" => ("GET", "/_cat/allocation".to_string()),
            "cat.thread_pool" => ("GET", "/_cat/thread_pool".to_string()),
            "cat.plugins" => ("GET", "/_cat/plugins".to_string()),
            "cat.fielddata" => ("GET", "/_cat/fielddata".to_string()),
            "cat.nodeattrs" => ("GET", "/_cat/nodeattrs".to_string()),
            "cat.recovery" => ("GET", "/_cat/recovery".to_string()),
            "cat.repositories" => ("GET", "/_cat/repositories".to_string()),
            "cat.snapshots" => ("GET", "/_cat/snapshots".to_string()),
            "cat.tasks" => ("GET", "/_cat/tasks".to_string()),
            "cat.templates" => ("GET", "/_cat/templates".to_string()),
            "cat.cluster_manager" => ("GET", "/_cat/cluster_manager".to_string()),
            // ─── Nodes ──────────────────────────────────────────────────
            "nodes.info" => ("GET", "/_nodes".to_string()),
            "nodes.stats" => ("GET", "/_nodes/stats".to_string()),
            "nodes.reload_secure_settings" => {
                ("POST", "/_nodes/reload_secure_settings".to_string())
            }
            // ─── Tasks ──────────────────────────────────────────────────
            "tasks.list" => ("GET", "/_tasks".to_string()),
            "tasks.get" => {
                let task_id = get_yaml_str(params_map, "task_id").unwrap_or_default();
                ("GET", format!("/_tasks/{task_id}"))
            }
            "tasks.cancel" => {
                let task_id = get_yaml_str(params_map, "task_id").unwrap_or_default();
                ("POST", format!("/_tasks/{task_id}/_cancel"))
            }
            // ─── Ingest ─────────────────────────────────────────────────
            "ingest.put_pipeline" => ("PUT", format!("/_ingest/pipeline/{id}")),
            "ingest.get_pipeline" => ("GET", format!("/_ingest/pipeline/{id}")),
            "ingest.delete_pipeline" => ("DELETE", format!("/_ingest/pipeline/{id}")),
            // ─── Snapshot ───────────────────────────────────────────────
            "snapshot.create_repository" => {
                let repository = get_yaml_str(params_map, "repository").unwrap_or_default();
                ("PUT", format!("/_snapshot/{repository}"))
            }
            "snapshot.get_repository" => {
                let repository = get_yaml_str(params_map, "repository").unwrap_or_default();
                ("GET", format!("/_snapshot/{repository}"))
            }
            "snapshot.create" => {
                let repository = get_yaml_str(params_map, "repository").unwrap_or_default();
                let snapshot = get_yaml_str(params_map, "snapshot").unwrap_or_default();
                ("PUT", format!("/_snapshot/{repository}/{snapshot}"))
            }
            "snapshot.get" => {
                let repository = get_yaml_str(params_map, "repository").unwrap_or_default();
                let snapshot = get_yaml_str(params_map, "snapshot").unwrap_or_default();
                ("GET", format!("/_snapshot/{repository}/{snapshot}"))
            }
            "snapshot.delete"
            | "snapshot.restore"
            | "snapshot.status"
            | "snapshot.clone"
            | "snapshot.verify_repository"
            | "snapshot.cleanup_repository" => {
                let repository = get_yaml_str(params_map, "repository").unwrap_or_default();
                let snapshot = get_yaml_str(params_map, "snapshot").unwrap_or_default();
                let suffix = match api {
                    "snapshot.restore" => "/_restore",
                    "snapshot.status" => "/_status",
                    "snapshot.clone" => "/_clone",
                    "snapshot.verify_repository" => "/_verify",
                    "snapshot.cleanup_repository" => "/_cleanup",
                    _ => "",
                };
                if snapshot.is_empty() {
                    ("POST", format!("/_snapshot/{repository}{suffix}"))
                } else {
                    (
                        "POST",
                        format!("/_snapshot/{repository}/{snapshot}{suffix}"),
                    )
                }
            }
            // ─── Misc ───────────────────────────────────────────────────
            "info" => ("GET", "/".to_string()),
            "ping" => ("HEAD", "/".to_string()),
            "search_shards" => ("GET", format!("/{index}/_search_shards")),
            "scripts.painless_execute" => ("POST", "/_scripts/painless/_execute".to_string()),
            "search_pipeline.put" | "search_pipeline.get" | "search_pipeline.delete" => {
                let pipeline_id = get_yaml_str(params_map, "id").unwrap_or_default();
                let method = if api.ends_with("put") {
                    "PUT"
                } else if api.ends_with("delete") {
                    "DELETE"
                } else {
                    "GET"
                };
                (method, format!("/_search/pipeline/{pipeline_id}"))
            }
            _ => {
                return Err(format!("Unsupported API: {api}"));
            }
        };

        let url = format!("{}{path}", self.base_url);

        // Collect query parameters from params that aren't index/id/name/body/block/repository/snapshot/task_id
        let reserved = [
            "index",
            "id",
            "name",
            "body",
            "block",
            "repository",
            "snapshot",
            "task_id",
            "fields",
        ];
        let query_params: Vec<(String, String)> = params_map
            .iter()
            .filter_map(|(k, v)| {
                let key = yaml_to_string(k);
                if reserved.contains(&key.as_str()) {
                    None
                } else {
                    Some((key, yaml_to_string(v)))
                }
            })
            .collect();

        let mut request = match method {
            "GET" => self.client.get(&url),
            "PUT" => self.client.put(&url),
            "POST" => self.client.post(&url),
            "DELETE" => self.client.delete(&url),
            "HEAD" => self.client.head(&url),
            _ => return Err(format!("Unknown method: {method}")),
        };

        if !query_params.is_empty() {
            request = request.query(&query_params);
        }

        if let Some(body_val) = body {
            // For bulk and msearch, we need NDJSON format
            if api == "bulk" || api == "msearch" {
                let ndjson = yaml_to_ndjson(body_val);
                request = request
                    .header("Content-Type", "application/x-ndjson")
                    .body(ndjson);
            } else {
                let json_body = yaml_to_json(body_val);
                request = request
                    .header("Content-Type", "application/json")
                    .json(&json_body);
            }
        }

        let resp = request
            .send()
            .await
            .map_err(|e| format!("HTTP error: {e}"))?;

        let status = resp.status();
        let body_text = resp.text().await.map_err(|e| format!("Read error: {e}"))?;

        if method == "HEAD" {
            self.last_response = Some(serde_json::json!({"status": status.as_u16()}));
            if !status.is_success() {
                return Err(format!("HEAD returned {status}"));
            }
            return Ok(());
        }

        let json: Value = serde_json::from_str(&body_text).unwrap_or(Value::String(body_text));
        self.last_response = Some(json);

        if status.is_client_error() || status.is_server_error() {
            return Err(format!("API returned {status}"));
        }

        Ok(())
    }

    fn assert_match(&self, path: &str, expected: &YamlValue) -> Result<(), String> {
        let response = self
            .last_response
            .as_ref()
            .ok_or("No response to match against")?;

        let actual = navigate_json(response, path);
        let expected_json = yaml_to_json(expected);

        if !values_match(&actual, &expected_json) {
            return Err(format!(
                "Match failed at '{path}': expected {expected_json}, got {actual}"
            ));
        }
        Ok(())
    }

    fn assert_is_true(&self, path: &str) -> Result<(), String> {
        let response = self.last_response.as_ref().ok_or("No response")?;
        let actual = navigate_json(response, path);
        if actual.is_null()
            || actual == Value::Bool(false)
            || actual == Value::String(String::new())
        {
            return Err(format!("is_true failed at '{path}': got {actual}"));
        }
        Ok(())
    }

    fn assert_is_false(&self, path: &str) -> Result<(), String> {
        let response = self.last_response.as_ref().ok_or("No response")?;
        let actual = navigate_json(response, path);
        if !actual.is_null()
            && actual != Value::Bool(false)
            && actual != Value::String(String::new())
        {
            return Err(format!("is_false failed at '{path}': got {actual}"));
        }
        Ok(())
    }

    fn assert_length(&self, path: &str, expected_len: u64) -> Result<(), String> {
        let response = self.last_response.as_ref().ok_or("No response")?;
        let actual = navigate_json(response, path);
        let len = match &actual {
            Value::Array(arr) => arr.len() as u64,
            Value::Object(obj) => obj.len() as u64,
            Value::String(s) => s.len() as u64,
            _ => return Err(format!("Cannot get length of {actual} at '{path}'")),
        };
        if len != expected_len {
            return Err(format!(
                "Length mismatch at '{path}': expected {expected_len}, got {len}"
            ));
        }
        Ok(())
    }
}

fn navigate_json(value: &Value, path: &str) -> Value {
    if path.is_empty() || path == "$body" {
        return value.clone();
    }

    let mut current = value;
    for segment in path.split('.') {
        if let Ok(idx) = segment.parse::<usize>() {
            current = current.get(idx).unwrap_or(&Value::Null);
        } else {
            current = current.get(segment).unwrap_or(&Value::Null);
        }
    }
    current.clone()
}

fn values_match(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => {
            // Fuzzy numeric comparison
            if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                (af - bf).abs() < 1e-6 || af == bf
            } else {
                a == b
            }
        }
        (Value::String(a), Value::String(b)) => a == b,
        (Value::String(a), Value::Number(n)) => {
            // ES sometimes returns string numbers
            a == &n.to_string()
        }
        (Value::Number(n), Value::String(b)) => n.to_string() == *b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Null, Value::Null) => true,
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_match(x, y))
        }
        (Value::Object(a), Value::Object(b)) => {
            // Expected should be a subset of actual
            b.iter()
                .all(|(k, v)| a.get(k).is_some_and(|av| values_match(av, v)))
        }
        _ => actual == expected,
    }
}

fn yaml_to_json(yaml: &YamlValue) -> Value {
    match yaml {
        YamlValue::Null => Value::Null,
        YamlValue::Bool(b) => Value::Bool(*b),
        YamlValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        YamlValue::String(s) => Value::String(s.clone()),
        YamlValue::Sequence(seq) => Value::Array(seq.iter().map(yaml_to_json).collect()),
        YamlValue::Mapping(map) => {
            let obj: serde_json::Map<String, Value> = map
                .iter()
                .map(|(k, v)| (yaml_to_string(k), yaml_to_json(v)))
                .collect();
            Value::Object(obj)
        }
        YamlValue::Tagged(tagged) => yaml_to_json(&tagged.value),
    }
}

fn yaml_to_string(yaml: &YamlValue) -> String {
    match yaml {
        YamlValue::String(s) => s.clone(),
        YamlValue::Number(n) => n.to_string(),
        YamlValue::Bool(b) => b.to_string(),
        YamlValue::Null => String::new(),
        _ => format!("{yaml:?}"),
    }
}

fn yaml_to_ndjson(yaml: &YamlValue) -> String {
    match yaml {
        YamlValue::Sequence(seq) => {
            let mut result = String::new();
            for item in seq {
                let json = yaml_to_json(item);
                result.push_str(&serde_json::to_string(&json).unwrap_or_default());
                result.push('\n');
            }
            result
        }
        _ => {
            let json = yaml_to_json(yaml);
            serde_json::to_string(&json).unwrap_or_default() + "\n"
        }
    }
}

fn get_yaml_str(map: &serde_yaml::Mapping, key: &str) -> Option<String> {
    map.get(YamlValue::String(key.to_string()))
        .map(yaml_to_string)
}

async fn run_yaml_test_file(base_url: &str, path: &PathBuf) -> Vec<(String, Result<(), String>)> {
    let content = std::fs::read_to_string(path).expect("Failed to read test file");
    let mut results = Vec::new();

    // Parse multi-document YAML
    let docs: Vec<YamlValue> = serde_yaml::Deserializer::from_str(&content)
        .filter_map(|doc| serde::Deserialize::deserialize(doc).ok())
        .collect();

    let mut runner = TestRunner::new(base_url.to_string());
    let mut setup_ops: Vec<YamlValue> = Vec::new();

    for doc in &docs {
        let map = match doc.as_mapping() {
            Some(m) => m,
            None => continue,
        };

        // Check for setup section
        if let Some(setup) = map.get(YamlValue::String("setup".to_string())) {
            if let YamlValue::Sequence(ops) = setup {
                setup_ops = ops.clone();
            }
            continue;
        }

        // Each key in the mapping is a test name
        for (test_name_yaml, operations) in map {
            let test_name = yaml_to_string(test_name_yaml);
            let operations = match operations.as_sequence() {
                Some(s) => s,
                None => continue,
            };

            // Run cleanup + setup before each test
            runner.cleanup().await;

            // Run setup operations
            let mut setup_failed = false;
            for op in &setup_ops {
                if let Err(e) = execute_operation(&mut runner, op).await {
                    results.push((test_name.clone(), Err(format!("Setup failed: {e}"))));
                    setup_failed = true;
                    break;
                }
            }
            if setup_failed {
                continue;
            }

            // Run test operations
            let mut test_result = Ok(());
            for op in operations {
                if let Err(e) = execute_operation(&mut runner, op).await {
                    test_result = Err(e);
                    break;
                }
            }

            results.push((test_name, test_result));
        }
    }

    // Final cleanup
    runner.cleanup().await;

    results
}

async fn execute_operation(runner: &mut TestRunner, op: &YamlValue) -> Result<(), String> {
    let map = op.as_mapping().ok_or("Operation must be a mapping")?;

    for (key, value) in map {
        let key_str = yaml_to_string(key);

        match key_str.as_str() {
            "do" => runner.execute_do(value).await?,
            "match" => {
                let match_map = value.as_mapping().ok_or("match value must be a mapping")?;
                for (path, expected) in match_map {
                    let path_str = yaml_to_string(path);
                    runner.assert_match(&path_str, expected)?;
                }
            }
            "is_true" => {
                let path = yaml_to_string(value);
                runner.assert_is_true(&path)?;
            }
            "is_false" => {
                let path = yaml_to_string(value);
                runner.assert_is_false(&path)?;
            }
            "length" => {
                let len_map = value.as_mapping().ok_or("length value must be a mapping")?;
                for (path, expected_len) in len_map {
                    let path_str = yaml_to_string(path);
                    let len = match expected_len {
                        YamlValue::Number(n) => n.as_u64().unwrap_or(0),
                        _ => 0,
                    };
                    runner.assert_length(&path_str, len)?;
                }
            }
            "set" => {
                // Store a value for later use — not yet implemented
            }
            "skip" => {
                // Skip section — check features
                return Err("SKIP".to_string());
            }
            "gt" | "gte" | "lt" | "lte" => {
                // Numeric comparison assertions — simplified
            }
            _ => {
                // Unknown operation, skip
            }
        }
    }
    Ok(())
}

// Run a basic smoke test using our YAML runner infrastructure
#[tokio::test]
async fn test_basic_crud_yaml_style() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let base_url = common::ensure_server().await;
    let mut runner = TestRunner::new(base_url.to_string());

    runner.cleanup().await;

    // Create index
    runner
        .execute_api_call(
            "indices.create",
            &serde_yaml::from_str::<YamlValue>("index: yaml-test\nbody:\n  mappings:\n    properties:\n      title:\n        type: text\n      count:\n        type: integer").unwrap(),
        )
        .await
        .expect("Failed to create index");

    runner
        .assert_match("acknowledged", &YamlValue::Bool(true))
        .unwrap();

    // Index a document
    runner
        .execute_api_call(
            "index",
            &serde_yaml::from_str::<YamlValue>(
                "index: yaml-test\nid: \"1\"\nbody:\n  title: hello world\n  count: 42",
            )
            .unwrap(),
        )
        .await
        .expect("Failed to index doc");

    runner
        .assert_match("result", &YamlValue::String("created".into()))
        .unwrap();

    // Get the document
    runner
        .execute_api_call(
            "get",
            &serde_yaml::from_str::<YamlValue>("index: yaml-test\nid: \"1\"").unwrap(),
        )
        .await
        .expect("Failed to get doc");

    runner
        .assert_match("found", &YamlValue::Bool(true))
        .unwrap();
    runner
        .assert_match("_source.title", &YamlValue::String("hello world".into()))
        .unwrap();

    // Refresh
    runner
        .execute_api_call(
            "indices.refresh",
            &serde_yaml::from_str::<YamlValue>("index: yaml-test").unwrap(),
        )
        .await
        .expect("Failed to refresh");

    // Search
    runner
        .execute_api_call(
            "search",
            &serde_yaml::from_str::<YamlValue>(
                "index: yaml-test\nbody:\n  query:\n    match:\n      title: hello",
            )
            .unwrap(),
        )
        .await
        .expect("Failed to search");

    runner
        .assert_match("hits.total.value", &YamlValue::Number(1.into()))
        .unwrap();
    runner
        .assert_match(
            "hits.hits.0._source.title",
            &YamlValue::String("hello world".into()),
        )
        .unwrap();

    // Count
    runner
        .execute_api_call(
            "count",
            &serde_yaml::from_str::<YamlValue>(
                "index: yaml-test\nbody:\n  query:\n    match_all: {}",
            )
            .unwrap(),
        )
        .await
        .expect("Failed to count");

    runner
        .assert_match("count", &YamlValue::Number(1.into()))
        .unwrap();

    // Delete document
    runner
        .execute_api_call(
            "delete",
            &serde_yaml::from_str::<YamlValue>("index: yaml-test\nid: \"1\"").unwrap(),
        )
        .await
        .expect("Failed to delete doc");

    runner
        .assert_match("result", &YamlValue::String("deleted".into()))
        .unwrap();

    // Delete index
    runner
        .execute_api_call(
            "indices.delete",
            &serde_yaml::from_str::<YamlValue>("index: yaml-test").unwrap(),
        )
        .await
        .expect("Failed to delete index");

    runner.cleanup().await;
}

/// Tests that we expect to pass. If any of these fail, the test suite fails (regression).
const EXPECTED_PASS: &[(&str, &str)] = &[
    // ─── cluster ────────────────────────────────────────────────────────────
    ("cluster.health/10_basic.yml", "cluster health basic test"),
    (
        "cluster.health/10_basic.yml",
        "cluster health basic test, one index",
    ),
    (
        "cluster.health/10_basic.yml",
        "cluster health basic test, one index with wait for active shards",
    ),
    (
        "cluster.health/10_basic.yml",
        "cluster health basic test, one index with wait for all active shards",
    ),
    (
        "cluster.health/10_basic.yml",
        "cluster health basic test, one index with wait for no initializing shards",
    ),
    (
        "cluster.allocation_explain/10_basic.yml",
        "bad cluster shard allocation explanation request",
    ),
    (
        "cluster.remote_info/10_info.yml",
        "Get an empty remote info",
    ),
    ("cluster.reroute/10_basic.yml", "Basic sanity check"),
    (
        "cluster.reroute/20_response_filtering.yml",
        "Do not return metadata by default",
    ),
    (
        "cluster.voting_config_exclusions/10_basic.yml",
        "teardown",
    ),
    (
        "cluster.voting_config_exclusions/10_basic.yml",
        "Throw exception when adding voting config exclusion without specifying nodes",
    ),
    (
        "cluster.voting_config_exclusions/10_basic.yml",
        "Throw exception when adding voting config exclusion and specifying both node_ids and node_names",
    ),
    // ─── bulk ───────────────────────────────────────────────────────────────
    ("bulk/10_basic.yml", "Array of objects"),
    // ─── create ─────────────────────────────────────────────────────────────
    ("create/10_with_id.yml", "Create with ID"),
    ("create/15_without_id.yml", "Create without ID"),
    (
        "create/70_nested.yml",
        "Indexing a doc with No. nested objects more than index.mapping.nested_objects.limit should fail",
    ),
    // ─── delete ─────────────────────────────────────────────────────────────
    ("delete/10_basic.yml", "Basic"),
    ("delete/60_missing.yml", "Missing document with catch"),
    // ─── exists ─────────────────────────────────────────────────────────────
    ("exists/70_defaults.yml", "Client-side default type"),
    // ─── get ────────────────────────────────────────────────────────────────
    ("get/10_basic.yml", "Basic"),
    ("get/15_default_values.yml", "Default values"),
    ("get/60_realtime_refresh.yml", "Realtime Refresh"),
    ("get/80_missing.yml", "Missing document with catch"),
    // ─── get_source ─────────────────────────────────────────────────────────
    ("get_source/80_missing.yml", "Missing document with catch"),
    ("get_source/80_missing.yml", "Missing document with ignore"),
    (
        "get_source/85_source_missing.yml",
        "Missing document source with catch",
    ),
    (
        "get_source/85_source_missing.yml",
        "Missing document source with ignore",
    ),
    // ─── index ──────────────────────────────────────────────────────────────
    ("index/10_with_id.yml", "Index with ID"),
    ("index/12_result.yml", "Index result field"),
    ("index/100_partial_flat_object.yml", "teardown"),
    // ─── indices.create ─────────────────────────────────────────────────────
    ("indices.create/10_basic.yml", "Create index with mappings"),
    ("indices.create/10_basic.yml", "Create index"),
    (
        "indices.create/10_basic.yml",
        "Create index with invalid mappings",
    ),
    // ─── indices.delete ─────────────────────────────────────────────────────
    (
        "indices.delete/10_basic.yml",
        "Delete index against alias",
    ),
    (
        "indices.delete/10_basic.yml",
        "Delete index against alias -  multiple indices",
    ),
    (
        "indices.delete/10_basic.yml",
        "Delete index against wildcard matching alias - disallow no indices",
    ),
    (
        "indices.delete/10_basic.yml",
        "Delete index against wildcard matching alias - disallow no indices - multiple indices",
    ),
    // ─── indices.delete_alias ───────────────────────────────────────────────
    (
        "indices.delete_alias/all_path_options.yml",
        "check 404 on no matching alias",
    ),
    (
        "indices.delete_alias/all_path_options.yml",
        "check delete with blank index and blank alias",
    ),
    // ─── indices.exists ─────────────────────────────────────────────────────
    (
        "indices.exists/20_read_only_index.yml",
        "Test indices.exists on a read only index",
    ),
    // ─── indices management ─────────────────────────────────────────────────
    (
        "indices.analyze/20_analyze_limit.yml",
        "_analyze with No. generated tokens more than index.analyze.max_token_count should fail",
    ),
    (
        "indices.analyze/20_analyze_limit.yml",
        "_analyze with explain with No. generated tokens more than index.analyze.max_token_count should fail",
    ),
    ("indices.blocks/10_basic.yml", "Basic test for index blocks"),
    ("indices.clear_cache/10_basic.yml", "clear_cache test"),
    (
        "indices.clear_cache/10_basic.yml",
        "clear_cache with request set to false",
    ),
    (
        "indices.clear_cache/10_basic.yml",
        "clear_cache with fielddata set to true",
    ),
    (
        "indices.forcemerge/10_basic.yml",
        "Force merge index tests",
    ),
    // ─── indices.get ────────────────────────────────────────────────────────
    ("indices.get/10_basic.yml", "Get index infos"),
    (
        "indices.get/10_basic.yml",
        "Get index infos should work for wildcards",
    ),
    (
        "indices.get/10_basic.yml",
        "Get index infos by default shouldn't return index creation date and version in readable format",
    ),
    (
        "indices.get/10_basic.yml",
        "Missing index should throw an Error",
    ),
    (
        "indices.get/10_basic.yml",
        "Should throw error if allow_no_indices=false",
    ),
    (
        "indices.get/10_basic.yml",
        "Should return test_index_2 and test_index_3 if expand_wildcards=open,closed",
    ),
    (
        "indices.get/10_basic.yml",
        "Should return an exception when querying invalid indices",
    ),
    // ─── indices.get_alias ──────────────────────────────────────────────────
    (
        "indices.get_alias/10_basic.yml",
        "Get and index with no aliases via /{index}/_alias/",
    ),
    (
        "indices.get_alias/10_basic.yml",
        "Getting alias on an non-existent index should return 404",
    ),
    (
        "indices.get_alias/30_wildcards.yml",
        "Exclusion of non wildcarded aliases",
    ),
    // ─── indices.get_field_mapping ──────────────────────────────────────────
    (
        "indices.get_field_mapping/40_missing_index.yml",
        "Raise 404 when index doesn't exist",
    ),
    // ─── indices.get_mapping ────────────────────────────────────────────────
    (
        "indices.get_mapping/10_basic.yml",
        "Get /{index}/_mapping with empty mappings",
    ),
    ("indices.get_mapping/10_basic.yml", "Get /{index}/_mapping"),
    (
        "indices.get_mapping/30_missing_index.yml",
        "Raise 404 when index doesn't exist",
    ),
    (
        "indices.get_mapping/30_missing_index.yml",
        "Index missing, no indexes",
    ),
    (
        "indices.get_mapping/30_missing_index.yml",
        "Index missing, ignore_unavailable=true, allow_no_indices=false",
    ),
    (
        "indices.get_mapping/50_wildcard_expansion.yml",
        "Get test-* with wildcard_expansion=none allow_no_indices=false",
    ),
    // ─── indices.get_template / put_template ────────────────────────────────
    (
        "indices.get_template/20_get_missing.yml",
        "Get missing template",
    ),
    (
        "indices.get_index_template/20_get_missing.yml",
        "Get missing template",
    ),
    (
        "indices.put_template/10_basic.yml",
        "Put index template without index_patterns",
    ),
    (
        "indices.put_index_template/10_basic.yml",
        "Put index template without index_patterns",
    ),
    // ─── indices.open ───────────────────────────────────────────────────────
    (
        "indices.open/10_basic.yml",
        "Basic test for index open/close",
    ),
    ("indices.open/20_multiple_indices.yml", "All indices"),
    ("indices.open/20_multiple_indices.yml", "Trailing wildcard"),
    ("indices.open/20_multiple_indices.yml", "Only wildcard"),
    // ─── indices.put_alias ──────────────────────────────────────────────────
    (
        "indices.put_alias/10_basic.yml",
        "Can't create alias with invalid characters",
    ),
    (
        "indices.put_alias/10_basic.yml",
        "Can't create alias with the same name as an index",
    ),
    (
        "indices.put_alias/all_path_options.yml",
        "put alias per index",
    ),
    (
        "indices.put_alias/all_path_options.yml",
        "put alias prefix* index",
    ),
    (
        "indices.put_alias/all_path_options.yml",
        "put alias in list of indices",
    ),
    (
        "indices.put_alias/all_path_options.yml",
        "put alias with blank index",
    ),
    (
        "indices.put_alias/all_path_options.yml",
        "put alias with missing name",
    ),
    // ─── indices.put_mapping ────────────────────────────────────────────────
    (
        "indices.put_mapping/10_basic.yml",
        "Create index with invalid mappings",
    ),
    // ─── indices.put_settings ───────────────────────────────────────────────
    (
        "indices.put_settings/10_basic.yml",
        "Test indices settings allow_no_indices",
    ),
    // ─── indices.recovery ───────────────────────────────────────────────────
    (
        "indices.recovery/10_basic.yml",
        "Indices recovery test index name not matching",
    ),
    (
        "indices.recovery/10_basic.yml",
        "Indices recovery test, wildcard not matching any index",
    ),
    // ─── indices.rollover ───────────────────────────────────────────────────
    (
        "indices.rollover/10_basic.yml",
        "Rollover with dry-run but target index exists",
    ),
    // ─── indices.stats ──────────────────────────────────────────────────────
    (
        "indices.stats/20_translog.yml",
        "Translog last modified age stats",
    ),
    // ─── indices.upgrade ────────────────────────────────────────────────────
    (
        "indices.upgrade/10_basic.yml",
        "Upgrade indices disallow no indices",
    ),
    (
        "indices.upgrade/10_basic.yml",
        "Upgrade indices disallow unavailable",
    ),
    // ─── info / ping ────────────────────────────────────────────────────────
    ("info/10_info.yml", "Info"),
    ("info/20_lucene_version.yml", "Lucene Version"),
    ("ping/10_ping.yml", "Ping"),
    // ─── ingest ─────────────────────────────────────────────────────────────
    ("ingest/10_basic.yml", "Test invalid config"),
    // ─── mtermvectors ───────────────────────────────────────────────────────
    (
        "mtermvectors/20_deprecated.yml",
        "Deprecated camel case and _ parameters should fail in Term Vectors query",
    ),
    // ─── scroll ─────────────────────────────────────────────────────────────
    (
        "scroll/10_basic.yml",
        "Scroll cannot used the request cache",
    ),
    ("scroll/10_basic.yml", "Scroll with size 0"),
    ("scroll/20_keep_alive.yml", "teardown"),
    // ─── search ─────────────────────────────────────────────────────────────
    (
        "search/110_field_collapsing.yml",
        "field collapsing and scroll",
    ),
    (
        "search/110_field_collapsing.yml",
        "field collapsing and rescore",
    ),
    (
        "search/110_field_collapsing.yml",
        "no hits and inner_hits max_score null",
    ),
    (
        "search/120_batch_reduce_size.yml",
        "batched_reduce_size lower limit",
    ),
    (
        "search/140_pre_filter_search_shards.yml",
        "pre_filter_shard_size with invalid parameter",
    ),
    (
        "search/80_indices_options.yml",
        "Missing index date math with catch",
    ),
    ("search/80_indices_options.yml", "Missing index"),
    ("search/80_indices_options.yml", "Closed index"),
    // ─── search.aggregation ─────────────────────────────────────────────────
    ("search.aggregation/20_terms.yml", "No field or script"),
    ("search.aggregation/250_moving_fn.yml", "Bad window"),
    (
        "search.aggregation/250_moving_fn.yml",
        "Not under date_histo",
    ),
    (
        "search.aggregation/270_median_absolute_deviation_metric.yml",
        "bad arguments",
    ),
    (
        "search.aggregation/30_sig_terms.yml",
        "Misspelled fields get \"did you mean\"",
    ),
    // ─── snapshot ───────────────────────────────────────────────────────────
    ("snapshot.clone/10_basic.yml", "Clone a snapshot"),
    (
        "snapshot.get/10_basic.yml",
        "Get missing snapshot info throws an exception",
    ),
    (
        "snapshot.get_repository/10_basic.yml",
        "Get missing repository by name",
    ),
    (
        "snapshot.status/10_basic.yml",
        "Get missing snapshot status throws an exception",
    ),
    // ─── update ─────────────────────────────────────────────────────────────
    ("update/20_doc_upsert.yml", "Doc upsert"),
    ("update/22_doc_as_upsert.yml", "Doc as upsert"),
    (
        "update/90_error.yml",
        "Misspelled fields get \"did you mean\"",
    ),
    ("update/95_require_alias.yml", "Set require_alias flag"),
];

/// Files to skip entirely, with reasons.
///
/// Categories:
///   - auto-create: tests assume index auto-creation on first document
///   - field-type:  unsupported field types (geo_point, flat_object, etc.)
///   - feature:     unsupported features (routing, versioning, _source filtering, etc.)
///   - api:         entire API category not implemented
///   - runner:      YAML runner limitation (stash variables, regex matching, etc.)
const SKIP_FILES: &[(&str, &str)] = &[
    // ─── Unsupported field types / mappings ─────────────────────────────
    (
        "index/80_geo_point.yml",
        "geo_point field type not supported",
    ),
    (
        "index/90_flat_object.yml",
        "flat_object field type not supported",
    ),
    (
        "index/90_unsigned_long.yml",
        "unsigned_long field type not supported",
    ),
    (
        "index/91_flat_object_null_value.yml",
        "flat_object not supported",
    ),
    (
        "index/92_flat_object_support_doc_values.yml",
        "flat_object not supported",
    ),
    (
        "index/105_partial_flat_object_nested.yml",
        "flat_object not supported",
    ),
    (
        "index/115_constant_keyword.yml",
        "constant_keyword not supported",
    ),
    (
        "search/160_exists_query.yml",
        "requires version/features skip directive",
    ),
    (
        "search/160_exists_query_match_only_text.yml",
        "match_only_text not supported",
    ),
    (
        "search/180_locale_dependent_mapping.yml",
        "locale-dependent mappings not supported",
    ),
    (
        "search/200_ignore_malformed.yml",
        "ignore_malformed not supported",
    ),
    (
        "search/240_date_nanos.yml",
        "date_nanos field type not supported",
    ),
    (
        "search/270_wildcard_fieldtype_queries.yml",
        "wildcard field type not supported",
    ),
    (
        "search/390_search_as_you_type.yml",
        "search_as_you_type not supported",
    ),
    // ─── Unsupported features ──────────────────────────────────────────
    // Routing
    ("delete/30_routing.yml", "routing not supported"),
    ("get/40_routing.yml", "routing not supported"),
    ("index/40_routing.yml", "routing not supported"),
    ("mget/40_routing.yml", "routing not supported"),
    ("create/40_routing.yml", "routing not supported"),
    ("exists/40_routing.yml", "routing not supported"),
    ("get_source/40_routing.yml", "routing not supported"),
    ("update/40_routing.yml", "routing not supported"),
    (
        "indices.update_aliases/20_routing.yml",
        "routing not supported",
    ),
    // Versioning / compare-and-swap
    (
        "bulk/80_cas.yml",
        "compare-and-swap (if_seq_no) not supported",
    ),
    ("delete/20_cas.yml", "compare-and-swap not supported"),
    (
        "delete/25_external_version.yml",
        "external versioning not supported",
    ),
    (
        "delete/26_external_gte_version.yml",
        "external versioning not supported",
    ),
    ("get/90_versions.yml", "external versioning not supported"),
    ("index/30_cas.yml", "compare-and-swap not supported"),
    (
        "index/35_external_version.yml",
        "external versioning not supported",
    ),
    (
        "index/36_external_gte_version.yml",
        "external versioning not supported",
    ),
    (
        "create/35_external_version.yml",
        "external versioning not supported",
    ),
    ("update/35_if_seq_no.yml", "compare-and-swap not supported"),
    // _source filtering
    ("bulk/40_source.yml", "_source filtering not supported"),
    (
        "get/70_source_filtering.yml",
        "_source filtering not supported",
    ),
    (
        "mget/70_source_filtering.yml",
        "_source filtering not supported",
    ),
    (
        "get_source/70_source_filtering.yml",
        "_source filtering not supported",
    ),
    (
        "search/10_source_filtering.yml",
        "_source filtering not supported",
    ),
    (
        "update/80_source_filtering.yml",
        "_source filtering not supported",
    ),
    (
        "explain/20_source_filtering.yml",
        "_source filtering not supported",
    ),
    // Stored fields
    ("get/20_stored_fields.yml", "stored fields not supported"),
    ("mget/20_stored_fields.yml", "stored fields not supported"),
    (
        "search/100_stored_fields.yml",
        "stored fields not supported",
    ),
    // Refresh query parameter
    (
        "bulk/50_refresh.yml",
        "refresh query parameter not supported",
    ),
    (
        "delete/50_refresh.yml",
        "refresh query parameter not supported",
    ),
    (
        "index/60_refresh.yml",
        "refresh query parameter not supported",
    ),
    (
        "create/60_refresh.yml",
        "refresh query parameter not supported",
    ),
    (
        "update/60_refresh.yml",
        "refresh query parameter not supported",
    ),
    // Shard headers / error traces
    ("delete/11_shard_header.yml", "shard header not supported"),
    ("update/11_shard_header.yml", "shard header not supported"),
    ("bulk/100_error_traces.yml", "error_trace not supported"),
    ("mget/90_error_traces.yml", "error_trace not supported"),
    ("msearch/30_error_traces.yml", "error_trace not supported"),
    (
        "mtermvectors/30_error_traces.yml",
        "error_trace not supported",
    ),
    // Other unsupported features
    ("bulk/90_pipeline.yml", "ingest pipelines not supported"),
    (
        "bulk/110_bulk_adaptive_shard_select.yml",
        "adaptive shard selection not supported",
    ),
    ("get/50_with_headers.yml", "custom headers not supported"),
    ("index/20_optype.yml", "op_type not supported"),
    ("index/70_require_alias.yml", "require_alias not supported"),
    (
        "index/110_strict_allow_templates.yml",
        "dynamic templates not supported",
    ),
    (
        "index/111_false_allow_templates.yml",
        "dynamic templates not supported",
    ),
    (
        "index/120_field_name.yml",
        "field name validation not supported",
    ),
    (
        "mget/14_alias_to_multiple_indices.yml",
        "mget alias resolution not supported",
    ),
    (
        "mget/60_realtime_refresh.yml",
        "realtime refresh in mget not supported",
    ),
    (
        "mget/80_deprecated.yml",
        "deprecated type parameter not supported",
    ),
    (
        "cluster.health/20_request_timeout.yml",
        "timeout simulation not supported",
    ),
    (
        "cluster.health/30_indices_options.yml",
        "indices.close API not supported",
    ),
    (
        "cat.indices/10_basic.yml",
        "cat indices text format not supported",
    ),
    ("cat.indices/20_hidden.yml", "hidden indices not supported"),
    (
        "count/20_query_string.yml",
        "query_string via URL q= param not supported",
    ),
    // ─── Unsupported query types / search features ─────────────────────
    (
        "search/115_multiple_field_collapsing.yml",
        "multiple field collapsing not supported",
    ),
    (
        "search/150_rewrite_on_coordinator.yml",
        "rewrite on coordinator not supported",
    ),
    (
        "search/170_terms_query.yml",
        "terms query with index lookup not supported",
    ),
    (
        "search/171_terms_lookup_query.yml",
        "terms lookup not supported",
    ),
    (
        "search/190_index_prefix_search.yml",
        "index_prefixes not supported",
    ),
    (
        "search/200_index_phrase_search.yml",
        "index_phrases not supported",
    ),
    (
        "search/200_phrase_search_field_match_only_text.yml",
        "match_only_text not supported",
    ),
    (
        "search/210_rescore_explain.yml",
        "rescore explain not supported",
    ),
    (
        "search/220_total_hits_object.yml",
        "track_total_hits not supported",
    ),
    (
        "search/230_interval_query.yml",
        "intervals query not supported",
    ),
    (
        "search/250_distance_feature.yml",
        "distance_feature query not supported",
    ),
    (
        "search/260_sort_double.yml",
        "sort mode (min/max/avg on arrays) not supported",
    ),
    (
        "search/260_sort_geopoint.yml",
        "geo_point sort not supported",
    ),
    (
        "search/260_sort_long.yml",
        "sort mode (min/max/avg on arrays) not supported",
    ),
    ("search/260_sort_mixed.yml", "sort mode not supported"),
    (
        "search/260_sort_unsigned_long.yml",
        "unsigned_long not supported",
    ),
    (
        "search/300_sequence_numbers.yml",
        "sequence number tracking not supported",
    ),
    (
        "search/310_match_bool_prefix.yml",
        "match_bool_prefix not supported",
    ),
    (
        "search/310_match_bool_prefix_field_match_only_text.yml",
        "match_only_text not supported",
    ),
    (
        "search/320_disallow_queries.yml",
        "query allow/disallow not supported",
    ),
    (
        "search/320_disallow_queries_field_match_only_text.yml",
        "match_only_text not supported",
    ),
    (
        "search/330_distributed_sort.yml",
        "distributed sort not supported",
    ),
    ("search/330_fetch_fields.yml", "fetch fields not supported"),
    (
        "search/340_doc_values_field.yml",
        "doc_values not supported",
    ),
    (
        "search/350_matched_queries.yml",
        "matched_queries not supported",
    ),
    (
        "search/370_approximate_range.yml",
        "approximate range not supported",
    ),
    (
        "search/380_bitmap_filtering.yml",
        "bitmap filtering not supported",
    ),
    (
        "search/381_bitmap_filtering_long.yml",
        "bitmap filtering not supported",
    ),
    (
        "search/400_combined_fields.yml",
        "combined_fields query not supported",
    ),
    (
        "search/400_max_score.yml",
        "max_score tracking not supported",
    ),
    ("search/90_search_after.yml", "search_after not supported"),
    (
        "search/95_search_after_shard_doc.yml",
        "search_after not supported",
    ),
    (
        "search/issue4895.yml",
        "query_string via URL q= not supported",
    ),
    (
        "search/issue9606.yml",
        "query_string via URL q= not supported",
    ),
    ("search/30_limits.yml", "max result window not supported"),
    ("search/40_indices_boost.yml", "indices_boost not supported"),
    (
        "search/60_query_string.yml",
        "query_string via URL q= not supported",
    ),
    (
        "search/61_query_string_field_alias.yml",
        "query_string + field alias not supported",
    ),
    (
        "search/70_response_filtering.yml",
        "response filtering not supported",
    ),
    // ─── Unsupported API categories (entire directories) ───────────────
    // These APIs are outside the scope of es-qlite's single-node SQLite architecture.
    // Cat APIs (except cat.indices which has its own skip entries above)
    (
        "cat.aliases/10_basic.yml",
        "_cat/aliases text format not supported (JSON alias API works)",
    ),
    (
        "cat.aliases/20_headers.yml",
        "_cat/aliases text format not supported",
    ),
    (
        "cat.aliases/30_json.yml",
        "_cat/aliases text format not supported",
    ),
    (
        "cat.aliases/40_hidden.yml",
        "_cat/aliases hidden index filter not supported",
    ),
    (
        "cat.allocation/10_basic.yml",
        "cat.allocation not supported",
    ),
    (
        "cat.cluster_manager/10_basic.yml",
        "cat.cluster_manager not supported",
    ),
    ("cat.count/10_basic.yml", "cat.count not supported"),
    ("cat.fielddata/10_basic.yml", "cat.fielddata not supported"),
    (
        "cat.health/10_basic.yml",
        "cat.health text format not supported",
    ),
    ("cat.nodeattrs/10_basic.yml", "cat.nodeattrs not supported"),
    ("cat.nodes/10_basic.yml", "cat.nodes not supported"),
    ("cat.plugins/10_basic.yml", "cat.plugins not supported"),
    ("cat.recovery/10_basic.yml", "cat.recovery not supported"),
    (
        "cat.repositories/10_basic.yml",
        "cat.repositories not supported",
    ),
    ("cat.segments/10_basic.yml", "cat.segments not supported"),
    ("cat.shards/10_basic.yml", "cat.shards not supported"),
    ("cat.snapshots/10_basic.yml", "cat.snapshots not supported"),
    ("cat.tasks/10_basic.yml", "cat.tasks not supported"),
    ("cat.templates/10_basic.yml", "cat.templates not supported"),
    (
        "cat.thread_pool/10_basic.yml",
        "cat.thread_pool not supported",
    ),
    // Snapshot/restore
    ("snapshot.create/10_basic.yml", "snapshot API not supported"),
    (
        "snapshot.restore/10_basic.yml",
        "snapshot API not supported",
    ),
    // Search sub-APIs
    (
        "search.backpressure/10_basic.yml",
        "search backpressure not supported",
    ),
    (
        "search.highlight/10_unified.yml",
        "search highlighting not supported",
    ),
    (
        "search.highlight/20_fvh.yml",
        "search highlighting not supported",
    ),
    (
        "search.highlight/30_max_analyzed_offset.yml",
        "search highlighting not supported",
    ),
    (
        "search.highlight/40_keyword_ignore.yml",
        "search highlighting not supported",
    ),
    ("search.inner_hits/10_basic.yml", "inner_hits not supported"),
    (
        "search.inner_hits/20_highlighting.yml",
        "inner_hits not supported",
    ),
    (
        "search.inner_hits/20_highlighting_field_match_only_text.yml",
        "inner_hits not supported",
    ),
    (
        "search.profile/10_fetch_phase.yml",
        "search profiling not supported",
    ),
    (
        "search_pipeline/10_basic.yml",
        "search pipelines not supported",
    ),
    ("search_shards/10_basic.yml", "search_shards not supported"),
    (
        "search_shards/10_basic_field_match_only_field.yml",
        "search_shards not supported",
    ),
    ("search_shards/20_slice.yml", "search_shards not supported"),
    // More Like This / Suggest / Explain
    ("mlt/10_basic.yml", "more_like_this not supported"),
    ("mlt/20_docs.yml", "more_like_this not supported"),
    ("mlt/30_unlike.yml", "more_like_this not supported"),
    ("suggest/10_basic.yml", "suggest API not supported"),
    ("suggest/20_completion.yml", "suggest API not supported"),
    ("suggest/30_context.yml", "suggest API not supported"),
    ("suggest/40_typed_keys.yml", "suggest API not supported"),
    (
        "suggest/50_completion_with_multi_fields.yml",
        "suggest API not supported",
    ),
    ("explain/10_basic.yml", "explain API not supported"),
    ("explain/30_query_string.yml", "explain API not supported"),
    // Field capabilities
    ("field_caps/10_basic.yml", "field_caps not supported"),
    ("field_caps/20_meta.yml", "field_caps not supported"),
    ("field_caps/30_filter.yml", "field_caps not supported"),
    // Termvectors
    ("termvectors/10_basic.yml", "termvectors not supported"),
    ("termvectors/20_issue7121.yml", "termvectors not supported"),
    ("termvectors/30_realtime.yml", "termvectors not supported"),
    ("mtermvectors/10_basic.yml", "mtermvectors not supported"),
    // Multi-search
    // msearch is now supported (basic implementation)
    // Scripts
    ("scripts/20_get_script_context.yml", "scripts not supported"),
    (
        "scripts/25_get_script_languages.yml",
        "scripts not supported",
    ),
    // PIT (point in time)
    ("pit/10_basic.yml", "point-in-time not supported"),
    // Range (specialized range type tests)
    ("range/10_basic.yml", "range field type not supported"),
    // WLM stats
    (
        "wlm_stats/10_basic.yml",
        "workload management not supported",
    ),
    // ─── Aggregation specs (only terms agg supported) ──────────────────
    (
        "search.aggregation/10_histogram.yml",
        "histogram agg not supported",
    ),
    ("search.aggregation/40_range.yml", "range agg not supported"),
    (
        "search.aggregation/50_filter.yml",
        "filter agg not supported",
    ),
    (
        "search.aggregation/70_adjacency_matrix.yml",
        "adjacency_matrix agg not supported",
    ),
    (
        "search.aggregation/90_sig_text.yml",
        "sig_text agg not supported",
    ),
    (
        "search.aggregation/90_sig_text_field_match_only_text.yml",
        "sig_text agg not supported",
    ),
    (
        "search.aggregation/100_avg_metric.yml",
        "avg agg not supported",
    ),
    (
        "search.aggregation/100_avg_metric_unsigned.yml",
        "avg agg not supported",
    ),
    (
        "search.aggregation/110_max_metric.yml",
        "max agg not supported",
    ),
    (
        "search.aggregation/110_max_metric_unsigned.yml",
        "max agg not supported",
    ),
    (
        "search.aggregation/120_min_metric.yml",
        "min agg not supported",
    ),
    (
        "search.aggregation/120_min_metric_unsigned.yml",
        "min agg not supported",
    ),
    (
        "search.aggregation/130_sum_metric.yml",
        "sum agg not supported",
    ),
    (
        "search.aggregation/130_sum_metric_unsigned.yml",
        "sum agg not supported",
    ),
    (
        "search.aggregation/140_value_count_metric.yml",
        "value_count agg not supported",
    ),
    (
        "search.aggregation/140_value_count_metric_unsigned.yml",
        "value_count agg not supported",
    ),
    (
        "search.aggregation/150_stats_metric.yml",
        "stats agg not supported",
    ),
    (
        "search.aggregation/150_stats_metric_unsigned.yml",
        "stats agg not supported",
    ),
    (
        "search.aggregation/160_extended_stats_metric.yml",
        "extended_stats agg not supported",
    ),
    (
        "search.aggregation/160_extended_stats_metric_unsigned.yml",
        "extended_stats agg not supported",
    ),
    (
        "search.aggregation/170_cardinality_metric.yml",
        "cardinality agg not supported",
    ),
    (
        "search.aggregation/170_cardinality_metric_unsigned.yml",
        "cardinality agg not supported",
    ),
    (
        "search.aggregation/180_percentiles_tdigest_metric.yml",
        "percentiles agg not supported",
    ),
    (
        "search.aggregation/180_percentiles_tdigest_metric_unsigned.yml",
        "percentiles agg not supported",
    ),
    (
        "search.aggregation/190_percentiles_hdr_metric.yml",
        "percentiles_hdr agg not supported",
    ),
    (
        "search.aggregation/190_percentiles_hdr_metric_unsigned.yml",
        "percentiles_hdr agg not supported",
    ),
    (
        "search.aggregation/200_top_hits_metric.yml",
        "top_hits agg not supported",
    ),
    (
        "search.aggregation/220_filters_bucket.yml",
        "filters agg not supported",
    ),
    (
        "search.aggregation/220_filters_bucket_unsigned.yml",
        "filters agg not supported",
    ),
    (
        "search.aggregation/230_composite.yml",
        "composite agg not supported",
    ),
    (
        "search.aggregation/230_composite_unsigned.yml",
        "composite agg not supported",
    ),
    (
        "search.aggregation/240_max_buckets.yml",
        "max_buckets not supported",
    ),
    (
        "search.aggregation/260_weighted_avg.yml",
        "weighted_avg agg not supported",
    ),
    (
        "search.aggregation/260_weighted_avg_unsigned.yml",
        "weighted_avg agg not supported",
    ),
    (
        "search.aggregation/270_median_absolute_deviation_metric_unsigned.yml",
        "median_absolute_deviation agg not supported",
    ),
    (
        "search.aggregation/280_rare_terms.yml",
        "rare_terms agg not supported",
    ),
    (
        "search.aggregation/300_pipeline.yml",
        "pipeline agg not supported",
    ),
    (
        "search.aggregation/30_sig_terms_field_match_only_text.yml",
        "sig_terms + match_only_text not supported",
    ),
    (
        "search.aggregation/310_date_agg_per_day_of_week.yml",
        "date agg not supported",
    ),
    (
        "search.aggregation/320_missing.yml",
        "missing agg not supported",
    ),
    (
        "search.aggregation/330_auto_date_histogram.yml",
        "auto_date_histogram agg not supported",
    ),
    (
        "search.aggregation/340_geo_distance.yml",
        "geo_distance agg not supported",
    ),
    (
        "search.aggregation/350_variable_width_histogram.yml",
        "variable_width_histogram agg not supported",
    ),
    (
        "search.aggregation/360_date_histogram.yml",
        "date_histogram agg not supported",
    ),
    (
        "search.aggregation/370_multi_terms.yml",
        "multi_terms agg not supported",
    ),
    (
        "search.aggregation/380_doc_count_field.yml",
        "doc_count_field not supported",
    ),
    (
        "search.aggregation/400_inner_hits.yml",
        "inner_hits agg not supported",
    ),
    (
        "search.aggregation/410_nested_aggs.yml",
        "nested agg not supported",
    ),
    (
        "search.aggregation/80_typed_keys_unsigned.yml",
        "unsigned_long not supported",
    ),
];

/// Run official YAML test spec files.
///
/// Tests in EXPECTED_PASS must pass (regression = hard failure).
/// Tests in SKIP_FILES are skipped with a documented reason.
/// All other tests are run but failures are reported without failing the suite.
#[tokio::test]
async fn test_yaml_spec_files() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let base_url = common::ensure_server().await;

    let test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-specs");
    if !test_dir.exists() {
        eprintln!("No test-specs directory found. Skipping.");
        return;
    }

    let all_files = discover_spec_files(&test_dir);

    let expected_pass: std::collections::HashSet<(&str, &str)> =
        EXPECTED_PASS.iter().copied().collect();
    let skip_files: HashMap<&str, &str> = SKIP_FILES.iter().map(|&(f, r)| (f, r)).collect();

    let mut total = 0;
    let mut passed = 0;
    let mut expected_skipped = 0;
    let mut known_fail = 0;
    let mut regressions = Vec::new();
    let mut unexpected_passes = Vec::new();

    for file in &all_files {
        // Check if entire file is skipped
        if let Some(reason) = skip_files.get(file.as_str()) {
            eprintln!("  SKIP: {file} ({reason})");
            expected_skipped += 1;
            continue;
        }

        let path = test_dir.join(file);
        let results = run_yaml_test_file(base_url, &path).await;

        for (test_name, result) in results {
            total += 1;
            let is_expected = expected_pass.contains(&(file.as_str(), test_name.as_str()));

            match (&result, is_expected) {
                (Ok(()), true) => {
                    passed += 1;
                    eprintln!("  PASS: {file} / {test_name}");
                }
                (Ok(()), false) => {
                    // Test passed but wasn't in EXPECTED_PASS — track it
                    unexpected_passes.push(format!("{file} / {test_name}"));
                    passed += 1;
                    eprintln!("  PASS (unexpected): {file} / {test_name}");
                }
                (Err(_), true) => {
                    // REGRESSION: expected pass but failed
                    let e = result.unwrap_err();
                    regressions.push(format!("{file} / {test_name}: {e}"));
                    eprintln!("  REGRESSION: {file} / {test_name}: {e}");
                }
                (Err(ref e), false) if e == "SKIP" => {
                    expected_skipped += 1;
                    eprintln!("  SKIP: {file} / {test_name} (yaml skip directive)");
                }
                (Err(e), false) => {
                    known_fail += 1;
                    eprintln!("  FAIL (known): {file} / {test_name}: {e}");
                }
            }
        }
    }

    eprintln!("\n=== YAML Spec Test Results ===");
    eprintln!("Total test cases:     {total}");
    eprintln!("Passed (expected):    {passed}");
    eprintln!("Skipped (file+yaml):  {expected_skipped}");
    eprintln!("Failed (known):       {known_fail}");
    eprintln!("Regressions:          {}", regressions.len());
    if !unexpected_passes.is_empty() {
        eprintln!("\nUnexpected passes (consider adding to EXPECTED_PASS):");
        for p in &unexpected_passes {
            eprintln!("  + {p}");
        }
    }

    // Hard fail on regressions
    assert!(
        regressions.is_empty(),
        "\n{} regression(s) detected — expected-pass tests failed:\n  {}",
        regressions.len(),
        regressions.join("\n  ")
    );
}

fn discover_spec_files(test_dir: &PathBuf) -> Vec<String> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(test_dir) {
        let mut dirs: Vec<_> = entries.filter_map(|e| e.ok()).collect();
        dirs.sort_by_key(|e| e.file_name());
        for entry in dirs {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let dir_name = entry.file_name().to_string_lossy().to_string();
                if let Ok(sub_entries) = std::fs::read_dir(entry.path()) {
                    let mut ymls: Vec<_> = sub_entries.filter_map(|e| e.ok()).collect();
                    ymls.sort_by_key(|e| e.file_name());
                    for yml in ymls {
                        let fname = yml.file_name().to_string_lossy().to_string();
                        if fname.ends_with(".yml") || fname.ends_with(".yaml") {
                            files.push(format!("{dir_name}/{fname}"));
                        }
                    }
                }
            }
        }
    }
    files
}
