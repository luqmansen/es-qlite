use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::EsError;
use crate::storage::registry::IndexRegistry;

pub async fn root_info() -> Response {
    Json(serde_json::json!({
        "name": "es-sqlite",
        "cluster_name": "opensearch-sqlite",
        "cluster_uuid": "es-sqlite-local",
        "version": {
            "distribution": "opensearch",
            "number": "2.17.1",
            "build_type": "tar",
            "build_hash": "unknown",
            "build_date": "2024-01-01T00:00:00.000000Z",
            "build_snapshot": false,
            "lucene_version": "9.11.1",
            "minimum_wire_compatibility_version": "7.10.0",
            "minimum_index_compatibility_version": "7.0.0"
        },
        "tagline": "The OpenSearch Project: https://opensearch.org/"
    }))
    .into_response()
}

pub async fn cluster_health(State(registry): State<Arc<IndexRegistry>>) -> Response {
    let indices = registry.list();
    Json(serde_json::json!({
        "cluster_name": "opensearch-sqlite",
        "status": "green",
        "timed_out": false,
        "number_of_nodes": 1,
        "number_of_data_nodes": 1,
        "active_primary_shards": indices.len(),
        "active_shards": indices.len(),
        "relocating_shards": 0,
        "initializing_shards": 0,
        "unassigned_shards": 0,
        "delayed_unassigned_shards": 0,
        "number_of_pending_tasks": 0,
        "number_of_in_flight_fetch": 0,
        "task_max_waiting_in_queue_millis": 0,
        "active_shards_percent_as_number": 100.0,
        "discovered_master": true,
        "discovered_cluster_manager": true
    }))
    .into_response()
}

pub async fn cat_indices(State(registry): State<Arc<IndexRegistry>>) -> Response {
    let indices = registry.list();
    let mut rows = Vec::new();

    for handle in &indices {
        let doc_count = handle.doc_count.load(Ordering::Relaxed);
        rows.push(serde_json::json!({
            "health": "green",
            "status": "open",
            "index": handle.name,
            "uuid": handle.name,
            "pri": "1",
            "rep": "0",
            "docs.count": doc_count.to_string(),
            "docs.deleted": "0",
            "store.size": "0b",
            "pri.store.size": "0b",
        }));
    }

    Json(serde_json::Value::Array(rows)).into_response()
}

pub async fn refresh(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
) -> Response {
    match refresh_inner(registry, index).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn refresh_inner(registry: Arc<IndexRegistry>, index: String) -> Result<Response, EsError> {
    let handle = registry.get(&index)?;
    handle
        .conn
        .call(|conn| {
            conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
            Ok(())
        })
        .await?;

    Ok(Json(serde_json::json!({
        "_shards": {
            "total": 1,
            "successful": 1,
            "failed": 0
        }
    }))
    .into_response())
}

/// Stub handler that returns {"acknowledged": true} for any method.
pub async fn stub_ok(_body: Bytes) -> Response {
    Json(serde_json::json!({"acknowledged": true})).into_response()
}

pub async fn cluster_settings() -> Response {
    Json(serde_json::json!({
        "persistent": {},
        "transient": {},
        "defaults": {
            "compatibility": {
                "override_main_response_version": "true"
            }
        }
    }))
    .into_response()
}

pub async fn put_cluster_settings(_body: Bytes) -> Response {
    Json(serde_json::json!({
        "acknowledged": true,
        "persistent": {},
        "transient": {}
    }))
    .into_response()
}

pub async fn get_all_aliases(State(registry): State<Arc<IndexRegistry>>) -> Response {
    let all_aliases = registry.get_all_aliases();
    let indices = registry.list();
    let mut result = serde_json::Map::new();

    for handle in &indices {
        let index_aliases = registry.get_aliases_for_index(&handle.name);
        let mut alias_map = serde_json::Map::new();
        for alias in &index_aliases {
            alias_map.insert(alias.clone(), serde_json::json!({}));
        }
        result.insert(handle.name.clone(), serde_json::json!({ "aliases": alias_map }));
    }

    let _ = all_aliases; // used above indirectly
    Json(serde_json::Value::Object(result)).into_response()
}

pub async fn post_aliases(
    State(registry): State<Arc<IndexRegistry>>,
    body: Bytes,
) -> Response {
    // Parse {"actions": [{"add": {"index": "x", "alias": "y"}}, {"remove": ...}]}
    let body_str = String::from_utf8_lossy(&body);
    tracing::debug!("POST /_aliases body: {}", &body_str[..body_str.len().min(500)]);
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) {
        if let Some(actions) = value.get("actions").and_then(|a| a.as_array()) {
            for action in actions {
                if let Some(add) = action.get("add") {
                    let index = add.get("index").and_then(|v| v.as_str()).unwrap_or("");
                    let aliases = if let Some(alias) = add.get("alias").and_then(|v| v.as_str()) {
                        vec![alias.to_string()]
                    } else if let Some(arr) = add.get("aliases").and_then(|v| v.as_array()) {
                        arr.iter().filter_map(|v| v.as_str().map(String::from)).collect()
                    } else {
                        vec![]
                    };
                    for alias in &aliases {
                        if !index.is_empty() && !alias.is_empty() {
                            tracing::debug!("Adding alias '{}' -> index '{}'", alias, index);
                            registry.add_alias(index, alias);
                        }
                    }
                }
                if let Some(remove) = action.get("remove") {
                    let index = remove.get("index").and_then(|v| v.as_str()).unwrap_or("");
                    let alias = remove.get("alias").and_then(|v| v.as_str()).unwrap_or("");
                    if !index.is_empty() && !alias.is_empty() {
                        registry.remove_alias(index, alias);
                    }
                }
            }
        }
    }
    Json(serde_json::json!({"acknowledged": true})).into_response()
}

pub async fn get_alias(
    State(registry): State<Arc<IndexRegistry>>,
    Path(name): Path<String>,
) -> Response {
    let handles = registry.resolve_name(&name);
    if handles.is_empty() {
        return Json(serde_json::json!({})).into_response();
    }
    let mut result = serde_json::Map::new();
    for handle in &handles {
        let index_aliases = registry.get_aliases_for_index(&handle.name);
        let mut alias_map = serde_json::Map::new();
        for alias in &index_aliases {
            alias_map.insert(alias.clone(), serde_json::json!({}));
        }
        result.insert(handle.name.clone(), serde_json::json!({ "aliases": alias_map }));
    }
    Json(serde_json::Value::Object(result)).into_response()
}

pub async fn put_alias(
    State(registry): State<Arc<IndexRegistry>>,
    Path((index, alias)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    // Also check body for aliases array
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) {
        if let Some(aliases) = value.get("aliases").and_then(|a| a.as_array()) {
            for alias_val in aliases {
                if let Some(a) = alias_val.as_str() {
                    registry.add_alias(&index, a);
                }
            }
            return Json(serde_json::json!({"acknowledged": true})).into_response();
        }
    }
    tracing::debug!("Adding alias '{}' -> index '{}'", alias, index);
    registry.add_alias(&index, &alias);
    Json(serde_json::json!({"acknowledged": true})).into_response()
}

pub async fn delete_alias(
    State(registry): State<Arc<IndexRegistry>>,
    Path((index, alias)): Path<(String, String)>,
) -> Response {
    registry.remove_alias(&index, &alias);
    Json(serde_json::json!({"acknowledged": true})).into_response()
}
