use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::sync::Arc;

use crate::error::EsError;
use crate::model::mapping::{CreateIndexRequest, IndexMapping};
use crate::storage::registry::IndexRegistry;

pub async fn create_index(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    match create_index_inner(registry, index, body).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn create_index_inner(
    registry: Arc<IndexRegistry>,
    index: String,
    body: Bytes,
) -> Result<Response, EsError> {
    let req: CreateIndexRequest = if body.is_empty() {
        CreateIndexRequest::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => {
                let body_str = String::from_utf8_lossy(&body);
                tracing::error!("Failed to parse create index body for '{}': {} -- body: {}", index, e, &body_str[..body_str.len().min(500)]);
                return Err(EsError::ParsingError(e.to_string()));
            }
        }
    };
    let mapping = req.mappings.unwrap_or_default();
    let settings = req.settings.unwrap_or_default();

    registry.create(index.clone(), mapping, settings).await?;

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "acknowledged": true,
            "shards_acknowledged": true,
            "index": index
        })),
    )
        .into_response())
}

pub async fn delete_index(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
) -> Response {
    match registry.delete(&index).await {
        Ok(()) => Json(serde_json::json!({"acknowledged": true})).into_response(),
        Err(e) => e.into_response(),
    }
}

pub async fn index_exists(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
) -> StatusCode {
    if IndexRegistry::is_pattern(&index) {
        if registry.resolve_pattern(&index).is_empty() {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::OK
        }
    } else if registry.exists(&index) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn get_mapping(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
) -> Response {
    match registry.get(&index) {
        Ok(handle) => {
            let mapping = handle.mapping.read().clone();
            Json(serde_json::json!({
                index: {
                    "mappings": mapping
                }
            }))
            .into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn get_index(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
) -> Response {
    if IndexRegistry::is_pattern(&index) {
        let handles = registry.resolve_pattern(&index);
        if handles.is_empty() {
            return EsError::IndexNotFound(index).into_response();
        }
        let mut result = serde_json::Map::new();
        for handle in &handles {
            let mapping = handle.mapping.read().clone();
            result.insert(handle.name.clone(), serde_json::json!({
                "aliases": {},
                "mappings": mapping,
                "settings": {
                    "index": {
                        "number_of_shards": "1",
                        "number_of_replicas": "0",
                        "provided_name": &handle.name,
                        "creation_date": "0",
                        "uuid": &handle.name,
                        "version": { "created": "136327827" }
                    }
                }
            }));
        }
        return Json(serde_json::Value::Object(result)).into_response();
    }

    match registry.get(&index) {
        Ok(handle) => {
            let mapping = handle.mapping.read().clone();
            let mut result = serde_json::Map::new();
            result.insert(index.clone(), serde_json::json!({
                "aliases": {},
                "mappings": mapping,
                "settings": {
                    "index": {
                        "number_of_shards": "1",
                        "number_of_replicas": "0",
                        "provided_name": &index,
                        "creation_date": "0",
                        "uuid": &index,
                        "version": { "created": "136327827" }
                    }
                }
            }));
            Json(serde_json::Value::Object(result)).into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn get_settings(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
) -> Response {
    match registry.get(&index) {
        Ok(_handle) => {
            let mut result = serde_json::Map::new();
            result.insert(index.clone(), serde_json::json!({
                "settings": {
                    "index": {
                        "number_of_shards": "1",
                        "number_of_replicas": "0",
                        "provided_name": &index,
                        "creation_date": "0",
                        "uuid": &index,
                        "version": { "created": "136327827" }
                    }
                }
            }));
            Json(serde_json::Value::Object(result)).into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn put_settings(
    State(_registry): State<Arc<IndexRegistry>>,
    Path(_index): Path<String>,
    _body: Bytes,
) -> Response {
    Json(serde_json::json!({"acknowledged": true})).into_response()
}

pub async fn get_aliases(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
) -> Response {
    let handles = registry.resolve_name(&index);
    if handles.is_empty() {
        // Return empty object for not-found indices (not an error for alias queries)
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

pub async fn put_mapping(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
    Json(new_mapping): Json<IndexMapping>,
) -> Response {
    match registry.get(&index) {
        Ok(handle) => {
            let mut mapping = handle.mapping.write();
            for (name, field) in new_mapping.properties {
                mapping.properties.entry(name).or_insert(field);
            }
            Json(serde_json::json!({"acknowledged": true})).into_response()
        }
        Err(e) => e.into_response(),
    }
}
