use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::sync::Arc;

use crate::error::EsError;
use crate::model::document::{DeleteDocResponse, GetDocResponse, IndexDocResponse, ShardsInfo};
use crate::storage::{reader, registry::IndexRegistry, writer};

pub async fn index_doc(
    State(registry): State<Arc<IndexRegistry>>,
    Path((index, id)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    match index_doc_inner(registry, index, id, body).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn index_doc_inner(
    registry: Arc<IndexRegistry>,
    index: String,
    id: String,
    body: serde_json::Value,
) -> Result<Response, EsError> {
    let handle = registry.get_or_create(&index).await?;
    let result = writer::index_document(&handle, id, body).await?;

    let status = if result.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };

    Ok((
        status,
        Json(IndexDocResponse {
            _index: index,
            _id: result.id,
            _version: result.version,
            result: if result.created {
                "created".to_string()
            } else {
                "updated".to_string()
            },
            _shards: ShardsInfo::ok(),
            _seq_no: result.seq_no,
            _primary_term: 1,
        }),
    )
        .into_response())
}

pub async fn index_doc_autoid(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let id = uuid::Uuid::new_v4().to_string();
    match index_doc_inner(registry, index, id, body).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

pub async fn get_doc(
    State(registry): State<Arc<IndexRegistry>>,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    match get_doc_inner(registry, index, id).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn get_doc_inner(
    registry: Arc<IndexRegistry>,
    index: String,
    id: String,
) -> Result<Response, EsError> {
    let handle = registry.get(&index)?;
    let record = reader::get_document(&handle, &id).await?;

    match record {
        Some(doc) => {
            let source: serde_json::Value = serde_json::from_str(&doc.source)?;
            Ok(Json(GetDocResponse {
                _index: index,
                _id: doc.id,
                _version: doc.version,
                _seq_no: doc.seq_no,
                _primary_term: 1,
                found: true,
                _source: Some(source),
            })
            .into_response())
        }
        None => Ok(Json(GetDocResponse {
            _index: index,
            _id: id,
            _version: 0,
            _seq_no: 0,
            _primary_term: 1,
            found: false,
            _source: None,
        })
        .into_response()),
    }
}

pub async fn delete_doc(
    State(registry): State<Arc<IndexRegistry>>,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    match delete_doc_inner(registry, index, id).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn delete_doc_inner(
    registry: Arc<IndexRegistry>,
    index: String,
    id: String,
) -> Result<Response, EsError> {
    let handle = registry.get(&index)?;
    let result = writer::delete_document(&handle, &id).await?;

    Ok(Json(DeleteDocResponse {
        _index: index,
        _id: result.id,
        _version: result.version,
        result: "deleted".to_string(),
        _shards: ShardsInfo::ok(),
        _seq_no: result.seq_no,
        _primary_term: 1,
    })
    .into_response())
}

pub async fn update_doc(
    State(registry): State<Arc<IndexRegistry>>,
    Path((index, id)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    match update_doc_inner(registry, index, id, body).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn update_doc_inner(
    registry: Arc<IndexRegistry>,
    index: String,
    id: String,
    body: serde_json::Value,
) -> Result<Response, EsError> {
    let handle = registry.get_or_create(&index).await?;

    let scripted_upsert = body
        .get("scripted_upsert")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let result = if scripted_upsert {
        // OpenMetadata uses scripted_upsert with Painless scripts.
        // Extract params from script and use as document source.
        let params = body
            .get("script")
            .and_then(|s| s.get("params"))
            .cloned()
            .unwrap_or_default();

        // Apply the script logic: put all params, remove fieldsToRemove
        let mut doc = if let Some(existing) = reader::get_document(&handle, &id).await? {
            serde_json::from_str::<serde_json::Value>(&existing.source)
                .unwrap_or(serde_json::Value::Object(Default::default()))
        } else {
            // Use upsert as base if document doesn't exist
            body.get("upsert")
                .cloned()
                .unwrap_or(serde_json::Value::Object(Default::default()))
        };

        // Merge params into document
        if let (Some(base), serde_json::Value::Object(updates)) = (doc.as_object_mut(), &params) {
            let fields_to_remove: Vec<String> = updates
                .get("fieldsToRemove")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            for (k, v) in updates {
                if k != "fieldsToRemove" {
                    base.insert(k.clone(), v.clone());
                }
            }
            for field in &fields_to_remove {
                base.remove(field);
            }
        }

        writer::index_document(&handle, id.clone(), doc).await?
    } else {
        // ES update expects {"doc": {...}, "doc_as_upsert": true} format
        let doc = body.get("doc").cloned().unwrap_or(body.clone());
        let doc_as_upsert = body
            .get("doc_as_upsert")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let upsert = body.get("upsert").cloned();

        match writer::update_document(&handle, &id, doc.clone()).await {
            Ok(r) => r,
            Err(EsError::DocumentNotFound(_, _)) if doc_as_upsert => {
                writer::index_document(&handle, id.clone(), doc).await?
            }
            Err(EsError::DocumentNotFound(_, _)) if upsert.is_some() => {
                writer::index_document(&handle, id.clone(), upsert.unwrap()).await?
            }
            Err(e) => return Err(e),
        }
    };

    Ok(Json(IndexDocResponse {
        _index: index,
        _id: result.id,
        _version: result.version,
        result: if result.created {
            "created".to_string()
        } else {
            "updated".to_string()
        },
        _shards: ShardsInfo::ok(),
        _seq_no: result.seq_no,
        _primary_term: 1,
    })
    .into_response())
}
