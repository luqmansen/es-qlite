use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

use crate::error::EsError;
use crate::storage::{reader, registry::IndexRegistry};

#[derive(Debug, Deserialize)]
pub struct MgetRequest {
    #[serde(default)]
    pub docs: Vec<MgetDoc>,
    #[serde(default)]
    pub ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct MgetDoc {
    #[serde(rename = "_index")]
    pub index: Option<String>,
    #[serde(rename = "_id")]
    pub id: String,
}

pub async fn mget(
    State(registry): State<Arc<IndexRegistry>>,
    Json(body): Json<MgetRequest>,
) -> Response {
    match mget_inner(registry, body).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn mget_inner(registry: Arc<IndexRegistry>, body: MgetRequest) -> Result<Response, EsError> {
    let mut docs = Vec::new();

    for doc in &body.docs {
        let index_name = doc.index.as_deref().unwrap_or("");
        match registry.get(index_name) {
            Ok(handle) => {
                let record = reader::get_document(&handle, &doc.id).await?;
                match record {
                    Some(r) => {
                        let source: serde_json::Value = serde_json::from_str(&r.source)?;
                        docs.push(serde_json::json!({
                            "_index": index_name,
                            "_id": r.id,
                            "_version": r.version,
                            "_seq_no": r.seq_no,
                            "_primary_term": 1,
                            "found": true,
                            "_source": source,
                        }));
                    }
                    None => {
                        docs.push(serde_json::json!({
                            "_index": index_name,
                            "_id": doc.id,
                            "found": false,
                        }));
                    }
                }
            }
            Err(_) => {
                docs.push(serde_json::json!({
                    "_index": index_name,
                    "_id": doc.id,
                    "found": false,
                }));
            }
        }
    }

    Ok(Json(serde_json::json!({ "docs": docs })).into_response())
}

pub async fn mget_index(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
    Json(body): Json<MgetRequest>,
) -> Response {
    match mget_index_inner(registry, index, body).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn mget_index_inner(
    registry: Arc<IndexRegistry>,
    index: String,
    body: MgetRequest,
) -> Result<Response, EsError> {
    let handle = registry.get(&index)?;
    let mut docs = Vec::new();

    let ids: Vec<String> = if !body.ids.is_empty() {
        body.ids
    } else {
        body.docs.into_iter().map(|d| d.id).collect()
    };

    for id in &ids {
        let record = reader::get_document(&handle, id).await?;
        match record {
            Some(r) => {
                let source: serde_json::Value = serde_json::from_str(&r.source)?;
                docs.push(serde_json::json!({
                    "_index": index,
                    "_id": r.id,
                    "_version": r.version,
                    "_seq_no": r.seq_no,
                    "_primary_term": 1,
                    "found": true,
                    "_source": source,
                }));
            }
            None => {
                docs.push(serde_json::json!({
                    "_index": index,
                    "_id": id,
                    "found": false,
                }));
            }
        }
    }

    Ok(Json(serde_json::json!({ "docs": docs })).into_response())
}
