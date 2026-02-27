use axum::body::Bytes;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::sync::Arc;
use std::time::Instant;

use crate::error::EsError;
use crate::model::bulk::{BulkAction, BulkItemResult, BulkResponse, BulkResponseItem};
use crate::model::document::ShardsInfo;
use crate::storage::{registry::IndexRegistry, writer};

pub async fn bulk(State(registry): State<Arc<IndexRegistry>>, body: Bytes) -> Response {
    match bulk_inner(registry, body).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn bulk_inner(registry: Arc<IndexRegistry>, body: Bytes) -> Result<Response, EsError> {
    let body_str = String::from_utf8_lossy(&body);
    let start = Instant::now();
    let lines: Vec<&str> = body_str.lines().filter(|l| !l.is_empty()).collect();

    let mut items = Vec::new();
    let mut errors = false;
    let mut i = 0;

    while i < lines.len() {
        let action: BulkAction = serde_json::from_str(lines[i])?;

        if let Some(meta) = &action.delete {
            let index_name = meta.index.as_deref().unwrap_or("");
            let id = meta.id.as_deref().unwrap_or("");

            match registry.get(index_name) {
                Ok(handle) => match writer::delete_document(&handle, id).await {
                    Ok(result) => {
                        items.push(BulkResponseItem {
                            index: None,
                            create: None,
                            update: None,
                            delete: Some(BulkItemResult {
                                _index: index_name.to_string(),
                                _id: result.id,
                                _version: result.version,
                                result: "deleted".to_string(),
                                _shards: ShardsInfo::ok(),
                                _seq_no: result.seq_no,
                                _primary_term: 1,
                                status: 200,
                            }),
                        });
                    }
                    Err(_) => {
                        errors = true;
                        items.push(BulkResponseItem {
                            index: None,
                            create: None,
                            update: None,
                            delete: Some(BulkItemResult {
                                _index: index_name.to_string(),
                                _id: id.to_string(),
                                _version: 0,
                                result: "not_found".to_string(),
                                _shards: ShardsInfo::ok(),
                                _seq_no: 0,
                                _primary_term: 1,
                                status: 404,
                            }),
                        });
                    }
                },
                Err(_) => {
                    errors = true;
                    items.push(BulkResponseItem {
                        index: None,
                        create: None,
                        update: None,
                        delete: Some(BulkItemResult {
                            _index: index_name.to_string(),
                            _id: id.to_string(),
                            _version: 0,
                            result: "not_found".to_string(),
                            _shards: ShardsInfo::ok(),
                            _seq_no: 0,
                            _primary_term: 1,
                            status: 404,
                        }),
                    });
                }
            }
            i += 1;
            continue;
        }

        if i + 1 >= lines.len() {
            break;
        }
        let body_line = lines[i + 1];
        let doc: serde_json::Value = serde_json::from_str(body_line)?;

        if let Some(meta) = &action.index {
            let index_name = meta.index.as_deref().unwrap_or("").to_string();
            let id = meta
                .id
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            match registry.get_or_create(&index_name).await {
                Ok(handle) => {
                    let result = writer::index_document(&handle, id, doc).await?;
                    items.push(BulkResponseItem {
                        index: Some(BulkItemResult {
                            _index: index_name,
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
                            status: if result.created { 201 } else { 200 },
                        }),
                        create: None,
                        update: None,
                        delete: None,
                    });
                }
                Err(e) => {
                    errors = true;
                    items.push(BulkResponseItem {
                        index: Some(BulkItemResult {
                            _index: index_name,
                            _id: String::new(),
                            _version: 0,
                            result: e.to_string(),
                            _shards: ShardsInfo::ok(),
                            _seq_no: 0,
                            _primary_term: 1,
                            status: 404,
                        }),
                        create: None,
                        update: None,
                        delete: None,
                    });
                }
            }
        } else if let Some(meta) = &action.create {
            let index_name = meta.index.as_deref().unwrap_or("").to_string();
            let id = meta
                .id
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            match registry.get_or_create(&index_name).await {
                Ok(handle) => {
                    let result = writer::index_document(&handle, id, doc).await?;
                    items.push(BulkResponseItem {
                        index: None,
                        create: Some(BulkItemResult {
                            _index: index_name,
                            _id: result.id,
                            _version: result.version,
                            result: "created".to_string(),
                            _shards: ShardsInfo::ok(),
                            _seq_no: result.seq_no,
                            _primary_term: 1,
                            status: 201,
                        }),
                        update: None,
                        delete: None,
                    });
                }
                Err(e) => {
                    errors = true;
                    items.push(BulkResponseItem {
                        index: None,
                        create: Some(BulkItemResult {
                            _index: index_name,
                            _id: String::new(),
                            _version: 0,
                            result: e.to_string(),
                            _shards: ShardsInfo::ok(),
                            _seq_no: 0,
                            _primary_term: 1,
                            status: 404,
                        }),
                        update: None,
                        delete: None,
                    });
                }
            }
        } else if let Some(meta) = &action.update {
            let index_name = meta.index.as_deref().unwrap_or("").to_string();
            let id = meta.id.as_deref().unwrap_or("").to_string();
            let update_doc = doc.get("doc").cloned().unwrap_or(doc);

            match registry.get(&index_name) {
                Ok(handle) => match writer::update_document(&handle, &id, update_doc).await {
                    Ok(result) => {
                        items.push(BulkResponseItem {
                            index: None,
                            create: None,
                            update: Some(BulkItemResult {
                                _index: index_name,
                                _id: result.id,
                                _version: result.version,
                                result: "updated".to_string(),
                                _shards: ShardsInfo::ok(),
                                _seq_no: result.seq_no,
                                _primary_term: 1,
                                status: 200,
                            }),
                            delete: None,
                        });
                    }
                    Err(e) => {
                        errors = true;
                        items.push(BulkResponseItem {
                            index: None,
                            create: None,
                            update: Some(BulkItemResult {
                                _index: index_name,
                                _id: id,
                                _version: 0,
                                result: e.to_string(),
                                _shards: ShardsInfo::ok(),
                                _seq_no: 0,
                                _primary_term: 1,
                                status: 404,
                            }),
                            delete: None,
                        });
                    }
                },
                Err(e) => {
                    errors = true;
                    items.push(BulkResponseItem {
                        index: None,
                        create: None,
                        update: Some(BulkItemResult {
                            _index: index_name,
                            _id: id,
                            _version: 0,
                            result: e.to_string(),
                            _shards: ShardsInfo::ok(),
                            _seq_no: 0,
                            _primary_term: 1,
                            status: 404,
                        }),
                        delete: None,
                    });
                }
            }
        }

        i += 2;
    }

    let took = start.elapsed().as_millis() as u64;

    Ok(Json(BulkResponse {
        took,
        errors,
        items,
    })
    .into_response())
}
