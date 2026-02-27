use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Instant;

use crate::error::EsError;
use crate::model::document::ShardsInfo;
use crate::model::search::{
    AggregationResult, CountResponse, Hit, HitsEnvelope, HitsTotal, SearchRequest, SearchResponse,
};
use crate::query::{parser, translator};
use crate::storage::{reader, registry::IndexRegistry, writer};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Default)]
pub struct SearchQueryParams {
    pub from: Option<usize>,
    pub size: Option<usize>,
    pub typed_keys: Option<bool>,
    pub rest_total_hits_as_int: Option<bool>,
}

pub async fn search(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
    Query(params): Query<SearchQueryParams>,
    body: Option<Json<SearchRequest>>,
) -> Response {
    match search_inner(registry, index, params, body).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

/// Serialize a SearchResponse, optionally flattening hits.total to a plain integer.
fn search_response_to_json(resp: SearchResponse, total_as_int: bool) -> Response {
    if !total_as_int {
        return Json(resp).into_response();
    }
    let mut val = serde_json::to_value(&resp).unwrap();
    if let Some(total_obj) = val.get("hits").and_then(|h| h.get("total")) {
        let total_int = total_obj.get("value").and_then(|v| v.as_u64()).unwrap_or(0);
        val["hits"]["total"] = serde_json::Value::from(total_int);
    }
    Json(val).into_response()
}

async fn search_inner(
    registry: Arc<IndexRegistry>,
    index: String,
    params: SearchQueryParams,
    body: Option<Json<SearchRequest>>,
) -> Result<Response, EsError> {
    let req = body.map(|b| b.0).unwrap_or_default();
    let total_as_int = params.rest_total_hits_as_int.unwrap_or(false);

    // Query params override body values
    let from = params.from.or(req.from).unwrap_or(0);
    let size = params.size.or(req.size).unwrap_or(10);

    let query = match &req.query {
        Some(q) => parser::parse_query(q)?,
        None => crate::query::ast::Query::MatchAll,
    };

    // Resolve index name (supports exact name, aliases, wildcards, comma-separated)
    let handles = registry.resolve_name(&index);

    if handles.is_empty() {
        // For wildcard/alias patterns, return empty results
        // For exact names, return index_not_found
        if IndexRegistry::is_pattern(&index) || !registry.exists(&index) {
            let envelope = HitsEnvelope {
                total: HitsTotal {
                    value: 0,
                    relation: "eq".to_string(),
                },
                max_score: None,
                hits: vec![],
            };
            // If aggregations were requested, return empty buckets so frontends
            // don't crash trying to iterate undefined
            let aggregations = if let Some(aggs_val) = &req.aggs {
                let defs = reader::parse_aggs(aggs_val);
                if !defs.is_empty() {
                    let mut empty_aggs = HashMap::new();
                    for name in defs.keys() {
                        empty_aggs.insert(
                            name.clone(),
                            AggregationResult {
                                doc_count_error_upper_bound: Some(0),
                                sum_other_doc_count: Some(0),
                                buckets: Some(vec![]),
                                value: None,
                            },
                        );
                    }
                    Some(apply_typed_keys(
                        empty_aggs,
                        params.typed_keys.unwrap_or(false),
                    ))
                } else {
                    None
                }
            } else {
                None
            };
            return Ok(search_response_to_json(
                SearchResponse {
                    took: 0,
                    timed_out: false,
                    _shards: ShardsInfo::ok(),
                    hits: envelope,
                    aggregations,
                },
                total_as_int,
            ));
        }
        return Err(EsError::IndexNotFound(index));
    }

    // Parse aggregations if present
    let agg_defs = req.aggs.as_ref().map(reader::parse_aggs);
    let typed_keys = params.typed_keys.unwrap_or(false);

    // Single index - direct path (no need for merge)
    if handles.len() == 1 {
        let handle = &handles[0];
        let mapping = handle.mapping.read().clone();
        let mut translated = translator::translate(&query, &mapping);
        if let Some(sort_val) = &req.sort {
            translated.sort_clauses = translator::parse_sort(sort_val, &mapping);
        }

        let start = Instant::now();
        let (hits, _) =
            reader::search(handle, translated.clone(), from, size, handle.name.clone()).await?;

        // Execute aggregations if requested
        let aggregations = if let Some(ref defs) = agg_defs {
            if !defs.is_empty() {
                match reader::aggregate(handle, translated, defs.clone()).await {
                    Ok(aggs) => Some(apply_typed_keys(aggs, typed_keys)),
                    Err(e) => {
                        tracing::warn!("Aggregation error: {}", e);
                        Some(HashMap::new())
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        let took = start.elapsed().as_millis() as u64;

        return Ok(search_response_to_json(
            SearchResponse {
                took,
                timed_out: false,
                _shards: ShardsInfo::ok(),
                hits,
                aggregations,
            },
            total_as_int,
        ));
    }

    // Multi-index search - merge results
    let start = Instant::now();
    let mut all_hits: Vec<Hit> = Vec::new();
    let mut total_count: u64 = 0;
    let mut max_score: Option<f64> = None;
    let mut merged_aggs: HashMap<String, HashMap<serde_json::Value, u64>> = HashMap::new();

    for handle in &handles {
        let mapping = handle.mapping.read().clone();
        let mut translated = translator::translate(&query, &mapping);
        if let Some(sort_val) = &req.sort {
            translated.sort_clauses = translator::parse_sort(sort_val, &mapping);
        }
        match reader::search(
            handle,
            translated.clone(),
            0,
            from + size,
            handle.name.clone(),
        )
        .await
        {
            Ok((hits, _)) => {
                total_count += hits.total.value;
                if let Some(ms) = hits.max_score {
                    max_score = Some(max_score.map_or(ms, |m: f64| m.max(ms)));
                }
                all_hits.extend(hits.hits);
            }
            Err(e) => {
                tracing::warn!("Error searching index {}: {}", handle.name, e);
            }
        }

        // Execute aggregations per index and merge
        if let Some(ref defs) = agg_defs {
            if !defs.is_empty() {
                if let Ok(aggs) = reader::aggregate(handle, translated, defs.clone()).await {
                    for (name, result) in aggs {
                        if let Some(buckets) = result.buckets {
                            let entry = merged_aggs.entry(name).or_default();
                            for bucket in buckets {
                                *entry.entry(bucket.key).or_insert(0) += bucket.doc_count;
                            }
                        }
                    }
                }
            }
        }
    }

    all_hits.sort_by(|a, b| {
        b._score
            .unwrap_or(0.0)
            .partial_cmp(&a._score.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let paginated: Vec<Hit> = all_hits.into_iter().skip(from).take(size).collect();
    let took = start.elapsed().as_millis() as u64;

    // Convert merged aggregations to response format
    let aggregations = if !merged_aggs.is_empty() {
        let mut agg_results = HashMap::new();
        for (name, counts) in merged_aggs {
            let mut buckets: Vec<_> = counts
                .into_iter()
                .map(|(key, count)| crate::model::search::AggBucket {
                    key,
                    doc_count: count,
                })
                .collect();
            buckets.sort_by(|a, b| b.doc_count.cmp(&a.doc_count));
            agg_results.insert(
                name,
                AggregationResult {
                    doc_count_error_upper_bound: Some(0),
                    sum_other_doc_count: Some(0),
                    buckets: Some(buckets),
                    value: None,
                },
            );
        }
        Some(apply_typed_keys(agg_results, typed_keys))
    } else {
        None
    };

    let envelope = HitsEnvelope {
        total: HitsTotal {
            value: total_count,
            relation: "eq".to_string(),
        },
        max_score,
        hits: paginated,
    };
    Ok(search_response_to_json(
        SearchResponse {
            took,
            timed_out: false,
            _shards: ShardsInfo::ok(),
            hits: envelope,
            aggregations,
        },
        total_as_int,
    ))
}

pub async fn count(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
    body: Option<Json<SearchRequest>>,
) -> Response {
    match count_inner(registry, index, body).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn count_inner(
    registry: Arc<IndexRegistry>,
    index: String,
    body: Option<Json<SearchRequest>>,
) -> Result<Response, EsError> {
    let req = body.map(|b| b.0).unwrap_or_default();

    let query = match &req.query {
        Some(q) => parser::parse_query(q)?,
        None => crate::query::ast::Query::MatchAll,
    };

    let handles = registry.resolve_name(&index);
    let mut total: u64 = 0;

    if handles.is_empty() && !IndexRegistry::is_pattern(&index) {
        return Err(EsError::IndexNotFound(index));
    }

    for handle in &handles {
        let mapping = handle.mapping.read().clone();
        let translated = translator::translate(&query, &mapping);
        match reader::count(handle, translated).await {
            Ok(count) => total += count,
            Err(e) => tracing::warn!("Error counting index {}: {}", handle.name, e),
        }
    }

    Ok(Json(CountResponse {
        count: total,
        _shards: ShardsInfo::ok(),
    })
    .into_response())
}

pub async fn delete_by_query(
    State(registry): State<Arc<IndexRegistry>>,
    Path(index): Path<String>,
    body: Option<Json<SearchRequest>>,
) -> Response {
    match delete_by_query_inner(registry, index, body).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

/// When typed_keys=true, prefix aggregation names with their type.
/// For terms aggregations: "sterms#name"
fn apply_typed_keys(
    aggs: HashMap<String, AggregationResult>,
    typed_keys: bool,
) -> HashMap<String, AggregationResult> {
    if !typed_keys {
        return aggs;
    }
    aggs.into_iter()
        .map(|(name, result)| {
            let prefixed = if result.buckets.is_some() {
                format!("sterms#{name}")
            } else {
                name
            };
            (prefixed, result)
        })
        .collect()
}

async fn delete_by_query_inner(
    registry: Arc<IndexRegistry>,
    index: String,
    body: Option<Json<SearchRequest>>,
) -> Result<Response, EsError> {
    let handle = registry.get(&index)?;
    let req = body.map(|b| b.0).unwrap_or_default();

    let query = match &req.query {
        Some(q) => parser::parse_query(q)?,
        None => crate::query::ast::Query::MatchAll,
    };

    let mapping = handle.mapping.read().clone();
    let translated = translator::translate(&query, &mapping);

    let start = Instant::now();
    // Get all matching doc IDs
    let (hits, _) = reader::search(&handle, translated, 0, 10000, index.clone()).await?;
    let total = hits.total.value;
    let mut deleted = 0u64;

    for hit in &hits.hits {
        if writer::delete_document(&handle, &hit._id).await.is_ok() {
            deleted += 1;
        }
    }
    let took = start.elapsed().as_millis() as u64;

    Ok(Json(serde_json::json!({
        "took": took,
        "timed_out": false,
        "total": total,
        "deleted": deleted,
        "batches": 1,
        "version_conflicts": 0,
        "noops": 0,
        "retries": { "bulk": 0, "search": 0 },
        "failures": []
    }))
    .into_response())
}

// ─── Multi-Search (_msearch) ─────────────────────────────────────────────

pub async fn msearch(
    State(registry): State<Arc<IndexRegistry>>,
    Query(params): Query<SearchQueryParams>,
    body: Bytes,
) -> Response {
    match msearch_inner(registry, params, body).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn msearch_inner(
    registry: Arc<IndexRegistry>,
    params: SearchQueryParams,
    body: Bytes,
) -> Result<Response, EsError> {
    let body_str = String::from_utf8_lossy(&body);
    let lines: Vec<&str> = body_str.lines().filter(|l| !l.is_empty()).collect();
    let total_as_int = params.rest_total_hits_as_int.unwrap_or(false);

    let mut responses: Vec<serde_json::Value> = Vec::new();

    let mut i = 0;
    while i + 1 < lines.len() {
        // Header line: {"index": "some-index", "ignore_unavailable": true, ...}
        let header: serde_json::Value =
            serde_json::from_str(lines[i]).unwrap_or(serde_json::json!({}));
        let search_body: SearchRequest = serde_json::from_str(lines[i + 1]).unwrap_or_default();
        i += 2;

        // Extract index from header (ES accepts both "index" and "indices")
        let index_val = header.get("index").or_else(|| header.get("indices"));
        let index = index_val
            .and_then(|v| match v {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Array(arr) => {
                    let names: Vec<String> = arr
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect();
                    if names.is_empty() {
                        None
                    } else {
                        Some(names.join(","))
                    }
                }
                _ => None,
            })
            .unwrap_or_default();

        // Build a SearchQueryParams for this sub-request
        let sub_params = SearchQueryParams {
            from: search_body.from,
            size: search_body.size,
            typed_keys: params.typed_keys,
            rest_total_hits_as_int: params.rest_total_hits_as_int,
        };

        // Execute the search
        let result =
            search_inner(registry.clone(), index, sub_params, Some(Json(search_body))).await;

        match result {
            Ok(resp) => {
                // Extract the JSON body from the response
                let (parts, body) = resp.into_parts();
                let body_bytes = axum::body::to_bytes(body, usize::MAX)
                    .await
                    .unwrap_or_default();
                let mut json_val: serde_json::Value =
                    serde_json::from_slice(&body_bytes).unwrap_or(serde_json::json!({}));
                json_val["status"] = serde_json::json!(parts.status.as_u16());
                responses.push(json_val);
            }
            Err(e) => {
                // Return error as a response item
                let status = match &e {
                    EsError::IndexNotFound(_) => 404,
                    _ => 400,
                };
                responses.push(serde_json::json!({
                    "error": {
                        "type": "search_phase_execution_exception",
                        "reason": e.to_string()
                    },
                    "status": status
                }));
            }
        }
    }

    // Apply rest_total_hits_as_int to each response if needed
    if total_as_int {
        for resp in &mut responses {
            if let Some(total_obj) = resp.get("hits").and_then(|h| h.get("total")) {
                if total_obj.is_object() {
                    let total_int = total_obj.get("value").and_then(|v| v.as_u64()).unwrap_or(0);
                    resp["hits"]["total"] = serde_json::Value::from(total_int);
                }
            }
        }
    }

    Ok(Json(serde_json::json!({ "responses": responses })).into_response())
}
