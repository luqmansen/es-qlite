use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::document::ShardsInfo;

#[derive(Debug, Deserialize, Default)]
pub struct SearchRequest {
    #[serde(default)]
    pub query: Option<serde_json::Value>,
    #[serde(default)]
    pub from: Option<usize>,
    #[serde(default)]
    pub size: Option<usize>,
    #[serde(default)]
    pub sort: Option<serde_json::Value>,
    #[serde(default)]
    pub _source: Option<serde_json::Value>,
    #[serde(default, alias = "aggregations")]
    pub aggs: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub took: u64,
    pub timed_out: bool,
    pub _shards: ShardsInfo,
    pub hits: HitsEnvelope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<HashMap<String, AggregationResult>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct AggregationResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_count_error_upper_bound: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sum_other_doc_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<Vec<AggBucket>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Clone)]
pub struct AggBucket {
    pub key: serde_json::Value,
    pub doc_count: u64,
}

#[derive(Debug, Serialize)]
pub struct HitsEnvelope {
    pub total: HitsTotal,
    pub max_score: Option<f64>,
    pub hits: Vec<Hit>,
}

#[derive(Debug, Serialize)]
pub struct HitsTotal {
    pub value: u64,
    pub relation: String,
}

#[derive(Debug, Serialize)]
pub struct Hit {
    pub _index: String,
    #[serde(rename = "_id")]
    pub _id: String,
    pub _score: Option<f64>,
    pub _source: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct CountResponse {
    pub count: u64,
    pub _shards: ShardsInfo,
}
