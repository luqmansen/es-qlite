use serde::{Deserialize, Serialize};

use super::document::ShardsInfo;

#[derive(Debug, Deserialize)]
pub struct BulkAction {
    #[serde(default)]
    pub index: Option<BulkActionMeta>,
    #[serde(default)]
    pub create: Option<BulkActionMeta>,
    #[serde(default)]
    pub update: Option<BulkActionMeta>,
    #[serde(default)]
    pub delete: Option<BulkActionMeta>,
}

#[derive(Debug, Deserialize)]
pub struct BulkActionMeta {
    #[serde(rename = "_index")]
    pub index: Option<String>,
    #[serde(rename = "_id")]
    pub id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BulkResponse {
    pub took: u64,
    pub errors: bool,
    pub items: Vec<BulkResponseItem>,
}

#[derive(Debug, Serialize)]
pub struct BulkResponseItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<BulkItemResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<BulkItemResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<BulkItemResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<BulkItemResult>,
}

#[derive(Debug, Serialize)]
pub struct BulkItemResult {
    pub _index: String,
    #[serde(rename = "_id")]
    pub _id: String,
    pub _version: i64,
    pub result: String,
    pub _shards: ShardsInfo,
    pub _seq_no: i64,
    pub _primary_term: i64,
    pub status: u16,
}
