use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct IndexDocResponse {
    pub _index: String,
    #[serde(rename = "_id")]
    pub _id: String,
    pub _version: i64,
    pub result: String,
    pub _shards: ShardsInfo,
    pub _seq_no: i64,
    pub _primary_term: i64,
}

#[derive(Debug, Serialize)]
pub struct GetDocResponse {
    pub _index: String,
    #[serde(rename = "_id")]
    pub _id: String,
    pub _version: i64,
    pub _seq_no: i64,
    pub _primary_term: i64,
    pub found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _source: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct DeleteDocResponse {
    pub _index: String,
    #[serde(rename = "_id")]
    pub _id: String,
    pub _version: i64,
    pub result: String,
    pub _shards: ShardsInfo,
    pub _seq_no: i64,
    pub _primary_term: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShardsInfo {
    pub total: u32,
    pub successful: u32,
    pub skipped: u32,
    pub failed: u32,
}

impl ShardsInfo {
    pub fn ok() -> Self {
        ShardsInfo {
            total: 1,
            successful: 1,
            skipped: 0,
            failed: 0,
        }
    }
}
