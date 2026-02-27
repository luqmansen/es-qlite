use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexMapping {
    #[serde(default)]
    pub properties: HashMap<String, FieldMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    #[serde(rename = "type", default = "default_field_type")]
    pub field_type: FieldType,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<HashMap<String, FieldMapping>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, FieldMapping>>,

    /// Catch-all for additional mapping options (analyzer, normalizer, copy_to, etc.)
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

fn default_field_type() -> FieldType {
    FieldType::Text
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Text,
    Keyword,
    Long,
    Integer,
    Short,
    Byte,
    Float,
    Double,
    HalfFloat,
    Boolean,
    Date,
    Object,
    Nested,
    Alias,
    Flattened,
    Wildcard,
    ScaledFloat,
    Completion,
    SearchAsYouType,
    ConstantKeyword,
    FlatObject,
    IcuCollationKeyword,
    Join,
    RankFeature,
    RankFeatures,
    Ip,
    #[serde(other)]
    Unknown,
}

impl FieldType {
    pub fn is_text(&self) -> bool {
        matches!(self, FieldType::Text)
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            FieldType::Long
                | FieldType::Integer
                | FieldType::Short
                | FieldType::Byte
                | FieldType::Float
                | FieldType::Double
                | FieldType::HalfFloat
        )
    }

    pub fn sqlite_type(&self) -> &str {
        match self {
            FieldType::Text
            | FieldType::Keyword
            | FieldType::Date
            | FieldType::Completion
            | FieldType::SearchAsYouType
            | FieldType::ConstantKeyword
            | FieldType::Wildcard
            | FieldType::Ip => "TEXT",
            FieldType::Long
            | FieldType::Integer
            | FieldType::Short
            | FieldType::Byte
            | FieldType::Boolean
            | FieldType::RankFeature => "INTEGER",
            FieldType::Float
            | FieldType::Double
            | FieldType::HalfFloat
            | FieldType::ScaledFloat => "REAL",
            FieldType::Object
            | FieldType::Nested
            | FieldType::Flattened
            | FieldType::FlatObject
            | FieldType::Join
            | FieldType::RankFeatures => "TEXT",
            // Unknown types, alias, etc. - store as TEXT
            _ => "TEXT",
        }
    }

    pub fn detect(value: &serde_json::Value) -> FieldType {
        match value {
            serde_json::Value::Bool(_) => FieldType::Boolean,
            serde_json::Value::Number(n) => {
                if n.is_f64() {
                    FieldType::Float
                } else {
                    FieldType::Long
                }
            }
            serde_json::Value::String(s) => {
                // Try to detect date patterns
                if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                    return FieldType::Date;
                }
                if chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok() {
                    return FieldType::Date;
                }
                FieldType::Text
            }
            serde_json::Value::Object(_) => FieldType::Object,
            serde_json::Value::Array(arr) => {
                if let Some(first) = arr.first() {
                    FieldType::detect(first)
                } else {
                    FieldType::Text
                }
            }
            serde_json::Value::Null => FieldType::Text,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexSettings {
    #[serde(default)]
    pub number_of_shards: Option<u32>,
    #[serde(default)]
    pub number_of_replicas: Option<u32>,
    /// Catch-all for additional settings
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateIndexRequest {
    #[serde(default)]
    pub mappings: Option<IndexMapping>,
    #[serde(default)]
    pub settings: Option<IndexSettings>,
    /// Catch-all for additional top-level fields (aliases, etc.)
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}
