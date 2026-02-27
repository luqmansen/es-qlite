use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct EsErrorResponse {
    pub error: EsErrorBody,
    pub status: u16,
}

#[derive(Debug, Serialize)]
pub struct EsErrorBody {
    pub root_cause: Vec<EsErrorCause>,
    #[serde(rename = "type")]
    pub error_type: String,
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct EsErrorCause {
    #[serde(rename = "type")]
    pub error_type: String,
    pub reason: String,
}

#[derive(Debug, thiserror::Error)]
pub enum EsError {
    #[error("index [{0}] not found")]
    IndexNotFound(String),

    #[error("index [{0}] already exists")]
    IndexAlreadyExists(String),

    #[error("document [{0}] not found in index [{1}]")]
    DocumentNotFound(String, String),

    #[error("parsing error: {0}")]
    ParsingError(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("invalid index name: {0}")]
    InvalidIndexName(String),
}

impl EsError {
    fn status_code(&self) -> StatusCode {
        match self {
            EsError::IndexNotFound(_) => StatusCode::NOT_FOUND,
            EsError::IndexAlreadyExists(_) => StatusCode::BAD_REQUEST,
            EsError::DocumentNotFound(_, _) => StatusCode::NOT_FOUND,
            EsError::ParsingError(_) => StatusCode::BAD_REQUEST,
            EsError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            EsError::InvalidIndexName(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_type(&self) -> &str {
        match self {
            EsError::IndexNotFound(_) => "index_not_found_exception",
            EsError::IndexAlreadyExists(_) => "resource_already_exists_exception",
            EsError::DocumentNotFound(_, _) => "document_missing_exception",
            EsError::ParsingError(_) => "parsing_exception",
            EsError::Internal(_) => "internal_server_error",
            EsError::InvalidIndexName(_) => "invalid_index_name_exception",
        }
    }
}

impl IntoResponse for EsError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let error_type = self.error_type().to_string();
        let reason = self.to_string();

        let body = EsErrorResponse {
            status: status.as_u16(),
            error: EsErrorBody {
                root_cause: vec![EsErrorCause {
                    error_type: error_type.clone(),
                    reason: reason.clone(),
                }],
                error_type,
                reason,
            },
        };

        (status, axum::Json(body)).into_response()
    }
}

impl From<rusqlite::Error> for EsError {
    fn from(err: rusqlite::Error) -> Self {
        EsError::Internal(err.to_string())
    }
}

impl From<tokio_rusqlite::Error> for EsError {
    fn from(err: tokio_rusqlite::Error) -> Self {
        EsError::Internal(err.to_string())
    }
}

impl From<serde_json::Error> for EsError {
    fn from(err: serde_json::Error) -> Self {
        EsError::ParsingError(err.to_string())
    }
}
