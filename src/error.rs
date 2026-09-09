use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("missing or invalid project credentials")]
    Unauthorized,
    #[error("invalid event: {0}")]
    BadRequest(String),
    #[error("event pipeline is temporarily unavailable")]
    Unavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}
