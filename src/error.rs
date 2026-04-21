//! Shared application errors.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

/// Minimal application error type for the initial router scaffolding.
#[derive(Debug, Error)]
pub enum AppError {
    /// Catch-all internal failure.
    #[error("internal error")]
    Internal,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody { error: self.to_string() }))
            .into_response()
    }
}
