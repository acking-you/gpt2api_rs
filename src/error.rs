//! Shared application errors.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

/// Minimal JSON application error with explicit HTTP status.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct AppError {
    status: StatusCode,
    message: String,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.status, Json(ErrorBody { error: self.message })).into_response()
    }
}

impl AppError {
    /// Creates an application error with an explicit status code and message.
    #[must_use]
    pub fn with_status(status: StatusCode, message: impl Into<String>) -> Self {
        Self { status, message: message.into() }
    }

    /// Creates a `400 Bad Request` error.
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::BAD_REQUEST, message)
    }

    /// Creates a `401 Unauthorized` error.
    #[must_use]
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::UNAUTHORIZED, message)
    }

    /// Creates a `404 Not Found` error.
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::NOT_FOUND, message)
    }

    /// Creates a `500 Internal Server Error` error.
    #[must_use]
    pub fn internal(error: impl std::fmt::Display) -> Self {
        Self::with_status(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }

    /// Creates a `502 Bad Gateway` error.
    #[must_use]
    pub fn upstream(error: impl std::fmt::Display) -> Self {
        Self::with_status(StatusCode::BAD_GATEWAY, error.to_string())
    }
}
