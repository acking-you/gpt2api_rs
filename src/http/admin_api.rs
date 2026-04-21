//! Admin management endpoints.

use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde_json::json;

/// Shared admin-only router state.
#[derive(Debug, Clone)]
pub struct AdminState {
    /// Expected bearer token for all admin endpoints.
    pub admin_token: String,
}

/// Validates the admin bearer token from the request headers.
pub fn require_admin(headers: &HeaderMap, expected: &str) -> Result<(), StatusCode> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);

    match token {
        Some(value) if value == expected => Ok(()),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

/// Returns a minimal operational snapshot for the admin control plane.
pub async fn status(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    require_admin(&headers, &state.admin_token)?;
    Ok(Json(json!({
        "accounts_total": 0,
        "accounts_active": 0,
        "accounts_limited": 0,
        "accounts_invalid": 0,
        "outbox_backlog": 0
    })))
}
