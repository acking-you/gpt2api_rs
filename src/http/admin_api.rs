//! Admin management endpoints.

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    models::{AccountRecord, ApiKeyRecord, UsageEventRecord},
    storage::Storage,
};

/// Shared admin-only router state.
#[derive(Debug, Clone)]
pub struct AdminState {
    /// Expected bearer token for all admin endpoints.
    pub admin_token: String,
    /// Optional attached runtime storage used by live handlers.
    pub storage: Option<Storage>,
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
    if let Some(storage) = &state.storage {
        let accounts =
            storage.control.list_accounts().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let outbox_backlog = storage
            .control
            .list_pending_outbox()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let accounts_active = accounts.iter().filter(|record| record.status == "active").count();
        let accounts_limited = accounts.iter().filter(|record| record.status == "limited").count();
        let accounts_invalid = accounts.iter().filter(|record| record.status == "invalid").count();
        return Ok(Json(json!({
            "accounts_total": accounts.len(),
            "accounts_active": accounts_active,
            "accounts_limited": accounts_limited,
            "accounts_invalid": accounts_invalid,
            "outbox_backlog": outbox_backlog.len()
        })));
    }

    Ok(Json(json!({
        "accounts_total": 0,
        "accounts_active": 0,
        "accounts_limited": 0,
        "accounts_invalid": 0,
        "outbox_backlog": 0
    })))
}

/// Lists imported accounts visible to the admin control plane.
pub async fn list_accounts(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<Vec<AccountRecord>>, StatusCode> {
    require_admin(&headers, &state.admin_token)?;
    let accounts = match &state.storage {
        Some(storage) => {
            storage.control.list_accounts().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        }
        None => Vec::new(),
    };
    Ok(Json(accounts))
}

/// Lists configured downstream API keys.
pub async fn list_keys(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ApiKeyRecord>>, StatusCode> {
    require_admin(&headers, &state.admin_token)?;
    let keys = match &state.storage {
        Some(storage) => {
            storage.control.list_api_keys().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        }
        None => Vec::new(),
    };
    Ok(Json(keys))
}

/// Query parameters supported by the usage listing endpoint.
#[derive(Debug, Deserialize)]
pub struct UsageQuery {
    /// Optional maximum number of rows to return.
    pub limit: Option<u64>,
}

/// Lists recent usage-event summaries.
pub async fn list_usage(
    Query(query): Query<UsageQuery>,
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<Vec<UsageEventRecord>>, StatusCode> {
    require_admin(&headers, &state.admin_token)?;
    let limit = query.limit.unwrap_or(50);
    let usage = match &state.storage {
        Some(storage) => storage
            .events
            .list_recent_usage(limit)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        None => Vec::new(),
    };
    Ok(Json(usage))
}
