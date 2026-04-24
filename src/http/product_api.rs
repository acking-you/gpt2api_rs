//! Product workspace APIs authenticated by downstream API keys.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    error::AppError,
    models::{ApiKeyRecord, ApiKeyRole, SessionDetail},
    service::{AppService, PublicAuthError, PublicAuthFailure},
};

/// Query parameters for session listings.
#[derive(Debug, Default, Deserialize)]
pub struct SessionListQuery {
    /// Optional maximum number of sessions.
    #[serde(default)]
    limit: Option<u64>,
    /// Optional updated-at cursor.
    #[serde(default)]
    cursor: Option<i64>,
    /// Optional admin key filter.
    #[serde(default)]
    key_id: Option<String>,
    /// Optional admin title search.
    #[serde(default)]
    q: Option<String>,
}

/// Body for creating one session.
#[derive(Debug, Default, Deserialize)]
pub struct CreateSessionRequest {
    /// Optional session title.
    #[serde(default)]
    title: Option<String>,
}

/// Body for updating notification settings.
#[derive(Debug, Default, Deserialize)]
pub struct NotificationRequest {
    /// Optional notification email. Empty string clears it.
    #[serde(default)]
    notification_email: Option<String>,
    /// Optional notification toggle.
    #[serde(default)]
    notification_enabled: Option<bool>,
}

/// Body for patching one session.
#[derive(Debug, Default, Deserialize)]
pub struct PatchSessionRequest {
    /// Optional title replacement.
    #[serde(default)]
    title: Option<String>,
    /// Optional status replacement.
    #[serde(default)]
    status: Option<String>,
}

/// Verifies a product API key.
pub async fn verify_auth(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    Ok(Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "key": serialize_product_key(&key),
    })))
}

/// Returns the current key profile.
pub async fn get_me(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    Ok(Json(json!({ "key": serialize_product_key(&key) })))
}

/// Updates current key notification settings.
pub async fn update_my_notification(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<NotificationRequest>,
) -> Result<Json<Value>, AppError> {
    let mut key = authenticate_product_key(&service, &headers).await?;
    if let Some(email) = body.notification_email {
        key.notification_email = normalize_optional_email(&email);
    }
    if let Some(enabled) = body.notification_enabled {
        key.notification_enabled = enabled;
    }
    service.storage().control.upsert_api_key(&key).await.map_err(AppError::internal)?;
    Ok(Json(json!({ "key": serialize_product_key(&key) })))
}

/// Lists sessions for the current key.
pub async fn list_sessions(
    Query(query): Query<SessionListQuery>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let items = service
        .storage()
        .control
        .list_sessions_for_key(&key.id, query.limit.unwrap_or(50), query.cursor)
        .await
        .map_err(AppError::internal)?;
    Ok(Json(json!({ "items": items })))
}

/// Creates a web session for the current key.
pub async fn create_session(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<CreateSessionRequest>,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let session = service
        .create_web_session(&key, body.title.as_deref())
        .await
        .map_err(AppError::internal)?;
    Ok(Json(json!({ "session": session })))
}

/// Returns one key-scoped session detail.
pub async fn get_session(
    Path(session_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<SessionDetail>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let detail = service
        .get_session_detail_for_key(&key, &session_id)
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("session not found"))?;
    Ok(Json(detail))
}

/// Patches one key-scoped session.
pub async fn patch_session(
    Path(session_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<PatchSessionRequest>,
) -> Result<Json<SessionDetail>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    if body.title.is_none() && body.status.is_none() {
        let detail = service
            .get_session_detail_for_key(&key, &session_id)
            .await
            .map_err(AppError::internal)?
            .ok_or_else(|| AppError::not_found("session not found"))?;
        return Ok(Json(detail));
    }
    let detail = service
        .patch_session_for_key(&key, &session_id, body.title.as_deref(), body.status.as_deref())
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("session not found"))?;
    Ok(Json(detail))
}

/// Lists sessions across keys for product-admin API keys.
pub async fn list_admin_sessions(
    Query(query): Query<SessionListQuery>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    if !service.is_product_admin(&key) {
        return Err(AppError::with_status(StatusCode::FORBIDDEN, "admin role required"));
    }
    let items = service
        .storage()
        .control
        .search_sessions_for_admin(
            query.key_id.as_deref(),
            query.q.as_deref(),
            query.limit.unwrap_or(50),
            query.cursor,
        )
        .await
        .map_err(AppError::internal)?;
    Ok(Json(json!({ "items": items })))
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

async fn authenticate_product_key(
    service: &Arc<AppService>,
    headers: &HeaderMap,
) -> Result<ApiKeyRecord, AppError> {
    let bearer = extract_bearer_token(headers)
        .ok_or_else(|| map_product_auth_error(PublicAuthError::InvalidKey))?;
    service.authenticate_product_key(&bearer).await.map_err(map_product_auth_failure)
}

fn map_product_auth_failure(error: PublicAuthFailure) -> AppError {
    match error {
        PublicAuthFailure::Auth(error) => map_product_auth_error(error),
        PublicAuthFailure::Internal(error) => AppError::internal(error),
    }
}

fn map_product_auth_error(error: PublicAuthError) -> AppError {
    match error {
        PublicAuthError::InvalidKey => {
            AppError::with_status(StatusCode::UNAUTHORIZED, error.to_string())
        }
        PublicAuthError::Disabled | PublicAuthError::QuotaExhausted => {
            AppError::with_status(StatusCode::FORBIDDEN, error.to_string())
        }
    }
}

fn serialize_product_key(key: &ApiKeyRecord) -> Value {
    json!({
        "id": key.id,
        "name": key.name,
        "status": key.status,
        "role": match key.role {
            ApiKeyRole::User => "user",
            ApiKeyRole::Admin => "admin",
        },
        "quota_total_calls": key.quota_total_calls,
        "quota_used_calls": key.quota_used_calls,
        "route_strategy": key.route_strategy,
        "account_group_id": key.account_group_id,
        "request_max_concurrency": key.request_max_concurrency,
        "request_min_start_interval_ms": key.request_min_start_interval_ms,
        "notification_email": key.notification_email,
        "notification_enabled": key.notification_enabled,
    })
}

fn normalize_optional_email(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}
