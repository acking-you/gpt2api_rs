//! Admin management endpoints.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    error::AppError,
    models::{AccountRecord, ApiKeyRecord, BrowserProfile, UsageEventRecord},
    service::{AccountImportItem, AccountUpdate, AppService},
};

/// Query parameters supported by the usage listing endpoint.
#[derive(Debug, Deserialize)]
pub struct UsageQuery {
    /// Optional maximum number of rows to return.
    pub limit: Option<u64>,
}

/// Request body for importing access tokens or session JSON blobs.
#[derive(Debug, Default, Deserialize)]
pub struct ImportAccountsRequest {
    /// Raw upstream access tokens.
    #[serde(default)]
    access_tokens: Vec<String>,
    /// Full ChatGPT session JSON blobs.
    #[serde(default)]
    session_jsons: Vec<String>,
}

/// Request body for deleting imported accounts.
#[derive(Debug, Default, Deserialize)]
pub struct DeleteAccountsRequest {
    /// Raw upstream access tokens.
    #[serde(default)]
    access_tokens: Vec<String>,
    /// Compatibility alias for access tokens.
    #[serde(default)]
    tokens: Vec<String>,
}

/// Request body for refreshing imported accounts.
#[derive(Debug, Default, Deserialize)]
pub struct RefreshAccountsRequest {
    /// Selected access tokens. Empty means refresh all accounts.
    #[serde(default)]
    access_tokens: Vec<String>,
}

/// Request body for updating one imported account.
#[derive(Debug, Default, Deserialize)]
pub struct UpdateAccountRequest {
    /// Raw upstream access token used as the account identifier for updates.
    access_token: String,
    /// Optional replacement plan type.
    plan_type: Option<String>,
    /// Optional replacement account status.
    status: Option<String>,
    /// Optional replacement local remaining quota.
    quota_remaining: Option<i64>,
    /// Optional replacement restore timestamp.
    restore_at: Option<String>,
    /// Optional replacement session cookie.
    session_token: Option<String>,
    /// Optional replacement user agent.
    user_agent: Option<String>,
    /// Optional replacement impersonated browser family.
    impersonate_browser: Option<String>,
    /// Optional replacement account-level concurrency cap.
    request_max_concurrency: Option<u64>,
    /// Optional replacement account-level minimum start interval.
    request_min_start_interval_ms: Option<u64>,
}

/// Returns a minimal operational snapshot for the admin control plane.
pub async fn status(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let accounts = service.storage().control.list_accounts().await.map_err(AppError::internal)?;
    let outbox_backlog = service
        .storage()
        .control
        .list_pending_outbox()
        .await
        .map_err(AppError::internal)?;
    let accounts_active = accounts.iter().filter(|record| record.status == "active").count();
    let accounts_limited = accounts.iter().filter(|record| record.status == "limited").count();
    let accounts_invalid = accounts.iter().filter(|record| record.status == "invalid").count();
    Ok(Json(json!({
        "accounts_total": accounts.len(),
        "accounts_active": accounts_active,
        "accounts_limited": accounts_limited,
        "accounts_invalid": accounts_invalid,
        "outbox_backlog": outbox_backlog.len(),
    })))
}

/// Lists imported accounts visible to the admin control plane.
pub async fn list_accounts(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Vec<AccountRecord>>, AppError> {
    require_admin(&headers, &service)?;
    let accounts = service.storage().control.list_accounts().await.map_err(AppError::internal)?;
    Ok(Json(accounts))
}

/// Imports access tokens or ChatGPT session JSONs.
pub async fn import_accounts(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<ImportAccountsRequest>,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let mut items = Vec::new();
    for token in body.access_tokens.into_iter().map(|value| value.trim().to_string()).filter(|value| !value.is_empty()) {
        items.push(AccountImportItem::AccessToken(token));
    }
    for session_json in body
        .session_jsons
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        items.push(AccountImportItem::SessionJson(session_json));
    }
    if items.is_empty() {
        return Err(AppError::bad_request("access_tokens or session_jsons is required"));
    }
    let imported = service.import_accounts(&items).await.map_err(AppError::internal)?;
    let access_tokens: Vec<String> = imported.iter().map(|account| account.access_token.clone()).collect();
    let refreshed = service.refresh_accounts(&access_tokens).await.map_err(AppError::internal)?;
    Ok(Json(json!({ "items": refreshed })))
}

/// Deletes accounts by raw access token.
pub async fn delete_accounts(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<DeleteAccountsRequest>,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let mut access_tokens = body.access_tokens;
    access_tokens.extend(body.tokens);
    access_tokens.retain(|value| !value.trim().is_empty());
    if access_tokens.is_empty() {
        return Err(AppError::bad_request("access_tokens is required"));
    }
    let items = service.delete_accounts(&access_tokens).await.map_err(AppError::internal)?;
    Ok(Json(json!({ "items": items })))
}

/// Refreshes all accounts or the selected access tokens.
pub async fn refresh_accounts(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<RefreshAccountsRequest>,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let items = service.refresh_accounts(&body.access_tokens).await.map_err(AppError::internal)?;
    Ok(Json(json!({ "items": items })))
}

/// Updates one imported account.
pub async fn update_account(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<UpdateAccountRequest>,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let access_token = body.access_token.trim();
    if access_token.is_empty() {
        return Err(AppError::bad_request("access_token is required"));
    }
    let browser_profile = if body.session_token.is_some() || body.user_agent.is_some() || body.impersonate_browser.is_some() {
        Some(BrowserProfile {
            session_token: body.session_token,
            user_agent: body.user_agent,
            impersonate_browser: body.impersonate_browser,
            oai_device_id: None,
            sec_ch_ua: None,
            sec_ch_ua_mobile: None,
            sec_ch_ua_platform: None,
        })
    } else {
        None
    };
    let update = AccountUpdate {
        plan_type: body.plan_type,
        status: body.status,
        quota_remaining: body.quota_remaining,
        restore_at: body.restore_at,
        browser_profile,
        request_max_concurrency: body.request_max_concurrency,
        request_min_start_interval_ms: body.request_min_start_interval_ms,
    };
    let Some(account) = service.update_account(access_token, &update).await.map_err(AppError::internal)? else {
        return Err(AppError::not_found("account not found"));
    };
    Ok(Json(json!({ "item": account })))
}

/// Lists configured downstream API keys.
pub async fn list_keys(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ApiKeyRecord>>, AppError> {
    require_admin(&headers, &service)?;
    let keys = service.storage().control.list_api_keys().await.map_err(AppError::internal)?;
    Ok(Json(keys))
}

/// Lists recent usage-event summaries.
pub async fn list_usage(
    Query(query): Query<UsageQuery>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Vec<UsageEventRecord>>, AppError> {
    require_admin(&headers, &service)?;
    let usage = service
        .storage()
        .events
        .list_recent_usage(query.limit.unwrap_or(50))
        .await
        .map_err(AppError::internal)?;
    Ok(Json(usage))
}

fn require_admin(headers: &HeaderMap, service: &Arc<AppService>) -> Result<(), AppError> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or_default();
    if service.is_admin_token(token) {
        Ok(())
    } else {
        Err(AppError::with_status(StatusCode::UNAUTHORIZED, "authorization is invalid"))
    }
}
