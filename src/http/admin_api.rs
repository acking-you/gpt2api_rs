//! Admin management endpoints.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    error::AppError,
    models::{ApiKeyRecord, BrowserProfile, ProxyConfigRecord, UsageEventRecord},
    service::{
        AccountImportItem, AccountUpdate, AdminAccountView, ApiKeyCreate, ApiKeySecretRecord,
        ApiKeyUpdate, AppService, ProxyConfigCreate, ProxyConfigUpdate,
    },
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
    /// Optional replacement proxy mode.
    #[serde(default)]
    proxy_mode: Option<String>,
    /// Optional replacement bound proxy config id. `null` clears the field.
    #[serde(default)]
    proxy_config_id: Option<Option<String>>,
}

/// Request body for creating one reusable proxy config.
#[derive(Debug, Deserialize)]
pub struct CreateProxyConfigRequest {
    /// Human-readable proxy-config label.
    name: String,
    /// Proxy URL including scheme and host.
    proxy_url: String,
    /// Optional proxy username.
    #[serde(default)]
    proxy_username: Option<String>,
    /// Optional proxy password.
    #[serde(default)]
    proxy_password: Option<String>,
    /// Optional replacement status. Defaults to `active`.
    #[serde(default)]
    status: Option<String>,
}

/// Request body for updating one reusable proxy config.
#[derive(Debug, Default, Deserialize)]
pub struct UpdateProxyConfigRequest {
    /// Optional replacement label.
    #[serde(default)]
    name: Option<String>,
    /// Optional replacement proxy URL.
    #[serde(default)]
    proxy_url: Option<String>,
    /// Optional replacement proxy username. `null` clears the field.
    #[serde(default)]
    proxy_username: Option<Option<String>>,
    /// Optional replacement proxy password. `null` clears the field.
    #[serde(default)]
    proxy_password: Option<Option<String>>,
    /// Optional replacement status.
    #[serde(default)]
    status: Option<String>,
}

/// Request body for creating one downstream API key.
#[derive(Debug, Deserialize)]
pub struct CreateKeyRequest {
    /// Human-readable key label.
    name: String,
    /// Maximum billable call quota assigned to the key.
    quota_total_calls: i64,
    /// Optional replacement status. Defaults to `active`.
    #[serde(default)]
    status: Option<String>,
    /// Stored route strategy. Defaults to `auto`.
    #[serde(default = "default_route_strategy")]
    route_strategy: String,
    /// Optional bound account-group id.
    #[serde(default)]
    account_group_id: Option<String>,
    /// Optional per-key concurrency cap.
    #[serde(default)]
    request_max_concurrency: Option<u64>,
    /// Optional per-key minimum start interval in milliseconds.
    #[serde(default)]
    request_min_start_interval_ms: Option<u64>,
}

/// Request body for updating one downstream API key.
#[derive(Debug, Default, Deserialize)]
pub struct UpdateKeyRequest {
    /// Optional replacement key label.
    #[serde(default)]
    name: Option<String>,
    /// Optional replacement key status.
    #[serde(default)]
    status: Option<String>,
    /// Optional replacement total call quota.
    #[serde(default)]
    quota_total_calls: Option<i64>,
    /// Optional replacement route strategy.
    #[serde(default)]
    route_strategy: Option<String>,
    /// Optional replacement account-group id. `null` clears the field.
    #[serde(default)]
    account_group_id: Option<Option<String>>,
    /// Optional replacement concurrency cap. `null` clears the field.
    #[serde(default)]
    request_max_concurrency: Option<Option<u64>>,
    /// Optional replacement minimum start interval. `null` clears the field.
    #[serde(default)]
    request_min_start_interval_ms: Option<Option<u64>>,
}

/// Returns a minimal operational snapshot for the admin control plane.
pub async fn status(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let accounts = service.storage().control.list_accounts().await.map_err(AppError::internal)?;
    let outbox_backlog =
        service.storage().control.list_pending_outbox().await.map_err(AppError::internal)?;
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
) -> Result<Json<Vec<AdminAccountView>>, AppError> {
    require_admin(&headers, &service)?;
    let accounts = service.list_admin_accounts().await.map_err(AppError::internal)?;
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
    for token in body
        .access_tokens
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
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
    let access_tokens: Vec<String> =
        imported.iter().map(|account| account.access_token.clone()).collect();
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
    let browser_profile = if body.session_token.is_some()
        || body.user_agent.is_some()
        || body.impersonate_browser.is_some()
    {
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
        proxy_mode: body.proxy_mode,
        proxy_config_id: body.proxy_config_id,
    };
    let Some(account) =
        service.update_account(access_token, &update).await.map_err(AppError::internal)?
    else {
        return Err(AppError::not_found("account not found"));
    };
    let item = service
        .list_admin_accounts()
        .await
        .map_err(AppError::internal)?
        .into_iter()
        .find(|item| item.account.name == account.name)
        .ok_or_else(|| AppError::internal("updated account disappeared"))?;
    Ok(Json(json!({ "item": item })))
}

/// Lists reusable proxy configs.
pub async fn list_proxy_configs(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ProxyConfigRecord>>, AppError> {
    require_admin(&headers, &service)?;
    Ok(Json(service.list_proxy_configs().await.map_err(AppError::internal)?))
}

/// Creates one reusable proxy config.
pub async fn create_proxy_config(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<CreateProxyConfigRequest>,
) -> Result<Json<ProxyConfigRecord>, AppError> {
    require_admin(&headers, &service)?;
    let record = service
        .create_proxy_config(&ProxyConfigCreate {
            name: body.name,
            proxy_url: body.proxy_url,
            proxy_username: body.proxy_username,
            proxy_password: body.proxy_password,
            status: body.status,
        })
        .await
        .map_err(AppError::internal)?;
    Ok(Json(record))
}

/// Updates one reusable proxy config.
pub async fn update_proxy_config(
    Path(proxy_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<UpdateProxyConfigRequest>,
) -> Result<Json<ProxyConfigRecord>, AppError> {
    require_admin(&headers, &service)?;
    let Some(record) = service
        .update_proxy_config(
            &proxy_id,
            &ProxyConfigUpdate {
                name: body.name,
                proxy_url: body.proxy_url,
                proxy_username: body.proxy_username,
                proxy_password: body.proxy_password,
                status: body.status,
            },
        )
        .await
        .map_err(AppError::internal)?
    else {
        return Err(AppError::not_found("proxy config not found"));
    };
    Ok(Json(record))
}

/// Deletes one reusable proxy config.
pub async fn delete_proxy_config(
    Path(proxy_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let deleted = service.delete_proxy_config(&proxy_id).await.map_err(|error| {
        let message = error.to_string();
        if message.contains("still bound") {
            AppError::conflict(message)
        } else {
            AppError::internal(error)
        }
    })?;
    Ok(Json(json!({ "deleted": deleted, "id": proxy_id })))
}

/// Checks one reusable proxy config against the configured upstream.
pub async fn check_proxy_config(
    Path(proxy_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let result = service.check_proxy_config(&proxy_id).await.map_err(AppError::internal)?;
    Ok(Json(json!({
        "ok": result.ok,
        "message": result.message,
        "status_code": result.status_code,
    })))
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

/// Creates one downstream API key and returns the stored plaintext secret.
pub async fn create_key(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<CreateKeyRequest>,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let created = service
        .create_api_key(&ApiKeyCreate {
            name: body.name,
            quota_total_calls: body.quota_total_calls,
            status: body.status,
            route_strategy: body.route_strategy,
            account_group_id: body.account_group_id,
            request_max_concurrency: body.request_max_concurrency,
            request_min_start_interval_ms: body.request_min_start_interval_ms,
        })
        .await
        .map_err(AppError::internal)?;
    Ok(Json(serialize_key_record(&created, true)))
}

/// Updates one downstream API key.
pub async fn update_key(
    Path(key_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<UpdateKeyRequest>,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let Some(key) = service
        .update_api_key(
            &key_id,
            &ApiKeyUpdate {
                name: body.name,
                status: body.status,
                quota_total_calls: body.quota_total_calls,
                route_strategy: body.route_strategy,
                account_group_id: body.account_group_id,
                request_max_concurrency: body.request_max_concurrency,
                request_min_start_interval_ms: body.request_min_start_interval_ms,
            },
        )
        .await
        .map_err(AppError::internal)?
    else {
        return Err(AppError::not_found("key not found"));
    };
    Ok(Json(serialize_key(&key, None)))
}

/// Rotates one downstream API key and returns the stored plaintext secret.
pub async fn rotate_key(
    Path(key_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    let Some(rotated) = service.rotate_api_key(&key_id).await.map_err(AppError::internal)? else {
        return Err(AppError::not_found("key not found"));
    };
    Ok(Json(serialize_key_record(&rotated, true)))
}

/// Deletes one downstream API key.
pub async fn delete_key(
    Path(key_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_admin(&headers, &service)?;
    if !service.delete_api_key(&key_id).await.map_err(AppError::internal)? {
        return Err(AppError::not_found("key not found"));
    }
    Ok(Json(json!({ "ok": true })))
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

fn default_route_strategy() -> String {
    "auto".to_string()
}

fn serialize_key_record(record: &ApiKeySecretRecord, include_secret: bool) -> Value {
    serialize_key(&record.key, include_secret.then_some(record.secret_plaintext.as_str()))
}

fn serialize_key(key: &ApiKeyRecord, secret_plaintext: Option<&str>) -> Value {
    let secret_plaintext = secret_plaintext.or(key.secret_plaintext.as_deref());
    json!({
        "id": key.id,
        "name": key.name,
        "secret_hash": key.secret_hash,
        "status": key.status,
        "quota_total_calls": key.quota_total_calls,
        "quota_used_calls": key.quota_used_calls,
        "route_strategy": key.route_strategy,
        "account_group_id": key.account_group_id,
        "request_max_concurrency": key.request_max_concurrency,
        "request_min_start_interval_ms": key.request_min_start_interval_ms,
        "secret_plaintext": secret_plaintext,
    })
}
