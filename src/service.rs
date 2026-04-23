//! Service orchestration layer.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};
use uuid::Uuid;

use crate::{
    accounts::import::{build_account_record, parse_access_token_seed, parse_session_seed},
    models::{AccountRecord, ApiKeyRecord, BrowserProfile, UsageEventRecord},
    routing::select_best_candidate,
    scheduler::{Lease, LocalRequestScheduler},
    storage::Storage,
    upstream::chatgpt::{
        is_token_invalid_error, ChatgptImageResult, ChatgptTextResult, ChatgptTextStream,
        ChatgptUpstreamClient,
    },
};

const DEFAULT_KEY_ID: &str = "default";
const DEFAULT_REFRESH_SECONDS: u64 = 300;
const DEFAULT_OUTBOX_FLUSH_INTERVAL_SECONDS: u64 = 5;
const DEFAULT_OUTBOX_FLUSH_BATCH_SIZE: u64 = 100;

/// One operator-supplied account import item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountImportItem {
    /// Raw access token import.
    AccessToken(String),
    /// Session JSON copied from ChatGPT Web.
    SessionJson(String),
}

/// Mutable account fields allowed through the admin update surface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountUpdate {
    /// Replacement plan type.
    pub plan_type: Option<String>,
    /// Replacement status.
    pub status: Option<String>,
    /// Replacement local quota remaining.
    pub quota_remaining: Option<i64>,
    /// Replacement restore timestamp.
    pub restore_at: Option<String>,
    /// Replacement browser profile.
    pub browser_profile: Option<BrowserProfile>,
    /// Replacement account-level concurrency cap.
    pub request_max_concurrency: Option<u64>,
    /// Replacement account-level minimum start interval.
    pub request_min_start_interval_ms: Option<u64>,
}

/// Image edit input used by the public service layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageEditInput {
    /// Uploaded image bytes.
    pub image_data: Vec<u8>,
    /// Original upload file name.
    pub file_name: String,
    /// Upload MIME type.
    pub mime_type: String,
}

/// Public-key authentication failure classes exposed to downstream callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PublicAuthError {
    /// The supplied bearer token does not match any configured key.
    #[error("invalid_key")]
    InvalidKey,
    /// The key exists but is not active.
    #[error("disabled")]
    Disabled,
    /// The key has no remaining call quota.
    #[error("quota_exhausted")]
    QuotaExhausted,
}

/// Public-key authentication result that preserves internal storage failures.
#[derive(Debug, Error)]
pub enum PublicAuthFailure {
    /// Authentication failed due to key state.
    #[error(transparent)]
    Auth(#[from] PublicAuthError),
    /// Authentication failed due to internal storage/runtime errors.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Mutable API-key fields allowed through the admin create surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyCreate {
    /// Human-readable key label.
    pub name: String,
    /// Maximum billable call quota assigned to the key.
    pub quota_total_calls: i64,
    /// Optional replacement status. Defaults to `active`.
    pub status: Option<String>,
    /// Stored route strategy. Defaults to `auto`.
    pub route_strategy: String,
    /// Optional bound account-group id.
    pub account_group_id: Option<String>,
    /// Optional per-key concurrency cap.
    pub request_max_concurrency: Option<u64>,
    /// Optional per-key minimum start interval in milliseconds.
    pub request_min_start_interval_ms: Option<u64>,
}

/// Mutable API-key fields allowed through the admin update surface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApiKeyUpdate {
    /// Optional replacement key label.
    pub name: Option<String>,
    /// Optional replacement key status.
    pub status: Option<String>,
    /// Optional replacement total call quota.
    pub quota_total_calls: Option<i64>,
    /// Optional replacement route strategy.
    pub route_strategy: Option<String>,
    /// Optional replacement account-group id. `Some(None)` clears the field.
    pub account_group_id: Option<Option<String>>,
    /// Optional replacement concurrency cap. `Some(None)` clears the field.
    pub request_max_concurrency: Option<Option<u64>>,
    /// Optional replacement minimum start interval. `Some(None)` clears the field.
    pub request_min_start_interval_ms: Option<Option<u64>>,
}

/// One admin-visible API-key record plus the stored plaintext secret.
#[derive(Debug, Clone)]
pub struct ApiKeySecretRecord {
    /// Persisted API-key record after the write completed.
    pub key: ApiKeyRecord,
    /// Plaintext secret stored with the key and returned to the caller.
    pub secret_plaintext: String,
}

/// Shared runtime service backing public and admin HTTP handlers.
#[derive(Debug, Clone)]
pub struct AppService {
    storage: Storage,
    admin_token: String,
    upstream: ChatgptUpstreamClient,
    key_scheduler: Arc<LocalRequestScheduler>,
    account_scheduler: Arc<LocalRequestScheduler>,
}

impl AppService {
    /// Opens the runtime service and ensures there is a default public API key.
    pub async fn new(
        storage: Storage,
        admin_token: String,
        upstream: ChatgptUpstreamClient,
    ) -> Result<Self> {
        let service = Self {
            storage,
            admin_token,
            upstream,
            key_scheduler: Arc::new(LocalRequestScheduler::default()),
            account_scheduler: Arc::new(LocalRequestScheduler::default()),
        };
        service.ensure_default_api_key().await?;
        Ok(service)
    }

    /// Returns a clone of the underlying storage handles.
    #[must_use]
    pub fn storage(&self) -> Storage {
        self.storage.clone()
    }

    /// Returns whether the supplied bearer token matches the configured admin token.
    #[must_use]
    pub fn is_admin_token(&self, bearer: &str) -> bool {
        !bearer.trim().is_empty() && bearer.trim() == self.admin_token
    }

    /// Authenticates one downstream public API key from a bearer token.
    pub async fn authenticate_public_key(
        &self,
        bearer: &str,
    ) -> std::result::Result<ApiKeyRecord, PublicAuthFailure> {
        let bearer = bearer.trim();
        if bearer.is_empty() {
            return Err(PublicAuthError::InvalidKey.into());
        }
        let expected_hash = sha256_hex(bearer);
        let Some(mut key) = (if let Some(key) =
            self.storage.control.find_api_key_by_secret_plaintext(bearer).await?
        {
            Some(key)
        } else {
            self.storage.control.find_api_key_by_secret_hash(&expected_hash).await?
        }) else {
            return Err(PublicAuthError::InvalidKey.into());
        };
        if key.secret_plaintext.as_deref() != Some(bearer) || key.secret_hash != expected_hash {
            key.secret_plaintext = Some(bearer.to_string());
            key.secret_hash = expected_hash;
            self.storage.control.upsert_api_key(&key).await?;
        }
        if key.status != "active" {
            return Err(PublicAuthError::Disabled.into());
        }
        ensure_key_can_consume(&key, 1)?;
        Ok(key)
    }

    /// Validates `/auth/login` using the public API key path.
    pub async fn login(
        &self,
        bearer: &str,
    ) -> std::result::Result<ApiKeyRecord, PublicAuthFailure> {
        self.authenticate_public_key(bearer).await
    }

    /// Imports accounts and returns the refreshed account list.
    pub async fn import_accounts(&self, items: &[AccountImportItem]) -> Result<Vec<AccountRecord>> {
        for item in items {
            let seed = match item {
                AccountImportItem::AccessToken(raw) => parse_access_token_seed(raw)?,
                AccountImportItem::SessionJson(raw) => parse_session_seed(raw)?,
            };
            self.storage.control.upsert_account(&build_account_record(seed)).await?;
        }
        self.storage.control.list_accounts().await
    }

    /// Deletes accounts by raw access token and returns the updated account list.
    pub async fn delete_accounts(&self, access_tokens: &[String]) -> Result<Vec<AccountRecord>> {
        self.storage.control.delete_accounts_by_access_tokens(access_tokens).await?;
        self.storage.control.list_accounts().await
    }

    /// Refreshes all or some accounts and returns the updated account list.
    pub async fn refresh_accounts(&self, access_tokens: &[String]) -> Result<Vec<AccountRecord>> {
        let targets = if access_tokens.is_empty() {
            self.storage.control.list_accounts().await?
        } else {
            let all = self.storage.control.list_accounts().await?;
            let wanted: HashSet<_> =
                access_tokens.iter().map(|value| value.trim().to_string()).collect();
            all.into_iter().filter(|account| wanted.contains(account.access_token.trim())).collect()
        };
        for account in targets {
            let refreshed = self.refresh_account(account).await?;
            self.storage.control.upsert_account(&refreshed).await?;
        }
        self.storage.control.list_accounts().await
    }

    /// Updates one account by access token and returns the updated record.
    pub async fn update_account(
        &self,
        access_token: &str,
        update: &AccountUpdate,
    ) -> Result<Option<AccountRecord>> {
        let Some(mut account) =
            self.storage.control.find_account_by_access_token(access_token).await?
        else {
            return Ok(None);
        };
        if let Some(plan_type) = &update.plan_type {
            account.plan_type = Some(plan_type.clone());
        }
        if let Some(status) = &update.status {
            account.status = status.clone();
        }
        if let Some(quota_remaining) = update.quota_remaining {
            account.quota_remaining = quota_remaining;
            account.quota_known = true;
        }
        if let Some(restore_at) = &update.restore_at {
            account.restore_at = Some(restore_at.clone());
        }
        if let Some(browser_profile) = &update.browser_profile {
            account.browser_profile_json = serde_json::to_string(browser_profile)?;
        }
        if let Some(value) = update.request_max_concurrency {
            account.request_max_concurrency = Some(value);
        }
        if let Some(value) = update.request_min_start_interval_ms {
            account.request_min_start_interval_ms = Some(value);
        }
        self.storage.control.upsert_account(&account).await?;
        Ok(Some(account))
    }

    /// Creates one downstream API key and returns the stored plaintext secret.
    pub async fn create_api_key(&self, input: &ApiKeyCreate) -> Result<ApiKeySecretRecord> {
        let secret_plaintext = generate_api_key_plaintext();
        let key = ApiKeyRecord {
            id: format!("key_{}", Uuid::new_v4().simple()),
            name: normalize_required_string(&input.name, "name")?,
            secret_hash: sha256_hex(&secret_plaintext),
            secret_plaintext: Some(secret_plaintext.clone()),
            status: normalize_api_key_status(input.status.as_deref().unwrap_or("active"))?,
            quota_total_calls: validate_quota_total_calls(input.quota_total_calls)?,
            quota_used_calls: 0,
            route_strategy: normalize_route_strategy(&input.route_strategy)?,
            account_group_id: normalize_optional_string(input.account_group_id.as_deref()),
            request_max_concurrency: input.request_max_concurrency,
            request_min_start_interval_ms: input.request_min_start_interval_ms,
        };
        self.storage.control.upsert_api_key(&key).await?;
        Ok(ApiKeySecretRecord { key, secret_plaintext })
    }

    /// Updates one downstream API key and returns the stored record.
    pub async fn update_api_key(
        &self,
        key_id: &str,
        update: &ApiKeyUpdate,
    ) -> Result<Option<ApiKeyRecord>> {
        let Some(mut key) = self.storage.control.get_api_key(key_id).await? else {
            return Ok(None);
        };
        if let Some(name) = update.name.as_deref() {
            key.name = normalize_required_string(name, "name")?;
        }
        if let Some(status) = update.status.as_deref() {
            key.status = normalize_api_key_status(status)?;
        }
        if let Some(quota_total_calls) = update.quota_total_calls {
            key.quota_total_calls = validate_quota_total_calls(quota_total_calls)?;
        }
        if let Some(route_strategy) = update.route_strategy.as_deref() {
            key.route_strategy = normalize_route_strategy(route_strategy)?;
        }
        if let Some(account_group_id) = &update.account_group_id {
            key.account_group_id = normalize_optional_string(account_group_id.as_deref());
        }
        if let Some(request_max_concurrency) = update.request_max_concurrency {
            key.request_max_concurrency = request_max_concurrency;
        }
        if let Some(request_min_start_interval_ms) = update.request_min_start_interval_ms {
            key.request_min_start_interval_ms = request_min_start_interval_ms;
        }
        self.storage.control.upsert_api_key(&key).await?;
        Ok(Some(key))
    }

    /// Rotates one downstream API key and returns the stored plaintext secret.
    pub async fn rotate_api_key(&self, key_id: &str) -> Result<Option<ApiKeySecretRecord>> {
        let Some(mut key) = self.storage.control.get_api_key(key_id).await? else {
            return Ok(None);
        };
        let secret_plaintext = generate_api_key_plaintext();
        key.secret_hash = sha256_hex(&secret_plaintext);
        key.secret_plaintext = Some(secret_plaintext.clone());
        self.storage.control.upsert_api_key(&key).await?;
        Ok(Some(ApiKeySecretRecord { key, secret_plaintext }))
    }

    /// Deletes one downstream API key by stable id.
    pub async fn delete_api_key(&self, key_id: &str) -> Result<bool> {
        self.storage.control.delete_api_key(key_id).await
    }

    /// Executes one text-to-image request and returns the downstream image payload.
    pub async fn generate_images(
        &self,
        bearer: &str,
        prompt: &str,
        requested_model: &str,
        requested_n: usize,
    ) -> Result<ChatgptImageResult> {
        let key = self
            .authenticate_public_key(bearer)
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        self.generate_images_for_key(&key, prompt, requested_model, requested_n).await
    }

    /// Executes one image-edit request and returns the downstream image payload.
    pub async fn edit_images(
        &self,
        bearer: &str,
        prompt: &str,
        requested_model: &str,
        requested_n: usize,
        edit_input: ImageEditInput,
    ) -> Result<ChatgptImageResult> {
        let key = self
            .authenticate_public_key(bearer)
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        self.edit_images_for_key(&key, prompt, requested_model, requested_n, edit_input).await
    }

    /// Executes one text-to-image request for an already authenticated key.
    pub async fn generate_images_for_key(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        requested_n: usize,
    ) -> Result<ChatgptImageResult> {
        self.execute_public_image_request(
            key,
            prompt,
            requested_model,
            requested_n,
            None,
            "/v1/images/generations",
        )
        .await
    }

    /// Executes one image-edit request for an already authenticated key.
    pub async fn edit_images_for_key(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        requested_n: usize,
        edit_input: ImageEditInput,
    ) -> Result<ChatgptImageResult> {
        self.execute_public_image_request(
            key,
            prompt,
            requested_model,
            requested_n,
            Some(edit_input),
            "/v1/images/edits",
        )
        .await
    }

    /// Executes one non-streaming text completion for an already authenticated key.
    pub async fn complete_text_for_key(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        endpoint: &str,
    ) -> Result<ChatgptTextResult> {
        self.execute_public_text_request(key, prompt, requested_model, endpoint).await
    }

    /// Opens one streaming text completion for an already authenticated key.
    pub async fn start_text_stream_for_key(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        endpoint: &str,
    ) -> Result<ChatgptTextStream> {
        self.start_public_text_stream(key, prompt, requested_model, endpoint).await
    }

    /// Builds a chat.completions-compatible payload around the image gateway result.
    pub fn build_chat_completion_response(
        &self,
        requested_model: &str,
        result: &ChatgptImageResult,
    ) -> Value {
        let markdown_images: Vec<String> = result
            .data
            .iter()
            .enumerate()
            .map(|(index, item)| {
                format!("![image_{}](data:image/png;base64,{})", index + 1, item.b64_json)
            })
            .collect();
        json!({
            "id": format!("chatcmpl-{}", Uuid::new_v4().simple()),
            "object": "chat.completion",
            "created": result.created,
            "model": requested_model,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": if markdown_images.is_empty() {
                        "Image generation completed.".to_string()
                    } else {
                        markdown_images.join("\n\n")
                    },
                },
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        })
    }

    /// Builds a chat.completions-compatible payload around the text gateway result.
    pub fn build_text_chat_completion_response(
        &self,
        requested_model: &str,
        result: &ChatgptTextResult,
    ) -> Value {
        json!({
            "id": format!("chatcmpl-{}", Uuid::new_v4().simple()),
            "object": "chat.completion",
            "created": result.created,
            "model": response_model_name(requested_model, &result.resolved_model),
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": result.text,
                },
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        })
    }

    /// Builds a responses-compatible payload around the image gateway result.
    pub fn build_responses_api_response(
        &self,
        requested_model: &str,
        result: &ChatgptImageResult,
    ) -> Value {
        let output: Vec<Value> = result
            .data
            .iter()
            .enumerate()
            .map(|(index, item)| {
                json!({
                    "id": format!("ig_{}", index + 1),
                    "type": "image_generation_call",
                    "status": "completed",
                    "result": item.b64_json,
                    "revised_prompt": item.revised_prompt,
                })
            })
            .collect();
        json!({
            "id": format!("resp_{}", result.created),
            "object": "response",
            "created_at": result.created,
            "status": "completed",
            "error": Value::Null,
            "incomplete_details": Value::Null,
            "model": requested_model,
            "output": output,
            "parallel_tool_calls": false,
        })
    }

    /// Builds a responses-compatible payload around the text gateway result.
    pub fn build_text_responses_api_response(
        &self,
        requested_model: &str,
        result: &ChatgptTextResult,
    ) -> Value {
        json!({
            "id": format!("resp_{}", result.created),
            "object": "response",
            "created_at": result.created,
            "status": "completed",
            "error": Value::Null,
            "incomplete_details": Value::Null,
            "model": response_model_name(requested_model, &result.resolved_model),
            "output": [{
                "id": format!("msg_{}", Uuid::new_v4().simple()),
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": result.text,
                    "annotations": [],
                }],
            }],
            "output_text": result.text,
            "parallel_tool_calls": false,
        })
    }

    /// Spawns the limited-account refresher worker.
    pub fn spawn_limited_account_refresher(
        self: Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(DEFAULT_REFRESH_SECONDS)) => {
                        if let Ok(accounts) = self.storage.control.list_accounts().await {
                            for account in accounts.into_iter().filter(|account| account.status == "limited") {
                                if let Ok(refreshed) = self.refresh_account(account).await {
                                    let _ = self.storage.control.upsert_account(&refreshed).await;
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    /// Spawns the SQLite outbox to DuckDB flusher worker.
    pub fn spawn_outbox_flusher(
        self: Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(DEFAULT_OUTBOX_FLUSH_INTERVAL_SECONDS)) => {
                        let Ok(rows) = self.storage.control.list_pending_outbox_rows(DEFAULT_OUTBOX_FLUSH_BATCH_SIZE).await else {
                            continue;
                        };
                        if rows.is_empty() {
                            continue;
                        }
                        let events: Vec<_> = rows.iter().map(|row| row.payload.clone()).collect();
                        if self.storage.events.insert_usage_events(&events).await.is_ok() {
                            let ids: Vec<_> = rows.into_iter().map(|row| row.id).collect();
                            let _ = self.storage.control.mark_outbox_flushed(&ids, unix_timestamp_secs()).await;
                        }
                    }
                }
            }
        })
    }

    async fn ensure_default_api_key(&self) -> Result<()> {
        let key = ApiKeyRecord {
            id: DEFAULT_KEY_ID.to_string(),
            name: "default".to_string(),
            secret_hash: sha256_hex(&self.admin_token),
            secret_plaintext: Some(self.admin_token.clone()),
            status: "active".to_string(),
            quota_total_calls: i64::MAX / 4,
            quota_used_calls: 0,
            route_strategy: "auto".to_string(),
            account_group_id: None,
            request_max_concurrency: None,
            request_min_start_interval_ms: None,
        };
        self.storage.control.upsert_api_key(&key).await
    }

    async fn execute_public_image_request(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        requested_n: usize,
        edit_input: Option<ImageEditInput>,
        endpoint: &str,
    ) -> Result<ChatgptImageResult> {
        ensure_key_can_consume(key, requested_n as i64)
            .map_err(|error| anyhow!(error.to_string()))?;
        let key_lease = self.acquire_key_lease(key).await?;
        let _keep_key_lease = key_lease;

        let requested_n = requested_n.clamp(1, 4);
        let request_id = Uuid::new_v4().to_string();
        let started = Instant::now();
        let mut images = Vec::new();
        let mut resolved_model = String::new();
        let mut selected_account_name = String::new();
        let mut last_error = None;

        for _ in 0..requested_n {
            let (account, lease) = self.acquire_account_for_key(key).await?;
            let _keep_account_lease = lease;
            let account = self.refresh_account_if_needed(account).await?;
            if !is_account_routeable(&account) {
                last_error = Some("no available accounts".to_string());
                continue;
            }
            selected_account_name = account.name.clone();
            let result = match &edit_input {
                Some(edit_input) => {
                    self.upstream
                        .edit_image(
                            &account,
                            prompt,
                            requested_model,
                            &edit_input.image_data,
                            &edit_input.file_name,
                            &edit_input.mime_type,
                        )
                        .await
                }
                None => self.upstream.generate_image(&account, prompt, requested_model).await,
            };
            match result {
                Ok(result) => {
                    resolved_model = result.resolved_model.clone();
                    images.extend(result.data);
                    self.record_account_success(&account).await?;
                }
                Err(error) => {
                    self.record_account_failure(&account, &error.to_string()).await?;
                    last_error = Some(error.to_string());
                    if is_token_invalid_error(&error.to_string()) {
                        continue;
                    }
                }
            }
        }

        if images.is_empty() {
            bail!(last_error.unwrap_or_else(|| "image generation failed".to_string()));
        }

        let result = ChatgptImageResult {
            created: unix_timestamp_secs(),
            data: images,
            resolved_model: if resolved_model.is_empty() {
                requested_model.to_string()
            } else {
                resolved_model
            },
        };
        let event = UsageEventRecord {
            event_id: Uuid::new_v4().to_string(),
            request_id,
            key_id: key.id.clone(),
            key_name: key.name.clone(),
            account_name: selected_account_name,
            endpoint: endpoint.to_string(),
            requested_model: requested_model.to_string(),
            resolved_upstream_model: result.resolved_model.clone(),
            requested_n: requested_n as i64,
            generated_n: result.data.len() as i64,
            billable_images: result.data.len() as i64,
            status_code: 200,
            latency_ms: started.elapsed().as_millis() as i64,
            error_code: None,
            error_message: None,
            detail_ref: None,
            created_at: unix_timestamp_secs(),
        };
        self.storage.control.apply_success_settlement(&event).await?;
        Ok(result)
    }

    async fn execute_public_text_request(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        endpoint: &str,
    ) -> Result<ChatgptTextResult> {
        ensure_key_can_consume(key, 1).map_err(|error| anyhow!(error.to_string()))?;
        let key_lease = self.acquire_key_lease(key).await?;
        let _keep_key_lease = key_lease;

        let request_id = Uuid::new_v4().to_string();
        let started = Instant::now();
        let max_attempts = self.storage.control.list_accounts().await?.len().max(1);
        let mut last_error = None;

        for _ in 0..max_attempts {
            let (account, lease) = self.acquire_account_for_key(key).await?;
            let _keep_account_lease = lease;
            let account = self.refresh_account_if_needed(account).await?;
            if !is_account_routeable(&account) {
                last_error = Some("no available accounts".to_string());
                continue;
            }
            match self.upstream.complete_text(&account, prompt, requested_model).await {
                Ok(result) => {
                    self.record_account_success(&account).await?;
                    let event = UsageEventRecord {
                        event_id: Uuid::new_v4().to_string(),
                        request_id: request_id.clone(),
                        key_id: key.id.clone(),
                        key_name: key.name.clone(),
                        account_name: account.name.clone(),
                        endpoint: endpoint.to_string(),
                        requested_model: requested_model.to_string(),
                        resolved_upstream_model: result.resolved_model.clone(),
                        requested_n: 1,
                        generated_n: 1,
                        billable_images: 1,
                        status_code: 200,
                        latency_ms: started.elapsed().as_millis() as i64,
                        error_code: None,
                        error_message: None,
                        detail_ref: None,
                        created_at: unix_timestamp_secs(),
                    };
                    self.storage.control.apply_success_settlement(&event).await?;
                    return Ok(result);
                }
                Err(error) => {
                    self.record_account_failure(&account, &error.to_string()).await?;
                    last_error = Some(error.to_string());
                    if is_token_invalid_error(&error.to_string()) {
                        continue;
                    }
                    break;
                }
            }
        }

        bail!(last_error.unwrap_or_else(|| "chat completion failed".to_string()))
    }

    async fn start_public_text_stream(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        endpoint: &str,
    ) -> Result<ChatgptTextStream> {
        ensure_key_can_consume(key, 1).map_err(|error| anyhow!(error.to_string()))?;
        let key_lease = self.acquire_key_lease(key).await?;
        let _keep_key_lease = key_lease;

        let request_id = Uuid::new_v4().to_string();
        let started = Instant::now();
        let max_attempts = self.storage.control.list_accounts().await?.len().max(1);
        let mut last_error = None;

        for _ in 0..max_attempts {
            let (account, lease) = self.acquire_account_for_key(key).await?;
            let _keep_account_lease = lease;
            let account = self.refresh_account_if_needed(account).await?;
            if !is_account_routeable(&account) {
                last_error = Some("no available accounts".to_string());
                continue;
            }
            match self.upstream.start_text_stream(&account, prompt, requested_model).await {
                Ok(stream) => {
                    self.record_account_success(&account).await?;
                    let event = UsageEventRecord {
                        event_id: Uuid::new_v4().to_string(),
                        request_id,
                        key_id: key.id.clone(),
                        key_name: key.name.clone(),
                        account_name: account.name.clone(),
                        endpoint: endpoint.to_string(),
                        requested_model: requested_model.to_string(),
                        resolved_upstream_model: stream.resolved_model.clone(),
                        requested_n: 1,
                        generated_n: 1,
                        billable_images: 1,
                        status_code: 200,
                        latency_ms: started.elapsed().as_millis() as i64,
                        error_code: None,
                        error_message: None,
                        detail_ref: None,
                        created_at: unix_timestamp_secs(),
                    };
                    self.storage.control.apply_success_settlement(&event).await?;
                    return Ok(stream);
                }
                Err(error) => {
                    self.record_account_failure(&account, &error.to_string()).await?;
                    last_error = Some(error.to_string());
                    if is_token_invalid_error(&error.to_string()) {
                        continue;
                    }
                    break;
                }
            }
        }

        bail!(last_error.unwrap_or_else(|| "chat completion failed".to_string()))
    }

    async fn acquire_key_lease(&self, key: &ApiKeyRecord) -> Result<Lease> {
        loop {
            match self.key_scheduler.try_acquire(
                &format!("api-key:{}", key.id),
                key.request_max_concurrency,
                key.request_min_start_interval_ms,
                Instant::now(),
            ) {
                Ok(lease) => return Ok(lease),
                Err(rejection) => self.key_scheduler.wait_for_available(rejection.wait).await,
            }
        }
    }

    async fn acquire_account_for_key(&self, key: &ApiKeyRecord) -> Result<(AccountRecord, Lease)> {
        loop {
            let accounts = self.storage.control.list_accounts().await?;
            let mut remaining: HashMap<String, AccountRecord> = accounts
                .into_iter()
                .filter(is_account_selectable)
                .map(|account| (account.name.clone(), account))
                .collect();
            if remaining.is_empty() {
                bail!("no available accounts");
            }

            let mut waits = Vec::new();
            while !remaining.is_empty() {
                let candidates: Vec<_> = remaining
                    .values()
                    .map(|account| crate::models::AccountRouteCandidate {
                        name: account.name.clone(),
                        quota_remaining: account.quota_remaining,
                        quota_known: account.quota_known,
                        last_routed_at_ms: account.last_used_at.unwrap_or_default(),
                    })
                    .collect();
                let strategy = if key.route_strategy.eq_ignore_ascii_case("fixed") {
                    crate::models::RouteStrategy::Fixed
                } else {
                    crate::models::RouteStrategy::Auto
                };
                let Some(selected) = select_best_candidate(strategy, &candidates) else {
                    bail!("no available accounts");
                };
                let account = remaining
                    .remove(&selected.name)
                    .ok_or_else(|| anyhow!("selected account disappeared"))?;
                let lease = self.try_acquire_account_lease(&account);
                match lease {
                    Ok(lease) => return Ok((account, lease)),
                    Err(wait) => waits.push(wait),
                }
            }

            let wait = waits.into_iter().flatten().min();
            self.account_scheduler.wait_for_available(wait).await;
        }
    }

    fn try_acquire_account_lease(
        &self,
        account: &AccountRecord,
    ) -> Result<Lease, Option<Duration>> {
        match self.account_scheduler.try_acquire(
            &format!("account:{}", account.name),
            account.request_max_concurrency,
            account.request_min_start_interval_ms,
            Instant::now(),
        ) {
            Ok(lease) => Ok(lease),
            Err(rejection) => Err(rejection.wait),
        }
    }

    async fn refresh_account_if_needed(&self, account: AccountRecord) -> Result<AccountRecord> {
        let Some(last_refresh_at) = account.last_refresh_at else {
            return Ok(account);
        };
        let stale =
            unix_timestamp_secs().saturating_sub(last_refresh_at) >= DEFAULT_REFRESH_SECONDS as i64;
        if !stale {
            return Ok(account);
        }
        let refreshed = self.refresh_account(account).await?;
        self.storage.control.upsert_account(&refreshed).await?;
        Ok(refreshed)
    }

    async fn refresh_account(&self, mut account: AccountRecord) -> Result<AccountRecord> {
        match self.upstream.fetch_account_metadata(&account).await {
            Ok(metadata) => {
                account.email = metadata.email;
                account.user_id = metadata.user_id;
                account.plan_type = metadata.plan_type;
                account.default_model_slug = metadata.default_model_slug;
                account.quota_remaining = metadata.quota_remaining;
                account.quota_known = metadata.quota_known;
                account.restore_at = metadata.restore_at;
                account.status = if account.quota_known && account.quota_remaining <= 0 {
                    "limited".to_string()
                } else {
                    "active".to_string()
                };
                account.last_refresh_at = Some(unix_timestamp_secs());
                account.last_error = None;
                Ok(account)
            }
            Err(error) => {
                account.last_refresh_at = Some(unix_timestamp_secs());
                account.last_error = Some(error.to_string());
                if is_token_invalid_error(&error.to_string()) {
                    account.status = "invalid".to_string();
                    account.quota_remaining = 0;
                    account.quota_known = true;
                }
                Ok(account)
            }
        }
    }

    async fn record_account_success(&self, account: &AccountRecord) -> Result<()> {
        let mut next = account.clone();
        next.success_count += 1;
        next.last_used_at = Some(unix_timestamp_secs());
        next.last_error = None;
        if next.quota_known {
            next.quota_remaining = (next.quota_remaining - 1).max(0);
            if next.quota_remaining == 0 {
                next.status = "limited".to_string();
            } else {
                next.status = "active".to_string();
            }
        } else {
            next.status = "active".to_string();
        }
        self.storage.control.upsert_account(&next).await
    }

    async fn record_account_failure(
        &self,
        account: &AccountRecord,
        error_message: &str,
    ) -> Result<()> {
        let mut next = account.clone();
        next.fail_count += 1;
        next.last_used_at = Some(unix_timestamp_secs());
        next.last_error = Some(error_message.to_string());
        if is_token_invalid_error(error_message) {
            next.status = "invalid".to_string();
            next.quota_remaining = 0;
            next.quota_known = true;
        }
        self.storage.control.upsert_account(&next).await
    }
}

fn is_account_selectable(account: &AccountRecord) -> bool {
    account.status != "invalid" && account.status != "disabled"
}

fn is_account_routeable(account: &AccountRecord) -> bool {
    account.status == "active"
}

fn ensure_key_can_consume(
    key: &ApiKeyRecord,
    cost: i64,
) -> std::result::Result<(), PublicAuthError> {
    if key.status != "active" {
        return Err(PublicAuthError::Disabled);
    }
    if cost <= 0 {
        return Ok(());
    }
    if key.quota_total_calls <= 0
        || key.quota_used_calls.saturating_add(cost) > key.quota_total_calls
    {
        return Err(PublicAuthError::QuotaExhausted);
    }
    Ok(())
}

fn normalize_api_key_status(raw: &str) -> Result<String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "active" => Ok("active".to_string()),
        "disabled" => Ok("disabled".to_string()),
        _ => bail!("status must be active or disabled"),
    }
}

fn normalize_route_strategy(raw: &str) -> Result<String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "auto" => Ok("auto".to_string()),
        "fixed" => Ok("fixed".to_string()),
        _ => bail!("route_strategy must be auto or fixed"),
    }
}

fn normalize_required_string(raw: &str, field_name: &str) -> Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        bail!("{field_name} is required");
    }
    Ok(value.to_string())
}

fn normalize_optional_string(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim).filter(|value| !value.is_empty()).map(ToString::to_string)
}

fn validate_quota_total_calls(value: i64) -> Result<i64> {
    if value < 0 {
        bail!("quota_total_calls must be >= 0");
    }
    Ok(value)
}

fn generate_api_key_plaintext() -> String {
    format!("sk-{}", Uuid::new_v4().simple())
}

fn response_model_name(requested_model: &str, resolved_model: &str) -> String {
    let requested_model = requested_model.trim();
    if requested_model.is_empty() {
        resolved_model.to_string()
    } else {
        requested_model.to_string()
    }
}

fn sha256_hex(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn unix_timestamp_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after unix epoch")
        .as_secs() as i64
}
