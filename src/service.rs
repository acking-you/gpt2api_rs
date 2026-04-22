//! Service orchestration layer.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};
use sha1::{Digest, Sha1};
use tokio::{sync::watch, task::JoinHandle};
use uuid::Uuid;

use crate::{
    accounts::import::{build_account_record, parse_access_token_seed, parse_session_seed},
    models::{AccountRecord, ApiKeyRecord, BrowserProfile, UsageEventRecord},
    routing::select_best_candidate,
    scheduler::{Lease, LocalRequestScheduler},
    storage::Storage,
    upstream::chatgpt::{is_token_invalid_error, ChatgptImageResult, ChatgptUpstreamClient},
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
    pub async fn authenticate_public_key(&self, bearer: &str) -> Result<ApiKeyRecord> {
        let secret_hash = sha1_hex(bearer.trim());
        let Some(key) = self.storage.control.find_api_key_by_secret_hash(&secret_hash).await?
        else {
            bail!("authorization is invalid");
        };
        if key.status != "active" {
            bail!("authorization is invalid");
        }
        Ok(key)
    }

    /// Validates `/auth/login` using the public API key path.
    pub async fn login(&self, bearer: &str) -> Result<()> {
        self.authenticate_public_key(bearer).await.map(|_| ())
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

    /// Executes one text-to-image request and returns the downstream image payload.
    pub async fn generate_images(
        &self,
        bearer: &str,
        prompt: &str,
        requested_model: &str,
        requested_n: usize,
    ) -> Result<ChatgptImageResult> {
        self.execute_public_image_request(
            bearer,
            prompt,
            requested_model,
            requested_n,
            None,
            "/v1/images/generations",
        )
        .await
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
        self.execute_public_image_request(
            bearer,
            prompt,
            requested_model,
            requested_n,
            Some(edit_input),
            "/v1/images/edits",
        )
        .await
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
            secret_hash: sha1_hex(&self.admin_token),
            status: "active".to_string(),
            quota_total_images: i64::MAX / 4,
            quota_used_images: 0,
            route_strategy: "auto".to_string(),
            account_group_id: None,
            request_max_concurrency: None,
            request_min_start_interval_ms: None,
        };
        self.storage.control.upsert_api_key(&key).await
    }

    async fn execute_public_image_request(
        &self,
        bearer: &str,
        prompt: &str,
        requested_model: &str,
        requested_n: usize,
        edit_input: Option<ImageEditInput>,
        endpoint: &str,
    ) -> Result<ChatgptImageResult> {
        let key = self.authenticate_public_key(bearer).await?;
        if key.quota_total_images > 0 && key.quota_used_images >= key.quota_total_images {
            bail!("quota exceeded");
        }
        let key_lease = self.acquire_key_lease(&key).await?;
        let _keep_key_lease = key_lease;

        let requested_n = requested_n.clamp(1, 4);
        let request_id = Uuid::new_v4().to_string();
        let started = Instant::now();
        let mut images = Vec::new();
        let mut resolved_model = String::new();
        let mut selected_account_name = String::new();

        for _ in 0..requested_n {
            let (account, lease) = self.acquire_account_for_key(&key).await?;
            let _keep_account_lease = lease;
            let account = self.refresh_account_if_needed(account).await?;
            if !is_account_routeable(&account) {
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
                    if is_token_invalid_error(&error.to_string()) {
                        continue;
                    }
                }
            }
        }

        if images.is_empty() {
            bail!("image generation failed");
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
        let stale = account
            .last_refresh_at
            .map(|last| unix_timestamp_secs() - last >= DEFAULT_REFRESH_SECONDS as i64)
            .unwrap_or(true);
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

fn sha1_hex(raw: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(raw.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn unix_timestamp_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after unix epoch")
        .as_secs() as i64
}
