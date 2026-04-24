//! Service orchestration layer.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};
use uuid::Uuid;

use crate::{
    accounts::import::{build_account_record, parse_access_token_seed, parse_session_seed},
    config::SmtpConfig,
    models::{
        AccountMetadata, AccountRecord, ApiKeyRecord, ApiKeyRole, BrowserProfile,
        ImageSubmissionResult, ImageTaskRecord, MessageRecord, MessageStatus, ProxyConfigRecord,
        SessionDetail, SessionRecord, SessionSource, UsageEventRecord,
    },
    notifications::{
        is_valid_notification_email, render_image_done_email, send_rendered_email,
        NotificationOutcome,
    },
    routing::select_best_candidate,
    scheduler::{Lease, LocalRequestScheduler},
    storage::{control::CreateImageTaskInput, Storage},
    upstream::chatgpt::{
        access_token_expires_at, is_token_invalid_error, ChatgptImageResult, ChatgptTextResult,
        ChatgptTextStream, ChatgptUpstreamClient, GeneratedImageItem,
    },
};

const DEFAULT_KEY_ID: &str = "default";
const DEFAULT_REFRESH_SECONDS: u64 = 300;
const ACCESS_TOKEN_REFRESH_LEEWAY_SECONDS: i64 = 300;
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
    /// Optional replacement account-level proxy mode.
    pub proxy_mode: Option<String>,
    /// Optional replacement bound proxy config id. `Some(None)` clears the field.
    pub proxy_config_id: Option<Option<String>>,
}

/// Effective upstream proxy settings resolved for one account request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedAccountProxy {
    /// Source label used for admin visibility and debugging.
    pub source: &'static str,
    /// Effective proxy URL, or `None` for direct connections.
    pub proxy_url: Option<String>,
    /// Optional proxy username.
    pub proxy_username: Option<String>,
    /// Optional proxy password.
    pub proxy_password: Option<String>,
    /// Optional bound proxy config id.
    pub proxy_config_id: Option<String>,
    /// Optional bound proxy config name.
    pub proxy_config_name: Option<String>,
}

/// One admin-visible account row including effective proxy resolution.
#[derive(Debug, Clone, Serialize)]
pub struct AdminAccountView {
    /// Flattened persisted account state.
    #[serde(flatten)]
    pub account: AccountRecord,
    /// Effective proxy source label.
    pub effective_proxy_source: String,
    /// Effective proxy URL, if one is used.
    pub effective_proxy_url: Option<String>,
    /// Effective proxy config name, when bound.
    pub effective_proxy_config_name: Option<String>,
}

/// Mutable proxy-config fields allowed through the admin create surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyConfigCreate {
    /// Human-readable proxy config label.
    pub name: String,
    /// Proxy URL including scheme and host.
    pub proxy_url: String,
    /// Optional proxy username.
    pub proxy_username: Option<String>,
    /// Optional proxy password.
    pub proxy_password: Option<String>,
    /// Optional replacement status. Defaults to `active`.
    pub status: Option<String>,
}

/// Mutable proxy-config fields allowed through the admin update surface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProxyConfigUpdate {
    /// Optional replacement proxy-config label.
    pub name: Option<String>,
    /// Optional replacement proxy URL.
    pub proxy_url: Option<String>,
    /// Optional replacement proxy username. `Some(None)` clears the field.
    pub proxy_username: Option<Option<String>>,
    /// Optional replacement proxy password. `Some(None)` clears the field.
    pub proxy_password: Option<Option<String>>,
    /// Optional replacement status.
    pub status: Option<String>,
}

/// Result of probing one stored proxy config against the upstream base URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyConfigCheckResult {
    /// Whether the probe reached the upstream successfully.
    pub ok: bool,
    /// Human-readable status summary.
    pub message: String,
    /// Optional observed HTTP status code.
    pub status_code: Option<u16>,
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
    /// Optional product role. Defaults to user.
    pub role: Option<ApiKeyRole>,
    /// Optional default notification email.
    pub notification_email: Option<String>,
    /// Optional notification toggle.
    pub notification_enabled: Option<bool>,
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
    /// Optional replacement product role.
    pub role: Option<ApiKeyRole>,
    /// Optional replacement notification email. `Some(None)` clears the field.
    pub notification_email: Option<Option<String>>,
    /// Optional replacement notification toggle.
    pub notification_enabled: Option<bool>,
}

/// One admin-visible API-key record plus the stored plaintext secret.
#[derive(Debug, Clone)]
pub struct ApiKeySecretRecord {
    /// Persisted API-key record after the write completed.
    pub key: ApiKeyRecord,
    /// Plaintext secret stored with the key and returned to the caller.
    pub secret_plaintext: String,
}

struct ImageMessageInput<'a> {
    key: &'a ApiKeyRecord,
    session_id: &'a str,
    mode: &'a str,
    prompt: &'a str,
    model: &'a str,
    n: usize,
    edit_input: Option<ImageEditInput>,
}

/// Shared runtime service backing public and admin HTTP handlers.
#[derive(Debug, Clone)]
pub struct AppService {
    storage: Storage,
    admin_token: String,
    upstream: ChatgptUpstreamClient,
    smtp_config: SmtpConfig,
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
            smtp_config: SmtpConfig::from_env(),
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

    /// Authenticates one downstream product API key.
    pub async fn authenticate_product_key(
        &self,
        bearer: &str,
    ) -> std::result::Result<ApiKeyRecord, PublicAuthFailure> {
        self.authenticate_public_key(bearer).await
    }

    /// Returns whether a key has product-admin privileges.
    #[must_use]
    pub fn is_product_admin(&self, key: &ApiKeyRecord) -> bool {
        key.role == ApiKeyRole::Admin
    }

    /// Creates one web session for an authenticated key.
    pub async fn create_web_session(
        &self,
        key: &ApiKeyRecord,
        title: Option<&str>,
    ) -> Result<SessionRecord> {
        self.storage
            .control
            .create_session(key.id.as_str(), title.unwrap_or("New chat"), SessionSource::Web)
            .await
    }

    /// Resolves or creates the API-backed session used by compatible endpoints.
    pub async fn resolve_api_session(
        &self,
        key: &ApiKeyRecord,
        supplied_session_id: Option<&str>,
    ) -> Result<SessionRecord> {
        if let Some(session_id) =
            supplied_session_id.map(str::trim).filter(|value| !value.is_empty())
        {
            return self
                .storage
                .control
                .get_session_for_key(session_id, &key.id)
                .await?
                .ok_or_else(|| anyhow!("x-gpt2api-session-id does not belong to this key"));
        }

        let sessions = self.storage.control.list_sessions_for_key(&key.id, 50, None).await?;
        if let Some(session) = sessions
            .into_iter()
            .find(|session| session.source == SessionSource::Api && session.status == "active")
        {
            return Ok(session);
        }
        self.storage.control.create_session(&key.id, "API Requests", SessionSource::Api).await
    }

    /// Fetches a key-scoped session detail.
    pub async fn get_session_detail_for_key(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
    ) -> Result<Option<SessionDetail>> {
        let Some(session) = self.storage.control.get_session_for_key(session_id, &key.id).await?
        else {
            return Ok(None);
        };
        Ok(Some(self.build_session_detail(session).await?))
    }

    /// Fetches an admin-visible session detail.
    pub async fn get_session_detail_for_admin(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionDetail>> {
        let Some(session) = self.storage.control.get_session_for_admin(session_id).await? else {
            return Ok(None);
        };
        Ok(Some(self.build_session_detail(session).await?))
    }

    /// Updates and returns one key-scoped session detail.
    pub async fn patch_session_for_key(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
        title: Option<&str>,
        status: Option<&str>,
    ) -> Result<Option<SessionDetail>> {
        let Some(session) =
            self.storage.control.update_session_for_key(session_id, &key.id, title, status).await?
        else {
            return Ok(None);
        };
        Ok(Some(self.build_session_detail(session).await?))
    }

    /// Appends a user text message, completes it immediately, and returns the session detail.
    pub async fn submit_text_message(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
        text: &str,
        model: &str,
    ) -> Result<Option<SessionDetail>> {
        let Some(session) = self.storage.control.get_session_for_key(session_id, &key.id).await?
        else {
            return Ok(None);
        };
        let text = normalize_required_string(text, "text")?;
        let model = normalize_required_string(model, "model")?;
        let _user_message = self
            .storage
            .control
            .append_message(
                &session.id,
                &key.id,
                "user",
                text_message_content(&text),
                MessageStatus::Done,
            )
            .await?;
        let assistant_message = self
            .storage
            .control
            .append_message(
                &session.id,
                &key.id,
                "assistant",
                json!({ "blocks": [] }),
                MessageStatus::Pending,
            )
            .await?;
        match self.complete_text_for_key(key, &text, &model, "/sessions/messages").await {
            Ok(result) => {
                self.storage
                    .control
                    .update_message_content_status(
                        &assistant_message.id,
                        text_message_content(&result.text),
                        MessageStatus::Done,
                    )
                    .await?;
                self.get_session_detail_for_key(key, &session.id).await
            }
            Err(error) => {
                self.storage
                    .control
                    .update_message_content_status(
                        &assistant_message.id,
                        json!({ "blocks": [{ "type": "error", "text": error.to_string() }] }),
                        MessageStatus::Failed,
                    )
                    .await?;
                Err(error)
            }
        }
    }

    /// Queues one text-to-image message for a web session.
    pub async fn submit_image_generation_message(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
        prompt: &str,
        model: &str,
        n: usize,
    ) -> Result<Option<ImageSubmissionResult>> {
        self.submit_image_message(ImageMessageInput {
            key,
            session_id,
            mode: "generation",
            prompt,
            model,
            n,
            edit_input: None,
        })
        .await
    }

    /// Queues one image-edit message for a web session.
    pub async fn submit_image_edit_message(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
        prompt: &str,
        model: &str,
        n: usize,
        edit_input: ImageEditInput,
    ) -> Result<Option<ImageSubmissionResult>> {
        self.submit_image_message(ImageMessageInput {
            key,
            session_id,
            mode: "edit",
            prompt,
            model,
            n,
            edit_input: Some(edit_input),
        })
        .await
    }

    async fn submit_image_message(
        &self,
        input: ImageMessageInput<'_>,
    ) -> Result<Option<ImageSubmissionResult>> {
        let key = input.key;
        let Some(session) =
            self.storage.control.get_session_for_key(input.session_id, &key.id).await?
        else {
            return Ok(None);
        };
        let prompt = normalize_required_string(input.prompt, "prompt")?;
        let model = normalize_required_string(input.model, "model")?;
        let n = input.n.clamp(1, 4);
        ensure_key_can_consume(key, n as i64).map_err(|error| anyhow!(error.to_string()))?;
        let user_message = self
            .storage
            .control
            .append_message(
                &session.id,
                &key.id,
                "user",
                image_prompt_message_content(input.mode, &prompt, &model, n),
                MessageStatus::Done,
            )
            .await?;
        let assistant_message = self
            .storage
            .control
            .append_message(
                &session.id,
                &key.id,
                "assistant",
                json!({ "blocks": [] }),
                MessageStatus::Pending,
            )
            .await?;
        let request_json =
            image_task_request_json(input.mode, &prompt, &model, n, input.edit_input.as_ref());
        let task = self
            .storage
            .control
            .create_image_task(CreateImageTaskInput {
                session_id: &session.id,
                message_id: &assistant_message.id,
                key_id: &key.id,
                mode: input.mode,
                prompt: &prompt,
                model: &model,
                n: n as i64,
                request_json,
            })
            .await?;
        let queue = self.storage.control.queue_snapshot_for_task(&task.id).await?;
        Ok(Some(ImageSubmissionResult {
            user_message,
            assistant_message,
            task: queue.task.clone(),
            queue,
        }))
    }

    /// Appends one API user text message.
    pub async fn append_api_user_text_message(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
        prompt: &str,
        model: &str,
        endpoint: &str,
    ) -> Result<MessageRecord> {
        self.storage
            .control
            .append_message(
                session_id,
                &key.id,
                "user",
                json!({
                    "blocks": [{ "type": "text", "text": prompt }],
                    "source": "api",
                    "endpoint": endpoint,
                    "model": model,
                }),
                MessageStatus::Done,
            )
            .await
    }

    /// Appends one API assistant text message after a successful completion.
    pub async fn append_api_assistant_text_message(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
        user_message_id: &str,
        result: &ChatgptTextResult,
    ) -> Result<MessageRecord> {
        self.storage
            .control
            .append_message(
                session_id,
                &key.id,
                "assistant",
                json!({
                    "blocks": [{ "type": "text", "text": result.text }],
                    "parent_message_id": user_message_id,
                    "model": result.resolved_model,
                }),
                MessageStatus::Done,
            )
            .await
    }

    /// Appends one API assistant failure message.
    pub async fn append_api_failed_assistant_message(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
        user_message_id: &str,
        error_message: &str,
    ) -> Result<MessageRecord> {
        self.storage
            .control
            .append_message(
                session_id,
                &key.id,
                "assistant",
                json!({
                    "blocks": [{ "type": "error", "text": error_message }],
                    "parent_message_id": user_message_id,
                }),
                MessageStatus::Failed,
            )
            .await
    }

    /// Waits for an image task to finish, helping drain queued work when needed.
    pub async fn wait_for_image_task(
        &self,
        task_id: &str,
        timeout: Duration,
    ) -> Result<ImageTaskRecord> {
        let deadline = Instant::now() + timeout;
        loop {
            let Some(task) = self.storage.control.get_image_task(task_id).await? else {
                bail!("image task not found");
            };
            match task.status {
                crate::models::ImageTaskStatus::Succeeded => return Ok(task),
                crate::models::ImageTaskStatus::Failed => {
                    bail!(task.error_message.unwrap_or_else(|| "image task failed".to_string()));
                }
                crate::models::ImageTaskStatus::Cancelled => bail!("image task cancelled"),
                crate::models::ImageTaskStatus::Queued
                | crate::models::ImageTaskStatus::Running => {}
            }
            if Instant::now() >= deadline {
                bail!("image task timed out");
            }

            let config = self.storage.control.get_runtime_config().await?;
            if let Some(claimed) = self
                .storage
                .control
                .claim_next_image_task(config.global_image_concurrency, unix_timestamp_secs())
                .await?
            {
                let claimed_id = claimed.id.clone();
                if let Err(error) = self.execute_claimed_image_task(claimed).await {
                    if claimed_id == task_id {
                        return Err(error);
                    }
                }
                continue;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Builds the OpenAI-compatible image result for a completed persisted task.
    pub async fn image_result_for_task(
        &self,
        task: &ImageTaskRecord,
    ) -> Result<ChatgptImageResult> {
        let artifacts = self.storage.control.list_artifacts_for_session(&task.session_id).await?;
        let mut data = Vec::new();
        for artifact in artifacts.into_iter().filter(|artifact| artifact.task_id == task.id) {
            let bytes = self.storage.artifacts.read_artifact(&artifact).await?;
            data.push(GeneratedImageItem {
                b64_json: BASE64.encode(bytes),
                revised_prompt: artifact.revised_prompt.unwrap_or_else(|| task.prompt.clone()),
            });
        }
        if data.is_empty() {
            bail!("image task completed without artifacts");
        }
        Ok(ChatgptImageResult {
            created: task.finished_at.unwrap_or_else(unix_timestamp_secs),
            data,
            resolved_model: task.model.clone(),
        })
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

    /// Lists admin-visible account rows including effective proxy resolution.
    pub async fn list_admin_accounts(&self) -> Result<Vec<AdminAccountView>> {
        let accounts = self.storage.control.list_accounts().await?;
        let mut items = Vec::with_capacity(accounts.len());
        for account in accounts {
            items.push(self.admin_account_view(account).await?);
        }
        Ok(items)
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
        if update.proxy_mode.is_some() || update.proxy_config_id.is_some() {
            let proxy_mode = match update.proxy_mode.as_deref() {
                Some(raw) => crate::models::AccountProxyMode::parse(raw)
                    .ok_or_else(|| anyhow!("proxy_mode must be inherit, direct, or fixed"))?,
                None => account.proxy_mode,
            };
            let proxy_config_id = match &update.proxy_config_id {
                Some(value) => normalize_optional_string(value.as_deref()),
                None => account.proxy_config_id.clone(),
            };
            account.proxy_mode = proxy_mode;
            account.proxy_config_id = match proxy_mode {
                crate::models::AccountProxyMode::Fixed => {
                    let proxy_id = proxy_config_id.as_deref().ok_or_else(|| {
                        anyhow!("proxy_config_id is required when proxy_mode=`fixed`")
                    })?;
                    let proxy = self
                        .storage
                        .control
                        .get_proxy_config(proxy_id)
                        .await?
                        .ok_or_else(|| anyhow!("proxy config `{proxy_id}` not found"))?;
                    if proxy.status != "active" {
                        bail!("proxy config `{proxy_id}` is not active");
                    }
                    Some(proxy.id)
                }
                crate::models::AccountProxyMode::Direct
                | crate::models::AccountProxyMode::Inherit => None,
            };
        }
        self.storage.control.upsert_account(&account).await?;
        Ok(Some(account))
    }

    /// Lists all stored proxy configs.
    pub async fn list_proxy_configs(&self) -> Result<Vec<ProxyConfigRecord>> {
        self.storage.control.list_proxy_configs().await
    }

    /// Creates one reusable proxy config and returns the stored row.
    pub async fn create_proxy_config(
        &self,
        input: &ProxyConfigCreate,
    ) -> Result<ProxyConfigRecord> {
        let now = unix_timestamp_secs();
        let record = ProxyConfigRecord {
            id: format!("proxy_{}", Uuid::new_v4().simple()),
            name: normalize_required_string(&input.name, "name")?,
            proxy_url: normalize_proxy_url(&input.proxy_url)?,
            proxy_username: normalize_optional_string(input.proxy_username.as_deref()),
            proxy_password: normalize_optional_string(input.proxy_password.as_deref()),
            status: normalize_proxy_status(input.status.as_deref().unwrap_or("active"))?,
            created_at: now,
            updated_at: now,
        };
        self.storage.control.upsert_proxy_config(&record).await?;
        Ok(record)
    }

    /// Updates one stored proxy config and returns the updated row.
    pub async fn update_proxy_config(
        &self,
        proxy_id: &str,
        update: &ProxyConfigUpdate,
    ) -> Result<Option<ProxyConfigRecord>> {
        let Some(mut record) = self.storage.control.get_proxy_config(proxy_id).await? else {
            return Ok(None);
        };
        if let Some(name) = update.name.as_deref() {
            record.name = normalize_required_string(name, "name")?;
        }
        if let Some(proxy_url) = update.proxy_url.as_deref() {
            record.proxy_url = normalize_proxy_url(proxy_url)?;
        }
        if let Some(proxy_username) = &update.proxy_username {
            record.proxy_username = normalize_optional_string(proxy_username.as_deref());
        }
        if let Some(proxy_password) = &update.proxy_password {
            record.proxy_password = normalize_optional_string(proxy_password.as_deref());
        }
        if let Some(status) = update.status.as_deref() {
            record.status = normalize_proxy_status(status)?;
        }
        record.updated_at = unix_timestamp_secs();
        self.storage.control.upsert_proxy_config(&record).await?;
        Ok(Some(record))
    }

    /// Deletes one stored proxy config when no accounts still bind it.
    pub async fn delete_proxy_config(&self, proxy_id: &str) -> Result<bool> {
        if self.storage.control.count_accounts_bound_to_proxy_config(proxy_id).await? > 0 {
            bail!("proxy config `{proxy_id}` is still bound to one or more accounts");
        }
        self.storage.control.delete_proxy_config(proxy_id).await
    }

    /// Checks whether one stored proxy config can reach the configured upstream.
    pub async fn check_proxy_config(&self, proxy_id: &str) -> Result<ProxyConfigCheckResult> {
        let proxy = self
            .storage
            .control
            .get_proxy_config(proxy_id)
            .await?
            .ok_or_else(|| anyhow!("proxy config `{proxy_id}` not found"))?;
        let proxy_url = render_proxy_url_for_check(
            &proxy.proxy_url,
            proxy.proxy_username.as_deref(),
            proxy.proxy_password.as_deref(),
        )?;
        let client = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(proxy_url).context("invalid proxy URL")?)
            .timeout(Duration::from_secs(15))
            .build()
            .context("build proxy check client failed")?;
        let response = client
            .get(self.upstream.base_url())
            .send()
            .await
            .context("proxy check request failed")?;
        Ok(ProxyConfigCheckResult {
            ok: response.status().is_success(),
            message: format!("HTTP {}", response.status()),
            status_code: Some(response.status().as_u16()),
        })
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
            role: input.role.unwrap_or(ApiKeyRole::User),
            notification_email: normalize_optional_string(input.notification_email.as_deref()),
            notification_enabled: input.notification_enabled.unwrap_or(false),
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
        if let Some(role) = update.role {
            key.role = role;
        }
        if let Some(notification_email) = &update.notification_email {
            key.notification_email = normalize_optional_string(notification_email.as_deref());
        }
        if let Some(notification_enabled) = update.notification_enabled {
            key.notification_enabled = notification_enabled;
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
            role: ApiKeyRole::User,
            notification_email: None,
            notification_enabled: false,
        };
        self.storage.control.upsert_api_key(&key).await
    }

    async fn build_session_detail(&self, session: SessionRecord) -> Result<SessionDetail> {
        let messages = self.storage.control.list_messages_for_session(&session.id).await?;
        let tasks = self.storage.control.list_tasks_for_session(&session.id).await?;
        let artifacts = self.storage.control.list_artifacts_for_session(&session.id).await?;
        Ok(SessionDetail { session, messages, tasks, artifacts })
    }

    /// Executes a queue-claimed image task and persists its artifacts.
    pub async fn execute_claimed_image_task(&self, task: ImageTaskRecord) -> Result<()> {
        match self.execute_claimed_image_task_inner(&task).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let error_message = error.to_string();
                let _ = self
                    .storage
                    .control
                    .mark_image_task_failed(
                        &task.id,
                        unix_timestamp_secs(),
                        "image_task_failed",
                        &error_message,
                    )
                    .await;
                let _ = self
                    .storage
                    .control
                    .update_message_content_status(
                        &task.message_id,
                        json!({
                            "blocks": [{
                                "type": "error",
                                "text": error_message,
                            }],
                        }),
                        MessageStatus::Failed,
                    )
                    .await;
                Err(error)
            }
        }
    }

    async fn execute_claimed_image_task_inner(&self, task: &ImageTaskRecord) -> Result<()> {
        let Some(key) = self.storage.control.get_api_key(&task.key_id).await? else {
            bail!("api key not found for image task");
        };
        if key.status != "active" {
            bail!("api key is disabled");
        }
        let (edit_input, endpoint) = match task.mode.as_str() {
            "generation" => (None, "/v1/images/generations"),
            "edit" => (Some(edit_input_for_task(task)?), "/v1/images/edits"),
            _ => bail!("unsupported image task mode: {}", task.mode),
        };

        self.storage
            .control
            .mark_image_task_phase(&task.id, "allocating", json!({ "phase": "allocating" }))
            .await?;
        self.storage
            .control
            .mark_image_task_phase(&task.id, "running", json!({ "phase": "running" }))
            .await?;
        let result = self
            .execute_public_image_request(
                &key,
                &task.prompt,
                &task.model,
                task.n.max(1) as usize,
                edit_input,
                endpoint,
            )
            .await?;
        self.storage
            .control
            .mark_image_task_phase(&task.id, "saving", json!({ "phase": "saving" }))
            .await?;

        let mut artifacts = Vec::new();
        for (index, item) in result.data.iter().enumerate() {
            let artifact = self
                .storage
                .artifacts
                .write_generated_image(
                    &task.id,
                    &task.session_id,
                    &task.message_id,
                    &task.key_id,
                    item,
                    index,
                )
                .await?;
            self.storage.control.insert_image_artifact(&artifact).await?;
            artifacts.push(artifact);
        }

        let artifact_ids: Vec<String> =
            artifacts.iter().map(|artifact| artifact.id.clone()).collect();
        let blocks: Vec<Value> = artifacts
            .iter()
            .map(|artifact| {
                json!({
                    "type": "image",
                    "artifact_id": artifact.id,
                    "mime_type": artifact.mime_type,
                    "relative_path": artifact.relative_path,
                    "width": artifact.width,
                    "height": artifact.height,
                    "revised_prompt": artifact.revised_prompt,
                })
            })
            .collect();
        self.storage
            .control
            .update_message_content_status(
                &task.message_id,
                json!({
                    "blocks": blocks,
                    "prompt": task.prompt,
                    "model": result.resolved_model,
                }),
                MessageStatus::Done,
            )
            .await?;
        self.storage
            .control
            .mark_image_task_succeeded(&task.id, unix_timestamp_secs(), &artifact_ids)
            .await?;
        self.send_image_done_notification(&key, task, &artifacts).await?;
        Ok(())
    }

    async fn send_image_done_notification(
        &self,
        key: &ApiKeyRecord,
        task: &ImageTaskRecord,
        artifacts: &[crate::models::ImageArtifactRecord],
    ) -> Result<()> {
        if !key.notification_enabled {
            return Ok(());
        }
        let Some(email) = key
            .notification_email
            .as_deref()
            .filter(|value| !value.trim().is_empty() && is_valid_notification_email(value))
        else {
            return Ok(());
        };
        if !self.smtp_config.is_complete() {
            return Ok(());
        }
        let Some(session) = self.storage.control.get_session_for_admin(&task.session_id).await?
        else {
            return Ok(());
        };
        let config = self.storage.control.get_runtime_config().await?;
        let link = self
            .storage
            .control
            .create_signed_link(
                "image_task",
                &task.id,
                unix_timestamp_secs(),
                config.signed_link_ttl_seconds,
            )
            .await?;
        let base_url = self.smtp_config.public_base_url.as_deref().unwrap_or_default().trim();
        let signed_link =
            format!("{}/gpt2api/share/{}", base_url.trim_end_matches('/'), link.plaintext_token);
        let rendered = render_image_done_email(
            &session.title,
            &task.prompt,
            &task.model,
            artifacts.len(),
            &signed_link,
        );
        match send_rendered_email(&self.smtp_config, email, &rendered).await {
            Ok(NotificationOutcome::Sent | NotificationOutcome::Skipped) => Ok(()),
            Err(error) => {
                self.storage
                    .control
                    .append_task_event(
                        &task.id,
                        "notification_failed",
                        json!({ "error": error.to_string() }),
                    )
                    .await?;
                Ok(())
            }
        }
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
            let resolved_proxy = self.resolve_account_proxy(&account).await?;
            let result = match &edit_input {
                Some(edit_input) => {
                    self.upstream
                        .edit_image(&account, &resolved_proxy, prompt, requested_model, edit_input)
                        .await
                }
                None => {
                    self.upstream
                        .generate_image(&account, &resolved_proxy, prompt, requested_model)
                        .await
                }
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
            let resolved_proxy = self.resolve_account_proxy(&account).await?;
            match self
                .upstream
                .complete_text(&account, &resolved_proxy, prompt, requested_model)
                .await
            {
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
            let resolved_proxy = self.resolve_account_proxy(&account).await?;
            match self
                .upstream
                .start_text_stream(&account, &resolved_proxy, prompt, requested_model)
                .await
            {
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
            let accounts = self.routable_accounts_for_key(key).await?;
            let mut remaining: BTreeMap<String, AccountRecord> =
                accounts.into_iter().map(|account| (account.name.clone(), account)).collect();
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
            &account_scheduler_key(account),
            account.request_max_concurrency,
            account.request_min_start_interval_ms,
            Instant::now(),
        ) {
            Ok(lease) => Ok(lease),
            Err(rejection) => Err(rejection.wait),
        }
    }

    async fn routable_accounts_for_key(&self, key: &ApiKeyRecord) -> Result<Vec<AccountRecord>> {
        let accounts: Vec<_> = self
            .storage
            .control
            .list_accounts()
            .await?
            .into_iter()
            .filter(is_account_selectable)
            .collect();
        if accounts.is_empty() {
            bail!("no available accounts");
        }

        let Some(group_id) = key.account_group_id.as_deref() else {
            return Ok(accounts);
        };

        let account_names = self
            .storage
            .control
            .get_account_group_account_names(group_id)
            .await?
            .ok_or_else(|| anyhow!("configured account_group_id does not exist"))?;
        if key.route_strategy.eq_ignore_ascii_case("fixed") && account_names.len() != 1 {
            bail!("fixed route_strategy requires an account group with exactly one account");
        }
        let allowed_names: HashSet<_> = account_names.into_iter().collect();
        let filtered: Vec<_> =
            accounts.into_iter().filter(|account| allowed_names.contains(&account.name)).collect();
        if filtered.is_empty() {
            bail!("no configured route accounts are available");
        }
        Ok(filtered)
    }

    async fn refresh_account_if_needed(&self, account: AccountRecord) -> Result<AccountRecord> {
        let stale = account.last_refresh_at.is_some_and(|last_refresh_at| {
            unix_timestamp_secs().saturating_sub(last_refresh_at) >= DEFAULT_REFRESH_SECONDS as i64
        });
        if !stale && !should_refresh_access_token(&account) {
            return Ok(account);
        }
        let refreshed = self.refresh_account(account).await?;
        self.storage.control.upsert_account(&refreshed).await?;
        Ok(refreshed)
    }

    async fn refresh_account(&self, mut account: AccountRecord) -> Result<AccountRecord> {
        let resolved_proxy = self.resolve_account_proxy(&account).await?;
        let _ = self.maybe_refresh_account_access_token(&mut account, &resolved_proxy, false).await;
        match self.upstream.fetch_account_metadata(&account, &resolved_proxy).await {
            Ok(metadata) => {
                apply_account_metadata(&mut account, metadata);
                account.last_refresh_at = Some(unix_timestamp_secs());
                account.last_error = None;
                Ok(account)
            }
            Err(error) => {
                let token_invalid = is_token_invalid_error(&error.to_string());
                if token_invalid {
                    match self
                        .maybe_refresh_account_access_token(&mut account, &resolved_proxy, true)
                        .await
                    {
                        Ok(true) => match self
                            .upstream
                            .fetch_account_metadata(&account, &resolved_proxy)
                            .await
                        {
                            Ok(metadata) => {
                                apply_account_metadata(&mut account, metadata);
                                account.last_refresh_at = Some(unix_timestamp_secs());
                                account.last_error = None;
                                return Ok(account);
                            }
                            Err(retry_error) => {
                                account.last_error = Some(retry_error.to_string());
                            }
                        },
                        Ok(false) => {
                            account.last_error = Some(error.to_string());
                        }
                        Err(refresh_error) => {
                            account.last_error =
                                Some(format!("session refresh failed: {refresh_error}"));
                        }
                    }
                } else {
                    account.last_error = Some(error.to_string());
                }
                account.last_refresh_at = Some(unix_timestamp_secs());
                if token_invalid {
                    account.status = "invalid".to_string();
                    account.quota_remaining = 0;
                    account.quota_known = true;
                }
                Ok(account)
            }
        }
    }

    async fn maybe_refresh_account_access_token(
        &self,
        account: &mut AccountRecord,
        resolved_proxy: &ResolvedAccountProxy,
        force: bool,
    ) -> Result<bool> {
        if !force && !should_refresh_access_token(account) {
            return Ok(false);
        }
        let mut profile = account.browser_profile();
        let current_session_token = profile.session_token.clone();
        let has_session_token = current_session_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some();
        if !has_session_token {
            return Ok(false);
        }
        let refreshed = self.upstream.refresh_session_access_token(account, resolved_proxy).await?;
        let next_session_token = refreshed
            .session_token
            .or(current_session_token.clone())
            .filter(|value| !value.trim().is_empty());
        let access_token_changed = account.access_token != refreshed.access_token;
        let session_token_changed = next_session_token != current_session_token;
        if !access_token_changed && !session_token_changed {
            return Ok(false);
        }
        account.access_token = refreshed.access_token;
        profile.session_token = next_session_token;
        account.browser_profile_json = serde_json::to_string(&profile)?;
        self.storage.control.upsert_account(account).await?;
        Ok(true)
    }

    async fn resolve_account_proxy(&self, account: &AccountRecord) -> Result<ResolvedAccountProxy> {
        match account.proxy_mode {
            crate::models::AccountProxyMode::Direct => Ok(ResolvedAccountProxy {
                source: "direct",
                proxy_url: None,
                proxy_username: None,
                proxy_password: None,
                proxy_config_id: None,
                proxy_config_name: None,
            }),
            crate::models::AccountProxyMode::Inherit => Ok(ResolvedAccountProxy {
                source: "inherit",
                proxy_url: self.upstream.default_proxy_url(),
                proxy_username: None,
                proxy_password: None,
                proxy_config_id: None,
                proxy_config_name: None,
            }),
            crate::models::AccountProxyMode::Fixed => {
                let proxy_id = account
                    .proxy_config_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        anyhow!("proxy_config_id is required when proxy_mode=`fixed`")
                    })?;
                let proxy = self
                    .storage
                    .control
                    .get_proxy_config(proxy_id)
                    .await?
                    .ok_or_else(|| anyhow!("proxy config `{proxy_id}` not found"))?;
                if proxy.status != "active" {
                    bail!("proxy config `{proxy_id}` is not active");
                }
                Ok(ResolvedAccountProxy {
                    source: "fixed",
                    proxy_url: Some(proxy.proxy_url.clone()),
                    proxy_username: proxy.proxy_username.clone(),
                    proxy_password: proxy.proxy_password.clone(),
                    proxy_config_id: Some(proxy.id.clone()),
                    proxy_config_name: Some(proxy.name.clone()),
                })
            }
        }
    }

    async fn admin_account_view(&self, account: AccountRecord) -> Result<AdminAccountView> {
        let resolved_proxy = self.resolve_account_proxy(&account).await?;
        Ok(AdminAccountView {
            account,
            effective_proxy_source: resolved_proxy.source.to_string(),
            effective_proxy_url: resolved_proxy.proxy_url,
            effective_proxy_config_name: resolved_proxy.proxy_config_name,
        })
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

fn apply_account_metadata(account: &mut AccountRecord, metadata: AccountMetadata) {
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
}

fn text_message_content(text: &str) -> Value {
    json!({
        "blocks": [{
            "type": "text",
            "text": text,
        }],
    })
}

fn image_prompt_message_content(mode: &str, prompt: &str, model: &str, n: usize) -> Value {
    json!({
        "blocks": [{
            "type": "text",
            "text": prompt,
        }],
        "kind": if mode == "edit" { "image_edit" } else { "image_generation" },
        "model": model,
        "n": n,
    })
}

fn image_task_request_json(
    mode: &str,
    prompt: &str,
    model: &str,
    n: usize,
    edit_input: Option<&ImageEditInput>,
) -> Value {
    let mut payload = json!({
        "mode": mode,
        "prompt": prompt,
        "model": model,
        "n": n,
    });
    if let Some(edit_input) = edit_input {
        payload["image"] = json!({
            "data_b64": BASE64.encode(&edit_input.image_data),
            "file_name": edit_input.file_name,
            "mime_type": edit_input.mime_type,
        });
    }
    payload
}

fn edit_input_for_task(task: &ImageTaskRecord) -> Result<ImageEditInput> {
    let payload: Value = serde_json::from_str(&task.request_json)?;
    let image = payload
        .get("image")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("image edit task is missing image payload"))?;
    let data_b64 = image
        .get("data_b64")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("image edit task is missing image data"))?;
    let image_data = BASE64.decode(data_b64.as_bytes()).context("invalid edit image base64")?;
    let file_name =
        image.get("file_name").and_then(Value::as_str).unwrap_or("image.png").to_string();
    let mime_type =
        image.get("mime_type").and_then(Value::as_str).unwrap_or("image/png").to_string();
    Ok(ImageEditInput { image_data, file_name, mime_type })
}

fn should_refresh_access_token(account: &AccountRecord) -> bool {
    let has_session_token = account
        .browser_profile()
        .session_token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    if !has_session_token {
        return false;
    }
    access_token_expires_at(&account.access_token).is_some_and(|expires_at| {
        expires_at <= unix_timestamp_secs() + ACCESS_TOKEN_REFRESH_LEEWAY_SECONDS
    })
}

fn is_account_selectable(account: &AccountRecord) -> bool {
    account.status != "invalid" && account.status != "disabled"
}

fn is_account_routeable(account: &AccountRecord) -> bool {
    account.status == "active"
}

fn account_scheduler_key(account: &AccountRecord) -> String {
    format!("account:{}", account_scheduler_identity(account))
}

fn account_scheduler_identity(account: &AccountRecord) -> &str {
    account
        .user_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&account.name)
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

fn normalize_proxy_status(raw: &str) -> Result<String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "active" => Ok("active".to_string()),
        "disabled" => Ok("disabled".to_string()),
        _ => bail!("status must be active or disabled"),
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

fn normalize_proxy_url(raw: &str) -> Result<String> {
    let value = normalize_required_string(raw, "proxy_url")?;
    reqwest::Url::parse(&value).context("proxy_url must be a valid URL")?;
    Ok(value)
}

fn render_proxy_url_for_check(
    proxy_url: &str,
    proxy_username: Option<&str>,
    proxy_password: Option<&str>,
) -> Result<String> {
    let mut url = reqwest::Url::parse(proxy_url).context("proxy_url must be a valid URL")?;
    if let Some(username) = proxy_username.map(str::trim).filter(|value| !value.is_empty()) {
        url.set_username(username).map_err(|_| anyhow!("invalid proxy username"))?;
    }
    if let Some(password) = proxy_password.map(str::trim).filter(|value| !value.is_empty()) {
        url.set_password(Some(password)).map_err(|_| anyhow!("invalid proxy password"))?;
    }
    Ok(url.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ResolvedPaths,
        models::{AccountProxyMode, AccountRecord, BrowserProfile, ProxyConfigRecord},
        storage::Storage,
    };
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use serde_json::json;
    use tempfile::TempDir;
    use wiremock::{
        matchers::{header, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    async fn build_service() -> (TempDir, AppService) {
        let temp = tempfile::tempdir().expect("temp dir");
        let paths = ResolvedPaths::new(temp.path().to_path_buf());
        let storage = Storage::open(&paths).await.expect("storage opens");
        let service = AppService::new(
            storage,
            "admin-secret".to_string(),
            ChatgptUpstreamClient::new("http://127.0.0.1:9", None),
        )
        .await
        .expect("service init");
        (temp, service)
    }

    async fn build_service_with_base_url(base_url: &str) -> (TempDir, AppService) {
        let temp = tempfile::tempdir().expect("temp dir");
        let paths = ResolvedPaths::new(temp.path().to_path_buf());
        let storage = Storage::open(&paths).await.expect("storage opens");
        let service = AppService::new(
            storage,
            "admin-secret".to_string(),
            ChatgptUpstreamClient::new(base_url, None),
        )
        .await
        .expect("service init");
        (temp, service)
    }

    fn access_token_with_exp(exp: i64) -> String {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(json!({ "exp": exp }).to_string());
        format!("{header}.{payload}.sig")
    }

    fn browser_profile_json(session_token: &str) -> String {
        serde_json::to_string(&BrowserProfile {
            session_token: Some(session_token.to_string()),
            user_agent: None,
            impersonate_browser: Some("edge".to_string()),
            oai_device_id: None,
            sec_ch_ua: None,
            sec_ch_ua_mobile: None,
            sec_ch_ua_platform: None,
        })
        .expect("browser profile json")
    }

    #[tokio::test]
    async fn account_scheduler_limits_are_shared_by_upstream_user_id() {
        let (_temp, service) = build_service().await;

        let mut alpha = AccountRecord::minimal("alpha", "tok-alpha");
        alpha.user_id = Some("user-123".to_string());
        alpha.request_max_concurrency = Some(1);

        let mut beta = AccountRecord::minimal("beta", "tok-beta");
        beta.user_id = Some("user-123".to_string());
        beta.request_max_concurrency = Some(1);

        let lease = service
            .try_acquire_account_lease(&alpha)
            .expect("first account should acquire shared slot");
        let rejection = service
            .try_acquire_account_lease(&beta)
            .expect_err("same upstream user should not get a second local slot");

        assert_eq!(rejection, None);
        drop(lease);
    }

    #[tokio::test]
    async fn resolve_account_proxy_prefers_fixed_direct_and_inherit() {
        let temp = tempfile::tempdir().expect("temp dir");
        let paths = ResolvedPaths::new(temp.path().to_path_buf());
        let storage = Storage::open(&paths).await.expect("storage");
        let service = AppService::new(
            storage.clone(),
            "admin".to_string(),
            ChatgptUpstreamClient::new(
                "http://127.0.0.1:9",
                Some("http://127.0.0.1:11118".to_string()),
            ),
        )
        .await
        .expect("service");

        storage
            .control
            .upsert_proxy_config(&ProxyConfigRecord {
                id: "proxy-a".to_string(),
                name: "proxy-a".to_string(),
                proxy_url: "http://127.0.0.1:22222".to_string(),
                proxy_username: Some("bob".to_string()),
                proxy_password: Some("pw".to_string()),
                status: "active".to_string(),
                created_at: 1,
                updated_at: 1,
            })
            .await
            .expect("seed proxy");

        let inherit = AccountRecord::minimal("inherit", "tok-inherit");

        let mut direct = AccountRecord::minimal("direct", "tok-direct");
        direct.proxy_mode = AccountProxyMode::Direct;

        let mut fixed = AccountRecord::minimal("fixed", "tok-fixed");
        fixed.proxy_mode = AccountProxyMode::Fixed;
        fixed.proxy_config_id = Some("proxy-a".to_string());

        let inherit_resolved = service.resolve_account_proxy(&inherit).await.expect("inherit");
        let direct_resolved = service.resolve_account_proxy(&direct).await.expect("direct");
        let fixed_resolved = service.resolve_account_proxy(&fixed).await.expect("fixed");

        assert_eq!(inherit_resolved.proxy_url.as_deref(), Some("http://127.0.0.1:11118"));
        assert_eq!(direct_resolved.proxy_url, None);
        assert_eq!(fixed_resolved.proxy_url.as_deref(), Some("http://127.0.0.1:22222"));
        assert_eq!(fixed_resolved.proxy_config_name.as_deref(), Some("proxy-a"));
    }

    #[tokio::test]
    async fn refresh_account_if_needed_renews_expiring_access_token_from_session_token() {
        let server = MockServer::start().await;
        let new_access_token = "fresh_access_token";

        Mock::given(method("GET"))
            .and(path("/api/auth/session"))
            .and(header("cookie", "__Secure-next-auth.session-token=session_cookie"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(
                        "set-cookie",
                        "__Secure-next-auth.session-token=rotated_session; Path=/; HttpOnly; Secure",
                    )
                    .set_body_json(json!({
                        "accessToken": new_access_token
                    })),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/backend-api/me"))
            .and(header("authorization", format!("Bearer {new_access_token}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "user_renewed",
                "email": "renewed@example.com"
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/backend-api/conversation/init"))
            .and(header("authorization", format!("Bearer {new_access_token}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "default_model_slug": "gpt-image-1",
                "limits_progress": [{
                    "feature_name": "image_gen",
                    "remaining": 9,
                    "reset_after": "2026-04-24T12:00:00Z"
                }]
            })))
            .mount(&server)
            .await;

        let (_temp, service) = build_service_with_base_url(&server.uri()).await;
        let mut account = AccountRecord::minimal(
            "acct-expiring",
            &access_token_with_exp(unix_timestamp_secs() - 30),
        );
        account.browser_profile_json = browser_profile_json("session_cookie");
        account.last_refresh_at = Some(unix_timestamp_secs());

        let refreshed = service.refresh_account_if_needed(account).await.expect("refresh account");

        assert_eq!(refreshed.access_token, new_access_token);
        assert_eq!(refreshed.status, "active");
        assert_eq!(refreshed.email.as_deref(), Some("renewed@example.com"));
        assert_eq!(refreshed.user_id.as_deref(), Some("user_renewed"));
        assert_eq!(refreshed.restore_at.as_deref(), Some("2026-04-24T12:00:00Z"));
        assert_eq!(refreshed.browser_profile().session_token.as_deref(), Some("rotated_session"));

        let persisted = service
            .storage
            .control
            .find_account_by_access_token(new_access_token)
            .await
            .expect("find account")
            .expect("persisted account");
        assert_eq!(persisted.name, "acct-expiring");
    }

    #[tokio::test]
    async fn refresh_account_retries_token_invalid_with_session_refresh() {
        let server = MockServer::start().await;
        let old_access_token = "stale_access_token";
        let new_access_token = "fresh_after_retry";

        Mock::given(method("GET"))
            .and(path("/backend-api/me"))
            .and(header("authorization", format!("Bearer {old_access_token}")))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/auth/session"))
            .and(header("cookie", "__Secure-next-auth.session-token=session_cookie"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "accessToken": new_access_token
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/backend-api/me"))
            .and(header("authorization", format!("Bearer {new_access_token}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "user_retry",
                "email": "retry@example.com"
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/backend-api/conversation/init"))
            .and(header("authorization", format!("Bearer {new_access_token}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "default_model_slug": "gpt-image-1",
                "limits_progress": [{
                    "feature_name": "image_gen",
                    "remaining": 4,
                    "reset_after": "2026-04-24T18:00:00Z"
                }]
            })))
            .mount(&server)
            .await;

        let (_temp, service) = build_service_with_base_url(&server.uri()).await;
        let mut account = AccountRecord::minimal("acct-invalid", old_access_token);
        account.browser_profile_json = browser_profile_json("session_cookie");

        let refreshed = service.refresh_account(account).await.expect("refresh account");

        assert_eq!(refreshed.access_token, new_access_token);
        assert_eq!(refreshed.status, "active");
        assert_eq!(refreshed.email.as_deref(), Some("retry@example.com"));
        assert_eq!(refreshed.user_id.as_deref(), Some("user_retry"));
        assert_eq!(refreshed.restore_at.as_deref(), Some("2026-04-24T18:00:00Z"));
    }
}
