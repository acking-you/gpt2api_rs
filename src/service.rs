//! Service orchestration layer.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};
use uuid::Uuid;

use crate::{
    accounts::import::{build_account_record, parse_access_token_seed, parse_session_seed},
    activity::{RequestActivityGuard, RequestActivitySnapshot, RequestActivityTracker},
    config::SmtpConfig,
    models::{
        AccountGroupRecord, AccountMetadata, AccountRecord, ApiKeyRecord, ApiKeyRole,
        BrowserProfile, ImageSubmissionResult, ImageTaskRecord, MessageRecord, MessageStatus,
        ProxyConfigRecord, SessionDetail, SessionRecord, SessionSource, UsageEventRecord,
    },
    notifications::{
        is_valid_notification_email, render_image_done_email, send_rendered_email,
        NotificationOutcome,
    },
    routing::select_best_candidate,
    scheduler::{Lease, LocalRequestScheduler},
    storage::{control::CreateImageTaskInput, Storage},
    upstream::chatgpt::{
        access_token_expires_at, is_token_invalid_error, ChatgptImageResult,
        ChatgptImageTextResponse, ChatgptTextResult, ChatgptTextStream, ChatgptUpstreamClient,
        GeneratedImageItem,
    },
};

const DEFAULT_KEY_ID: &str = "default";
const DEFAULT_REFRESH_SECONDS: u64 = 300;
const ACCESS_TOKEN_REFRESH_LEEWAY_SECONDS: i64 = 300;
const DEFAULT_OUTBOX_FLUSH_INTERVAL_SECONDS: u64 = 5;
const DEFAULT_OUTBOX_FLUSH_BATCH_SIZE: u64 = 100;
const DEFAULT_IMAGE_SIZE: &str = "1024x1024";
const BASE_IMAGE_CREDIT_AREA: u64 = 1024 * 1024;
const MIN_IMAGE_DIMENSION: u32 = 256;
const MAX_IMAGE_DIMENSION: u32 = 4096;

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

/// Mutable account-group fields allowed through the admin create surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountGroupCreate {
    /// Human-readable account-group label.
    pub name: String,
    /// Upstream account names that belong to this group.
    pub account_names: Vec<String>,
}

/// Mutable account-group fields allowed through the admin update surface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountGroupUpdate {
    /// Optional replacement account-group label.
    pub name: Option<String>,
    /// Optional replacement member account names.
    pub account_names: Option<Vec<String>>,
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
    /// Optional directly bound upstream account name for fixed routing.
    pub fixed_account_name: Option<String>,
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
    /// Optional replacement fixed account name. `Some(None)` clears the field.
    pub fixed_account_name: Option<Option<String>>,
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
    size: &'a str,
    edit_input: Option<ImageEditInput>,
    request_context: RequestLogContext,
}

/// Sanitized downstream request metadata persisted into usage events.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestLogContext {
    /// Downstream HTTP method.
    pub method: String,
    /// Downstream request path.
    pub url: String,
    /// Client IP derived by the HTTP layer.
    pub client_ip: String,
    /// Sanitized request headers JSON.
    pub request_headers_json: Option<String>,
}

/// Image-generation request options plus request metadata.
#[derive(Debug, Clone)]
pub struct ImageGenerationSubmission {
    /// Requested output size.
    pub size: String,
    /// Downstream request metadata.
    pub request_context: RequestLogContext,
}

/// Image-edit upload plus request metadata.
#[derive(Debug, Clone)]
pub struct ImageEditSubmission {
    /// Uploaded edit image.
    pub edit_input: ImageEditInput,
    /// Requested output size.
    pub size: String,
    /// Downstream request metadata.
    pub request_context: RequestLogContext,
}

impl RequestLogContext {
    /// Builds internal metadata when no external HTTP request is available.
    #[must_use]
    pub fn internal(endpoint: &str) -> Self {
        Self {
            method: "POST".to_string(),
            url: endpoint.to_string(),
            client_ip: String::new(),
            request_headers_json: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ImagePromptContext {
    effective_prompt: String,
    billable_credits: i64,
    size: String,
    size_credit_units: i64,
    context_text_count: i64,
    context_image_count: i64,
    context_credit_surcharge: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct UsageEventContext {
    request: RequestLogContext,
    endpoint: String,
    session_id: Option<String>,
    task_id: Option<String>,
    mode: String,
    image_size: Option<String>,
    size_credit_units: i64,
    billable_credits: i64,
    context_text_count: i64,
    context_image_count: i64,
    context_credit_surcharge: i64,
    raw_prompt: String,
    effective_prompt: String,
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
    request_activity: Arc<RequestActivityTracker>,
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
            request_activity: Arc::new(RequestActivityTracker::new()),
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

    /// Returns whether image-completion email delivery has complete SMTP config.
    #[must_use]
    pub fn email_notifications_configured(&self) -> bool {
        self.smtp_config.is_complete()
    }

    /// Records one started API request for live admin activity metrics.
    #[must_use]
    pub fn request_activity_start(&self, key_id: &str) -> RequestActivityGuard {
        self.request_activity.start(key_id)
    }

    /// Returns current total or per-key API request activity.
    #[must_use]
    pub fn request_activity_snapshot(&self, key_id: Option<&str>) -> RequestActivitySnapshot {
        self.request_activity.snapshot(key_id)
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

    /// Returns a key clone with usage populated from DuckDB plus pending outbox events.
    pub async fn key_with_ledger_usage(&self, key: &ApiKeyRecord) -> Result<ApiKeyRecord> {
        let mut hydrated = key.clone();
        hydrated.quota_used_calls = self.current_key_usage_credits(&key.id).await?;
        Ok(hydrated)
    }

    /// Lists keys with usage populated from the immutable usage-event ledger.
    pub async fn list_api_keys_with_ledger_usage(&self) -> Result<Vec<ApiKeyRecord>> {
        let keys = self.storage.control.list_api_keys().await?;
        let mut hydrated = Vec::with_capacity(keys.len());
        for key in keys {
            hydrated.push(self.key_with_ledger_usage(&key).await?);
        }
        Ok(hydrated)
    }

    /// Returns persisted DuckDB usage plus not-yet-flushed outbox usage for one key.
    pub async fn current_key_usage_credits(&self, key_id: &str) -> Result<i64> {
        let persisted = self.storage.events.sum_billable_credits_for_key(key_id).await?;
        let pending = self.storage.control.sum_pending_billable_credits_for_key(key_id).await?;
        Ok(persisted.saturating_add(pending))
    }

    async fn ensure_key_can_consume_current(&self, key: &ApiKeyRecord, cost: i64) -> Result<()> {
        if key.status != "active" {
            bail!(PublicAuthError::Disabled.to_string());
        }
        if cost <= 0 {
            return Ok(());
        }
        let used = self.current_key_usage_credits(&key.id).await?;
        let current = ApiKeyRecord { quota_used_calls: used, ..key.clone() };
        ensure_key_can_consume(&current, cost).map_err(|error| anyhow!(error.to_string()))
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

    /// Permanently deletes one key-scoped session and its stored conversation artifacts.
    pub async fn delete_session_for_key(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
    ) -> Result<bool> {
        let Some(artifacts) =
            self.storage.control.hard_delete_session_for_key(session_id, &key.id).await?
        else {
            return Ok(false);
        };
        self.storage.artifacts.delete_artifacts(&artifacts).await?;
        Ok(true)
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
        size: &str,
    ) -> Result<Option<ImageSubmissionResult>> {
        self.submit_image_generation_message_with_context(
            key,
            session_id,
            prompt,
            model,
            n,
            ImageGenerationSubmission {
                size: size.to_string(),
                request_context: RequestLogContext::internal("/v1/images/generations"),
            },
        )
        .await
    }

    /// Queues one image-generation message with downstream request metadata.
    pub async fn submit_image_generation_message_with_context(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
        prompt: &str,
        model: &str,
        n: usize,
        submission: ImageGenerationSubmission,
    ) -> Result<Option<ImageSubmissionResult>> {
        self.submit_image_message(ImageMessageInput {
            key,
            session_id,
            mode: "generation",
            prompt,
            model,
            n,
            size: &submission.size,
            edit_input: None,
            request_context: submission.request_context,
        })
        .await
    }

    /// Queues one image-edit message with downstream request metadata.
    pub async fn submit_image_edit_message_with_context(
        &self,
        key: &ApiKeyRecord,
        session_id: &str,
        prompt: &str,
        model: &str,
        n: usize,
        submission: ImageEditSubmission,
    ) -> Result<Option<ImageSubmissionResult>> {
        self.submit_image_message(ImageMessageInput {
            key,
            session_id,
            mode: "edit",
            prompt,
            model,
            n,
            size: &submission.size,
            edit_input: Some(submission.edit_input),
            request_context: submission.request_context,
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
        let size = normalize_image_size(input.size)?;
        let existing_detail = self.build_session_detail(session.clone()).await?;
        let image_context = build_session_aware_image_prompt(&existing_detail, &prompt, n, &size);
        self.ensure_key_can_consume_current(key, image_context.billable_credits).await?;
        let user_message = self
            .storage
            .control
            .append_message(
                &session.id,
                &key.id,
                "user",
                image_prompt_message_content(input.mode, &prompt, &model, n, &size),
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
        let request_json = image_task_request_json(
            input.mode,
            &prompt,
            &model,
            n,
            input.edit_input.as_ref(),
            &image_context,
            &input.request_context,
        );
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
                if let Err(error) = self
                    .execute_claimed_image_task_with_timeout(
                        claimed,
                        config.image_task_timeout_seconds,
                    )
                    .await
                {
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

    /// Lists all reusable account groups.
    pub async fn list_account_groups(&self) -> Result<Vec<AccountGroupRecord>> {
        self.storage.control.list_account_groups().await
    }

    /// Creates one reusable account group.
    pub async fn create_account_group(
        &self,
        input: &AccountGroupCreate,
    ) -> Result<AccountGroupRecord> {
        let account_names = self.normalize_account_group_members(&input.account_names).await?;
        let record = AccountGroupRecord {
            id: format!("group_{}", Uuid::new_v4().simple()),
            name: normalize_required_string(&input.name, "name")?,
            account_names,
        };
        self.storage.control.upsert_account_group(&record).await?;
        Ok(record)
    }

    /// Updates one reusable account group and returns the updated row.
    pub async fn update_account_group(
        &self,
        group_id: &str,
        update: &AccountGroupUpdate,
    ) -> Result<Option<AccountGroupRecord>> {
        let Some(mut record) = self.storage.control.get_account_group(group_id).await? else {
            return Ok(None);
        };
        if let Some(name) = update.name.as_deref() {
            record.name = normalize_required_string(name, "name")?;
        }
        if let Some(account_names) = &update.account_names {
            record.account_names = self.normalize_account_group_members(account_names).await?;
        }
        self.storage.control.upsert_account_group(&record).await?;
        Ok(Some(record))
    }

    /// Deletes one reusable account group when no API key still references it.
    pub async fn delete_account_group(&self, group_id: &str) -> Result<bool> {
        let keys = self.storage.control.list_api_keys().await?;
        if keys.iter().any(|key| key.account_group_id.as_deref() == Some(group_id)) {
            bail!("account group `{group_id}` is still bound to one or more keys");
        }
        self.storage.control.delete_account_group(group_id).await
    }

    async fn normalize_account_group_members(&self, names: &[String]) -> Result<Vec<String>> {
        let known_accounts: HashSet<_> = self
            .storage
            .control
            .list_accounts()
            .await?
            .into_iter()
            .map(|account| account.name)
            .collect();
        let mut normalized = Vec::new();
        let mut seen = HashSet::new();
        for name in names {
            let name = normalize_required_string(name, "account_name")?;
            if !known_accounts.contains(&name) {
                bail!("unknown account `{name}`");
            }
            if seen.insert(name.clone()) {
                normalized.push(name);
            }
        }
        normalized.sort();
        Ok(normalized)
    }

    async fn validate_api_key_route_config(
        &self,
        route_strategy: &str,
        account_group_id: Option<&str>,
        fixed_account_name: Option<&str>,
    ) -> Result<()> {
        if fixed_account_name.is_some() && account_group_id.is_some() {
            bail!("route can bind either an account group or a single account, not both");
        }
        if let Some(account_name) = fixed_account_name {
            if !route_strategy.eq_ignore_ascii_case("fixed") {
                bail!("single-account binding requires fixed routing");
            }
            let account = self
                .storage
                .control
                .get_account(account_name)
                .await?
                .ok_or_else(|| anyhow!("account `{account_name}` not found"))?;
            if !is_account_selectable(&account) {
                bail!("account `{account_name}` is not selectable");
            }
            return Ok(());
        }
        let Some(group_id) = account_group_id else {
            if route_strategy.eq_ignore_ascii_case("fixed") {
                bail!("fixed route requires a fixed account or a single-account group");
            }
            return Ok(());
        };
        let group = self
            .storage
            .control
            .get_account_group(group_id)
            .await?
            .ok_or_else(|| anyhow!("account group `{group_id}` not found"))?;
        if route_strategy.eq_ignore_ascii_case("fixed") && group.account_names.len() != 1 {
            bail!("fixed route requires an account group with exactly one account");
        }
        Ok(())
    }

    /// Creates one downstream API key and returns the stored plaintext secret.
    pub async fn create_api_key(&self, input: &ApiKeyCreate) -> Result<ApiKeySecretRecord> {
        let secret_plaintext = generate_api_key_plaintext();
        let route_strategy = normalize_route_strategy(&input.route_strategy)?;
        let account_group_id = normalize_optional_string(input.account_group_id.as_deref());
        let fixed_account_name = normalize_optional_string(input.fixed_account_name.as_deref());
        self.validate_api_key_route_config(
            &route_strategy,
            account_group_id.as_deref(),
            fixed_account_name.as_deref(),
        )
        .await?;
        let key = ApiKeyRecord {
            id: format!("key_{}", Uuid::new_v4().simple()),
            name: normalize_required_string(&input.name, "name")?,
            secret_hash: sha256_hex(&secret_plaintext),
            secret_plaintext: Some(secret_plaintext.clone()),
            status: normalize_api_key_status(input.status.as_deref().unwrap_or("active"))?,
            quota_total_calls: validate_quota_total_calls(input.quota_total_calls)?,
            quota_used_calls: 0,
            route_strategy,
            account_group_id,
            fixed_account_name,
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
        if let Some(fixed_account_name) = &update.fixed_account_name {
            key.fixed_account_name = normalize_optional_string(fixed_account_name.as_deref());
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
        self.validate_api_key_route_config(
            &key.route_strategy,
            key.account_group_id.as_deref(),
            key.fixed_account_name.as_deref(),
        )
        .await?;
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
        self.generate_images_for_key(&key, prompt, requested_model, requested_n, DEFAULT_IMAGE_SIZE)
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
        let key = self
            .authenticate_public_key(bearer)
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        self.edit_images_for_key(
            &key,
            prompt,
            requested_model,
            requested_n,
            DEFAULT_IMAGE_SIZE,
            edit_input,
        )
        .await
    }

    /// Executes one text-to-image request for an already authenticated key.
    pub async fn generate_images_for_key(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        requested_n: usize,
        requested_size: &str,
    ) -> Result<ChatgptImageResult> {
        let size = normalize_image_size(requested_size)?;
        let size_credit_units = image_size_credit_units(&size);
        let mut usage_context =
            direct_usage_context("/v1/images/generations", "generation", prompt);
        usage_context.image_size = Some(size.clone());
        usage_context.size_credit_units = size_credit_units;
        usage_context.billable_credits = requested_n.clamp(1, 4) as i64 * size_credit_units;
        usage_context.effective_prompt = render_image_mode_prompt(&[], &[], prompt, &size);
        let effective_prompt = usage_context.effective_prompt.clone();
        self.execute_public_image_request(
            key,
            &effective_prompt,
            requested_model,
            requested_n,
            None,
            usage_context,
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
        requested_size: &str,
        edit_input: ImageEditInput,
    ) -> Result<ChatgptImageResult> {
        let size = normalize_image_size(requested_size)?;
        let size_credit_units = image_size_credit_units(&size);
        let mut usage_context = direct_usage_context("/v1/images/edits", "edit", prompt);
        usage_context.image_size = Some(size.clone());
        usage_context.size_credit_units = size_credit_units;
        usage_context.billable_credits = requested_n.clamp(1, 4) as i64 * size_credit_units;
        usage_context.effective_prompt = render_image_mode_prompt(&[], &[], prompt, &size);
        let effective_prompt = usage_context.effective_prompt.clone();
        self.execute_public_image_request(
            key,
            &effective_prompt,
            requested_model,
            requested_n,
            Some(edit_input),
            usage_context,
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
        self.complete_text_for_key_with_context(
            key,
            prompt,
            requested_model,
            endpoint,
            RequestLogContext::internal(endpoint),
        )
        .await
    }

    /// Executes one non-streaming text completion with downstream request metadata.
    pub async fn complete_text_for_key_with_context(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        endpoint: &str,
        request_context: RequestLogContext,
    ) -> Result<ChatgptTextResult> {
        let mut usage_context = direct_usage_context(endpoint, "text", prompt);
        usage_context.request = request_context;
        self.execute_public_text_request(key, prompt, requested_model, endpoint, usage_context)
            .await
    }

    /// Opens one streaming text completion for an already authenticated key.
    pub async fn start_text_stream_for_key(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        endpoint: &str,
    ) -> Result<ChatgptTextStream> {
        self.start_text_stream_for_key_with_context(
            key,
            prompt,
            requested_model,
            endpoint,
            RequestLogContext::internal(endpoint),
        )
        .await
    }

    /// Opens one streaming text completion with downstream request metadata.
    pub async fn start_text_stream_for_key_with_context(
        &self,
        key: &ApiKeyRecord,
        prompt: &str,
        requested_model: &str,
        endpoint: &str,
        request_context: RequestLogContext,
    ) -> Result<ChatgptTextStream> {
        let mut usage_context = direct_usage_context(endpoint, "text_stream", prompt);
        usage_context.request = request_context;
        self.start_public_text_stream(key, prompt, requested_model, endpoint, usage_context).await
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
            fixed_account_name: None,
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

    /// Fails image tasks that were claimed by a previous process.
    ///
    /// A running task is owned by an in-process worker future. After a service restart that future
    /// no longer exists, so the task cannot produce artifacts or worker updates. Leaving it as
    /// running would permanently consume one global queue slot.
    pub async fn recover_interrupted_image_tasks(&self) -> Result<usize> {
        let tasks = self.storage.control.list_running_image_tasks().await?;
        if tasks.is_empty() {
            return Ok(0);
        }

        let finished_at = unix_timestamp_secs();
        let error_message =
            "Image task interrupted because the gpt2api service restarted. Please send again.";
        for task in &tasks {
            self.storage
                .control
                .mark_image_task_failed(
                    &task.id,
                    finished_at,
                    "image_task_interrupted",
                    error_message,
                )
                .await?;
            self.storage
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
                .await?;
        }

        Ok(tasks.len())
    }

    /// Executes a queue-claimed image task with a hard runtime cap.
    pub async fn execute_claimed_image_task_with_timeout(
        &self,
        task: ImageTaskRecord,
        timeout_seconds: i64,
    ) -> Result<()> {
        let timeout_seconds = timeout_seconds.max(1) as u64;
        match tokio::time::timeout(
            Duration::from_secs(timeout_seconds),
            self.execute_claimed_image_task(task.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let error_message = format!(
                    "Image task timed out after {timeout_seconds} seconds. Please send again."
                );
                self.storage
                    .control
                    .mark_image_task_failed(
                        &task.id,
                        unix_timestamp_secs(),
                        "image_task_timeout",
                        &error_message,
                    )
                    .await?;
                self.storage
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
                    .await?;
                bail!(error_message);
            }
        }
    }

    /// Executes a queue-claimed image task and persists its artifacts.
    pub async fn execute_claimed_image_task(&self, task: ImageTaskRecord) -> Result<()> {
        match self.execute_claimed_image_task_inner(&task).await {
            Ok(()) => Ok(()),
            Err(error) => {
                if let Some(text_response) = error.downcast_ref::<ChatgptImageTextResponse>() {
                    self.finish_image_task_with_assistant_text(&task, text_response).await?;
                    return Ok(());
                }
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

    async fn finish_image_task_with_assistant_text(
        &self,
        task: &ImageTaskRecord,
        text_response: &ChatgptImageTextResponse,
    ) -> Result<()> {
        let finished_at = unix_timestamp_secs();
        self.storage
            .control
            .mark_image_task_phase(
                &task.id,
                "saving",
                json!({ "phase": "saving", "assistant_text": true }),
            )
            .await?;
        self.storage
            .control
            .update_message_content_status(
                &task.message_id,
                json!({
                    "blocks": [{ "type": "text", "text": text_response.text }],
                    "prompt": task.prompt,
                    "model": text_response.resolved_model,
                    "image_task_result": "assistant_text",
                }),
                MessageStatus::Done,
            )
            .await?;
        self.storage.control.mark_image_task_succeeded(&task.id, finished_at, &[]).await?;
        Ok(())
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
        let usage_context = usage_context_for_image_task(task, endpoint);
        let effective_prompt = usage_context.effective_prompt.clone();
        let result = self
            .execute_public_image_request(
                &key,
                &effective_prompt,
                &task.model,
                task.n.max(1) as usize,
                edit_input,
                usage_context,
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
        usage_context: UsageEventContext,
    ) -> Result<ChatgptImageResult> {
        self.ensure_key_can_consume_current(key, usage_context.billable_credits).await?;
        let key_lease = self.acquire_key_lease(key).await?;
        let _keep_key_lease = key_lease;

        let requested_n = requested_n.clamp(1, 4);
        let request_id = Uuid::new_v4().to_string();
        let started = Instant::now();
        let mut images = Vec::new();
        let mut resolved_model = String::new();
        let mut selected_account_name = String::new();
        let mut last_error = None;
        let mut text_response = None;

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
                    if let Some(response) = error.downcast_ref::<ChatgptImageTextResponse>() {
                        self.record_account_success(&account).await?;
                        resolved_model = response.resolved_model.clone();
                        text_response = Some(response.clone());
                        break;
                    }
                    self.record_account_failure(&account, &error.to_string()).await?;
                    last_error = Some(error.to_string());
                    if is_token_invalid_error(&error.to_string()) {
                        continue;
                    }
                }
            }
        }

        if images.is_empty() {
            if let Some(response) = text_response {
                return Err(response.into());
            }
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
            endpoint: usage_context.endpoint.clone(),
            request_method: usage_context.request.method.clone(),
            request_url: usage_context.request.url.clone(),
            requested_model: requested_model.to_string(),
            resolved_upstream_model: result.resolved_model.clone(),
            session_id: usage_context.session_id.clone(),
            task_id: usage_context.task_id.clone(),
            mode: usage_context.mode.clone(),
            image_size: usage_context.image_size.clone(),
            requested_n: requested_n as i64,
            generated_n: result.data.len() as i64,
            billable_images: result.data.len() as i64,
            billable_credits: usage_context.billable_credits,
            size_credit_units: usage_context.size_credit_units,
            context_text_count: usage_context.context_text_count,
            context_image_count: usage_context.context_image_count,
            context_credit_surcharge: usage_context.context_credit_surcharge,
            client_ip: usage_context.request.client_ip.clone(),
            request_headers_json: usage_context.request.request_headers_json.clone(),
            prompt_preview: prompt_preview(&usage_context.raw_prompt),
            last_message_content: prompt_preview(&usage_context.raw_prompt),
            request_body_json: None,
            prompt_chars: usage_context.raw_prompt.chars().count() as i64,
            effective_prompt_chars: usage_context.effective_prompt.chars().count() as i64,
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
        usage_context: UsageEventContext,
    ) -> Result<ChatgptTextResult> {
        self.ensure_key_can_consume_current(key, 1).await?;
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
                        request_method: usage_context.request.method.clone(),
                        request_url: usage_context.request.url.clone(),
                        requested_model: requested_model.to_string(),
                        resolved_upstream_model: result.resolved_model.clone(),
                        session_id: usage_context.session_id.clone(),
                        task_id: usage_context.task_id.clone(),
                        mode: usage_context.mode.clone(),
                        image_size: usage_context.image_size.clone(),
                        requested_n: 1,
                        generated_n: 1,
                        billable_images: 1,
                        billable_credits: usage_context.billable_credits,
                        size_credit_units: usage_context.size_credit_units,
                        context_text_count: usage_context.context_text_count,
                        context_image_count: usage_context.context_image_count,
                        context_credit_surcharge: usage_context.context_credit_surcharge,
                        client_ip: usage_context.request.client_ip.clone(),
                        request_headers_json: usage_context.request.request_headers_json.clone(),
                        prompt_preview: prompt_preview(&usage_context.raw_prompt),
                        last_message_content: prompt_preview(&usage_context.raw_prompt),
                        request_body_json: None,
                        prompt_chars: usage_context.raw_prompt.chars().count() as i64,
                        effective_prompt_chars: usage_context.effective_prompt.chars().count()
                            as i64,
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
        usage_context: UsageEventContext,
    ) -> Result<ChatgptTextStream> {
        self.ensure_key_can_consume_current(key, 1).await?;
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
                        request_method: usage_context.request.method.clone(),
                        request_url: usage_context.request.url.clone(),
                        requested_model: requested_model.to_string(),
                        resolved_upstream_model: stream.resolved_model.clone(),
                        session_id: usage_context.session_id.clone(),
                        task_id: usage_context.task_id.clone(),
                        mode: usage_context.mode.clone(),
                        image_size: usage_context.image_size.clone(),
                        requested_n: 1,
                        generated_n: 1,
                        billable_images: 1,
                        billable_credits: usage_context.billable_credits,
                        size_credit_units: usage_context.size_credit_units,
                        context_text_count: usage_context.context_text_count,
                        context_image_count: usage_context.context_image_count,
                        context_credit_surcharge: usage_context.context_credit_surcharge,
                        client_ip: usage_context.request.client_ip.clone(),
                        request_headers_json: usage_context.request.request_headers_json.clone(),
                        prompt_preview: prompt_preview(&usage_context.raw_prompt),
                        last_message_content: prompt_preview(&usage_context.raw_prompt),
                        request_body_json: None,
                        prompt_chars: usage_context.raw_prompt.chars().count() as i64,
                        effective_prompt_chars: usage_context.effective_prompt.chars().count()
                            as i64,
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

        if let Some(account_name) = normalize_optional_string(key.fixed_account_name.as_deref()) {
            let Some(account) = accounts.into_iter().find(|account| account.name == account_name)
            else {
                bail!("configured fixed account is not available");
            };
            return Ok(vec![account]);
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

fn build_session_aware_image_prompt(
    detail: &SessionDetail,
    prompt: &str,
    requested_n: usize,
    size: &str,
) -> ImagePromptContext {
    let text_items = prior_text_context(detail, 8);
    let image_items = prior_image_context(detail, 4);
    let size_credit_units = image_size_credit_units(size);
    let context_text_count = text_items.len() as i64;
    let context_image_count = image_items.len() as i64;
    let context_credit_surcharge =
        session_context_credit_surcharge(context_text_count, context_image_count);
    let billable_credits = requested_n as i64 * size_credit_units + context_credit_surcharge;
    let effective_prompt = render_image_mode_prompt(&text_items, &image_items, prompt, size);

    ImagePromptContext {
        effective_prompt,
        billable_credits,
        size: size.to_string(),
        size_credit_units,
        context_text_count,
        context_image_count,
        context_credit_surcharge,
    }
}

fn render_image_mode_prompt(
    text_items: &[String],
    image_items: &[String],
    prompt: &str,
    size: &str,
) -> String {
    let mut effective_prompt = String::from(
        "Image generation mode: create the image now. Do not ask follow-up questions. Do not answer with text, markdown, JSON, or options. If details are missing, make reasonable visual choices and generate a finished image.\n\n",
    );
    if !text_items.is_empty() {
        effective_prompt.push_str("Prior text context:\n");
        for item in text_items {
            effective_prompt.push_str("- ");
            effective_prompt.push_str(item);
            effective_prompt.push('\n');
        }
        effective_prompt.push('\n');
    }
    if !image_items.is_empty() {
        effective_prompt.push_str("Prior generated image context:\n");
        for item in image_items {
            effective_prompt.push_str("- ");
            effective_prompt.push_str(item);
            effective_prompt.push('\n');
        }
        effective_prompt.push('\n');
    }
    effective_prompt.push_str("Requested image size: ");
    effective_prompt.push_str(size);
    effective_prompt.push_str(
        ". Match this aspect ratio and output size as closely as the image model allows.\n\n",
    );
    effective_prompt.push_str("Current request:\n");
    effective_prompt.push_str(prompt);
    effective_prompt
}

fn prior_text_context(detail: &SessionDetail, limit: usize) -> Vec<String> {
    let mut items = Vec::new();
    for message in detail.messages.iter().rev() {
        if message.status != MessageStatus::Done {
            continue;
        }
        for text in message_text_blocks(&message.content_json).into_iter().rev() {
            let text = compact_context_text(&text, 600);
            if text.is_empty() {
                continue;
            }
            items.push(format!("{}: {}", message.role, text));
            if items.len() >= limit {
                items.reverse();
                return items;
            }
        }
    }
    items.reverse();
    items
}

fn prior_image_context(detail: &SessionDetail, limit: usize) -> Vec<String> {
    let task_prompts = detail
        .tasks
        .iter()
        .map(|task| (task.id.as_str(), task.prompt.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut artifacts = detail.artifacts.clone();
    artifacts.sort_by_key(|artifact| artifact.created_at);
    artifacts
        .into_iter()
        .rev()
        .map(|artifact| {
            let mut parts = Vec::new();
            if let Some(prompt) = task_prompts
                .get(artifact.task_id.as_str())
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
            {
                parts.push(format!("source prompt: {}", compact_context_text(prompt, 800)));
            } else if let Some(revised_prompt) =
                artifact.revised_prompt.as_deref().map(str::trim).filter(|value| !value.is_empty())
            {
                parts
                    .push(format!("revised prompt: {}", compact_context_text(revised_prompt, 800)));
            }
            if let (Some(width), Some(height)) = (artifact.width, artifact.height) {
                parts.push(format!("size: {}x{}", width, height));
            }
            parts.push(format!("artifact: {}", artifact.relative_path));
            parts.join("; ")
        })
        .take(limit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

fn message_text_blocks(content_json: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<Value>(content_json) else {
        return vec![content_json.to_string()];
    };
    value
        .get("blocks")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .map(ToString::to_string)
        .collect()
}

fn compact_context_text(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let mut compact = normalized.chars().take(max_chars).collect::<String>();
    compact.push_str("...");
    compact
}

fn session_context_credit_surcharge(text_count: i64, image_count: i64) -> i64 {
    let text_credit = i64::from(text_count > 0);
    let image_credit = image_count.min(3);
    (text_credit + image_credit).min(4)
}

fn normalize_image_size(size: &str) -> Result<String> {
    let normalized = size.trim().to_ascii_lowercase();
    let size = normalized.as_str();
    let size = if size.is_empty() || size.eq_ignore_ascii_case("auto") {
        DEFAULT_IMAGE_SIZE
    } else {
        size
    };
    let (width, height) = parse_image_dimensions(size)?;
    Ok(format!("{width}x{height}"))
}

fn image_size_credit_units(size: &str) -> i64 {
    parse_image_dimensions(size)
        .map(|(width, height)| {
            let area = u64::from(width) * u64::from(height);
            area.div_ceil(BASE_IMAGE_CREDIT_AREA).max(1) as i64
        })
        .unwrap_or(1)
}

fn parse_image_dimensions(size: &str) -> Result<(u32, u32)> {
    let (width, height) = size
        .split_once('x')
        .ok_or_else(|| anyhow!("size must use WIDTHxHEIGHT format, for example 1024x1024"))?;
    let width = parse_image_dimension(width, "width")?;
    let height = parse_image_dimension(height, "height")?;
    Ok((width, height))
}

fn parse_image_dimension(value: &str, label: &str) -> Result<u32> {
    let dimension = value
        .trim()
        .parse::<u32>()
        .with_context(|| format!("image {label} must be a positive integer"))?;
    if !(MIN_IMAGE_DIMENSION..=MAX_IMAGE_DIMENSION).contains(&dimension) {
        bail!(
            "image {label} must be between {MIN_IMAGE_DIMENSION} and {MAX_IMAGE_DIMENSION} pixels"
        );
    }
    Ok(dimension)
}

fn image_prompt_message_content(
    mode: &str,
    prompt: &str,
    model: &str,
    n: usize,
    size: &str,
) -> Value {
    json!({
        "blocks": [{
            "type": "text",
            "text": prompt,
        }],
        "kind": if mode == "edit" { "image_edit" } else { "image_generation" },
        "model": model,
        "n": n,
        "size": size,
    })
}

fn image_task_request_json(
    mode: &str,
    prompt: &str,
    model: &str,
    n: usize,
    edit_input: Option<&ImageEditInput>,
    image_context: &ImagePromptContext,
    request_context: &RequestLogContext,
) -> Value {
    let mut payload = json!({
        "mode": mode,
        "prompt": prompt,
        "effective_prompt": &image_context.effective_prompt,
        "model": model,
        "n": n,
        "size": image_context.size,
        "billing": {
            "billable_credits": image_context.billable_credits,
            "size_credit_units": image_context.size_credit_units,
            "context_text_count": image_context.context_text_count,
            "context_image_count": image_context.context_image_count,
            "context_credit_surcharge": image_context.context_credit_surcharge,
        },
        "request": request_context,
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

fn usage_context_for_image_task(task: &ImageTaskRecord, endpoint: &str) -> UsageEventContext {
    let payload = serde_json::from_str::<Value>(&task.request_json).unwrap_or(Value::Null);
    let billing = payload.get("billing").and_then(Value::as_object);
    let request = payload
        .get("request")
        .and_then(|value| serde_json::from_value::<RequestLogContext>(value.clone()).ok())
        .unwrap_or_else(|| RequestLogContext::internal(endpoint));
    let effective_prompt =
        payload.get("effective_prompt").and_then(Value::as_str).unwrap_or(&task.prompt).to_string();
    let image_size = payload
        .get("size")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| Some(DEFAULT_IMAGE_SIZE.to_string()));
    let size_credit_units = billing
        .and_then(|value| value.get("size_credit_units"))
        .and_then(Value::as_i64)
        .or_else(|| image_size.as_deref().map(image_size_credit_units))
        .unwrap_or(1);
    let billable_credits = billing
        .and_then(|value| value.get("billable_credits"))
        .and_then(Value::as_i64)
        .unwrap_or_else(|| task.n.max(1) * size_credit_units);
    let context_text_count = billing
        .and_then(|value| value.get("context_text_count"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let context_image_count = billing
        .and_then(|value| value.get("context_image_count"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let context_credit_surcharge = billing
        .and_then(|value| value.get("context_credit_surcharge"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    UsageEventContext {
        request,
        endpoint: endpoint.to_string(),
        session_id: Some(task.session_id.clone()),
        task_id: Some(task.id.clone()),
        mode: task.mode.clone(),
        image_size,
        size_credit_units,
        billable_credits,
        context_text_count,
        context_image_count,
        context_credit_surcharge,
        raw_prompt: task.prompt.clone(),
        effective_prompt,
    }
}

fn direct_usage_context(endpoint: &str, mode: &str, prompt: &str) -> UsageEventContext {
    let is_image_mode = matches!(mode, "generation" | "edit");
    UsageEventContext {
        request: RequestLogContext::internal(endpoint),
        endpoint: endpoint.to_string(),
        session_id: None,
        task_id: None,
        mode: mode.to_string(),
        image_size: is_image_mode.then(|| DEFAULT_IMAGE_SIZE.to_string()),
        size_credit_units: i64::from(is_image_mode),
        billable_credits: 1,
        context_text_count: 0,
        context_image_count: 0,
        context_credit_surcharge: 0,
        raw_prompt: prompt.to_string(),
        effective_prompt: prompt.to_string(),
    }
}

fn prompt_preview(prompt: &str) -> Option<String> {
    let preview = compact_context_text(prompt, 240);
    if preview.is_empty() {
        None
    } else {
        Some(preview)
    }
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
        models::{
            AccountProxyMode, AccountRecord, BrowserProfile, ImageArtifactRecord, ImageTaskStatus,
            ProxyConfigRecord,
        },
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

    #[test]
    fn image_size_credit_units_use_area_formula() {
        assert_eq!(normalize_image_size("auto").expect("auto size"), DEFAULT_IMAGE_SIZE);
        assert_eq!(normalize_image_size("1536X1024").expect("custom size"), "1536x1024");
        assert_eq!(image_size_credit_units("1024x1024"), 1);
        assert_eq!(image_size_credit_units("1024x1536"), 2);
        assert_eq!(image_size_credit_units("1536x1024"), 2);
        assert_eq!(image_size_credit_units("2048x2048"), 4);
        assert!(normalize_image_size("128x1024").is_err());
        assert!(normalize_image_size("4097x1024").is_err());
    }

    #[test]
    fn session_image_context_ignores_failed_assistant_text() {
        let detail = SessionDetail {
            session: SessionRecord {
                id: "sess".to_string(),
                key_id: "key".to_string(),
                title: "title".to_string(),
                source: SessionSource::Web,
                status: "active".to_string(),
                created_at: 1,
                updated_at: 1,
                last_message_at: Some(1),
            },
            messages: vec![
                MessageRecord {
                    id: "user".to_string(),
                    session_id: "sess".to_string(),
                    key_id: "key".to_string(),
                    role: "user".to_string(),
                    content_json: text_message_content("深圳 美女").to_string(),
                    status: MessageStatus::Done,
                    created_at: 1,
                    updated_at: 1,
                },
                MessageRecord {
                    id: "assistant".to_string(),
                    session_id: "sess".to_string(),
                    key_id: "key".to_string(),
                    role: "assistant".to_string(),
                    content_json: json!({
                        "blocks": [{ "type": "error", "text": "duplicated upstream text" }]
                    })
                    .to_string(),
                    status: MessageStatus::Failed,
                    created_at: 2,
                    updated_at: 2,
                },
            ],
            tasks: vec![ImageTaskRecord {
                id: "task".to_string(),
                session_id: "sess".to_string(),
                message_id: "assistant-done".to_string(),
                key_id: "key".to_string(),
                status: ImageTaskStatus::Succeeded,
                mode: "generation".to_string(),
                prompt: "写实风格".to_string(),
                model: "gpt-image-2".to_string(),
                n: 1,
                request_json: "{}".to_string(),
                phase: "done".to_string(),
                queue_entered_at: 1,
                started_at: Some(1),
                finished_at: Some(2),
                position_snapshot: Some(0),
                estimated_start_after_ms: None,
                error_code: None,
                error_message: None,
            }],
            artifacts: vec![ImageArtifactRecord {
                id: "artifact".to_string(),
                task_id: "task".to_string(),
                session_id: "sess".to_string(),
                message_id: "assistant-done".to_string(),
                key_id: "key".to_string(),
                relative_path: "artifacts/image.png".to_string(),
                mime_type: "image/png".to_string(),
                sha256: "hash".to_string(),
                size_bytes: 1,
                width: Some(1024),
                height: Some(1024),
                revised_prompt: Some(
                    "Continue this image-generation session. Prior text context: duplicated upstream text"
                        .to_string(),
                ),
                created_at: 3,
            }],
        };

        let context = build_session_aware_image_prompt(&detail, "写实风格", 1, DEFAULT_IMAGE_SIZE);

        assert!(context.effective_prompt.contains("user: 深圳 美女"));
        assert!(context.effective_prompt.contains("source prompt: 写实风格"));
        assert!(!context.effective_prompt.contains("duplicated upstream text"));
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
