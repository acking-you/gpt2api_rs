//! Shared domain models used across the service.

use serde::{Deserialize, Serialize};

/// Supported sources for imported ChatGPT access credentials.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AccountSourceKind {
    /// Raw `access_token` pasted by an operator.
    Token,
    /// JSON copied from `https://chatgpt.com/api/auth/session`.
    SessionJson,
    /// CPA JSON containing an access token field.
    CpaJson,
}

impl AccountSourceKind {
    /// Returns the stable storage representation used in SQLite rows.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Token => "token",
            Self::SessionJson => "session_json",
            Self::CpaJson => "cpa_json",
        }
    }
}

/// Account-level upstream proxy selection mode.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccountProxyMode {
    /// Reuse the service-wide default upstream proxy.
    #[default]
    Inherit,
    /// Force the account to connect without any proxy.
    Direct,
    /// Force the account to use one stored proxy config.
    Fixed,
}

impl AccountProxyMode {
    /// Returns the stable storage representation used in SQLite rows.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::Direct => "direct",
            Self::Fixed => "fixed",
        }
    }

    /// Parses one stable storage string into an account proxy mode.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "inherit" => Some(Self::Inherit),
            "direct" => Some(Self::Direct),
            "fixed" => Some(Self::Fixed),
            _ => None,
        }
    }
}

/// Persisted account state tracked in the control database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountRecord {
    /// Stable account identifier used by admin APIs and routing.
    pub name: String,
    /// Imported ChatGPT access token.
    pub access_token: String,
    /// Original import source for the current credential.
    pub source_kind: AccountSourceKind,
    /// Known ChatGPT email, if already refreshed.
    pub email: Option<String>,
    /// Known upstream user id, if already refreshed.
    pub user_id: Option<String>,
    /// Last observed plan type.
    pub plan_type: Option<String>,
    /// Last observed default upstream model slug.
    pub default_model_slug: Option<String>,
    /// Current local account status.
    pub status: String,
    /// Remaining image quota as last observed locally.
    pub quota_remaining: i64,
    /// Whether the upstream currently exposes an explicit remaining image quota.
    pub quota_known: bool,
    /// Optional upstream restore timestamp string.
    pub restore_at: Option<String>,
    /// Last refresh epoch seconds.
    pub last_refresh_at: Option<i64>,
    /// Last successful use epoch seconds.
    pub last_used_at: Option<i64>,
    /// Last recorded error summary.
    pub last_error: Option<String>,
    /// Count of successful routed requests.
    pub success_count: i64,
    /// Count of failed routed requests.
    pub fail_count: i64,
    /// Optional account-level concurrency cap.
    pub request_max_concurrency: Option<u64>,
    /// Optional account-level minimum start interval in milliseconds.
    pub request_min_start_interval_ms: Option<u64>,
    /// Account-level upstream proxy selection mode.
    pub proxy_mode: AccountProxyMode,
    /// Optional bound proxy config id when `proxy_mode = fixed`.
    pub proxy_config_id: Option<String>,
    /// Serialized browser impersonation profile JSON.
    pub browser_profile_json: String,
}

/// Persisted browser/session hints used for upstream ChatGPT Web calls.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrowserProfile {
    /// Optional ChatGPT web session cookie value.
    pub session_token: Option<String>,
    /// Optional preferred user agent.
    pub user_agent: Option<String>,
    /// Optional impersonated browser family such as `edge` or `chrome`.
    pub impersonate_browser: Option<String>,
    /// Optional persisted OpenAI device id.
    pub oai_device_id: Option<String>,
    /// Optional client hint header.
    pub sec_ch_ua: Option<String>,
    /// Optional mobile client hint header.
    pub sec_ch_ua_mobile: Option<String>,
    /// Optional platform client hint header.
    pub sec_ch_ua_platform: Option<String>,
}

impl AccountRecord {
    /// Builds a minimal active account record for tests and bootstrap paths.
    #[must_use]
    pub fn minimal(name: &str, access_token: &str) -> Self {
        Self {
            name: name.to_string(),
            access_token: access_token.to_string(),
            source_kind: AccountSourceKind::Token,
            email: None,
            user_id: None,
            plan_type: None,
            default_model_slug: None,
            status: "active".to_string(),
            quota_remaining: 0,
            quota_known: false,
            restore_at: None,
            last_refresh_at: None,
            last_used_at: None,
            last_error: None,
            success_count: 0,
            fail_count: 0,
            request_max_concurrency: None,
            request_min_start_interval_ms: None,
            proxy_mode: AccountProxyMode::Inherit,
            proxy_config_id: None,
            browser_profile_json: "{}".to_string(),
        }
    }

    /// Deserializes the persisted browser profile JSON, returning defaults on invalid data.
    #[must_use]
    pub fn browser_profile(&self) -> BrowserProfile {
        serde_json::from_str(&self.browser_profile_json).unwrap_or_default()
    }
}

/// Persisted reusable upstream proxy configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyConfigRecord {
    /// Stable proxy config identifier.
    pub id: String,
    /// Human-readable proxy config label.
    pub name: String,
    /// Proxy URL including scheme and host.
    pub proxy_url: String,
    /// Optional proxy username.
    pub proxy_username: Option<String>,
    /// Optional proxy password.
    pub proxy_password: Option<String>,
    /// Current proxy config status.
    pub status: String,
    /// Creation time as unix epoch seconds.
    pub created_at: i64,
    /// Last update time as unix epoch seconds.
    pub updated_at: i64,
}

/// Product role assigned to a downstream API key.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiKeyRole {
    /// Normal user scoped to their own sessions and artifacts.
    User,
    /// Product administrator with cross-key visibility and queue controls.
    Admin,
}

impl ApiKeyRole {
    /// Returns the stable SQLite representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Admin => "admin",
        }
    }

    /// Parses the stable SQLite representation.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "user" => Some(Self::User),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }
}

/// Durable session source.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionSource {
    /// Created by the standalone web UI.
    Web,
    /// Created by OpenAI-compatible API calls.
    Api,
}

impl SessionSource {
    /// Returns the stable SQLite representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Web => "web",
            Self::Api => "api",
        }
    }

    /// Parses the stable SQLite representation.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "web" => Some(Self::Web),
            "api" => Some(Self::Api),
            _ => None,
        }
    }
}

/// Persisted session row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRecord {
    /// Stable session id.
    pub id: String,
    /// Owning API-key id.
    pub key_id: String,
    /// Human-readable title.
    pub title: String,
    /// Session source.
    pub source: SessionSource,
    /// Active or archived.
    pub status: String,
    /// Creation epoch seconds.
    pub created_at: i64,
    /// Update epoch seconds.
    pub updated_at: i64,
    /// Last message epoch seconds.
    pub last_message_at: Option<i64>,
}

/// Complete session detail returned by product APIs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionDetail {
    /// Session metadata.
    pub session: SessionRecord,
    /// Ordered persisted messages.
    pub messages: Vec<MessageRecord>,
    /// Image tasks associated with the session.
    pub tasks: Vec<ImageTaskRecord>,
    /// Image artifacts associated with the session.
    pub artifacts: Vec<ImageArtifactRecord>,
}

/// Message lifecycle status.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageStatus {
    /// Created but no assistant content yet.
    Pending,
    /// Text stream is currently being forwarded.
    Streaming,
    /// Message is complete.
    Done,
    /// Message failed.
    Failed,
}

impl MessageStatus {
    /// Returns the stable SQLite representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Streaming => "streaming",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    /// Parses the stable SQLite representation.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pending" => Some(Self::Pending),
            "streaming" => Some(Self::Streaming),
            "done" => Some(Self::Done),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Persisted message row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageRecord {
    /// Stable message id.
    pub id: String,
    /// Parent session id.
    pub session_id: String,
    /// Owning API-key id.
    pub key_id: String,
    /// Conversation role.
    pub role: String,
    /// Structured content JSON string.
    pub content_json: String,
    /// Message status.
    pub status: MessageStatus,
    /// Creation epoch seconds.
    pub created_at: i64,
    /// Update epoch seconds.
    pub updated_at: i64,
}

/// Image task lifecycle status.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImageTaskStatus {
    /// Waiting in the global queue.
    Queued,
    /// Claimed by a worker.
    Running,
    /// Artifacts were written successfully.
    Succeeded,
    /// Upstream or local processing failed.
    Failed,
    /// Cancelled before execution.
    Cancelled,
}

impl ImageTaskStatus {
    /// Returns the stable SQLite representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Parses the stable SQLite representation.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Persisted image task row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageTaskRecord {
    /// Stable task id.
    pub id: String,
    /// Parent session id.
    pub session_id: String,
    /// Assistant message id updated by this task.
    pub message_id: String,
    /// Owning API-key id.
    pub key_id: String,
    /// Task status.
    pub status: ImageTaskStatus,
    /// Generation or edit mode.
    pub mode: String,
    /// Original user prompt.
    pub prompt: String,
    /// Requested model.
    pub model: String,
    /// Requested image count.
    pub n: i64,
    /// Original request JSON.
    pub request_json: String,
    /// UI phase such as queued, allocating, running, saving, done, failed.
    pub phase: String,
    /// Queue entry epoch seconds.
    pub queue_entered_at: i64,
    /// Start epoch seconds.
    pub started_at: Option<i64>,
    /// Finish epoch seconds.
    pub finished_at: Option<i64>,
    /// Last known number of queued tasks ahead.
    pub position_snapshot: Option<i64>,
    /// Approximate wait in milliseconds.
    pub estimated_start_after_ms: Option<i64>,
    /// Stable error code.
    pub error_code: Option<String>,
    /// Human-readable error summary.
    pub error_message: Option<String>,
}

/// Queue snapshot for one image task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueSnapshot {
    /// Current task row.
    pub task: ImageTaskRecord,
    /// Number of queued tasks that should run before this one.
    pub position_ahead: i64,
    /// Approximate start wait in milliseconds.
    pub estimated_start_after_ms: Option<i64>,
}

/// Result returned after queuing one image message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageSubmissionResult {
    /// Persisted user prompt message.
    pub user_message: MessageRecord,
    /// Pending assistant message that will receive artifacts.
    pub assistant_message: MessageRecord,
    /// Queued image task.
    pub task: ImageTaskRecord,
    /// Current queue position snapshot.
    pub queue: QueueSnapshot,
}

/// Admin-visible queue snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminQueueSnapshot {
    /// Running task rows.
    pub running: Vec<ImageTaskRecord>,
    /// Queued task rows.
    pub queued: Vec<ImageTaskRecord>,
    /// Global image concurrency setting.
    pub global_image_concurrency: i64,
}

/// Generated image artifact metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageArtifactRecord {
    /// Stable artifact id.
    pub id: String,
    /// Parent task id.
    pub task_id: String,
    /// Parent session id.
    pub session_id: String,
    /// Parent message id.
    pub message_id: String,
    /// Owning API-key id.
    pub key_id: String,
    /// Relative filesystem path under the service root.
    pub relative_path: String,
    /// MIME type.
    pub mime_type: String,
    /// Hex SHA-256 of the file.
    pub sha256: String,
    /// File size in bytes.
    pub size_bytes: i64,
    /// Optional image width.
    pub width: Option<i64>,
    /// Optional image height.
    pub height: Option<i64>,
    /// Upstream revised prompt.
    pub revised_prompt: Option<String>,
    /// Creation epoch seconds.
    pub created_at: i64,
}

/// Persisted task event row used by session progress streams.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskEventRecord {
    /// Monotonic SQLite row sequence for polling.
    pub sequence: i64,
    /// Stable event id.
    pub id: String,
    /// Parent task id.
    pub task_id: String,
    /// Parent session id.
    pub session_id: String,
    /// Owning API-key id.
    pub key_id: String,
    /// Stable event kind.
    pub event_kind: String,
    /// Structured event payload JSON string.
    pub payload_json: String,
    /// Creation epoch seconds.
    pub created_at: i64,
}

/// Runtime configuration stored in SQLite.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeConfigRecord {
    /// Minimum account refresh interval.
    pub refresh_min_seconds: i64,
    /// Maximum account refresh interval.
    pub refresh_max_seconds: i64,
    /// Refresh jitter.
    pub refresh_jitter_seconds: i64,
    /// Default per-key/account concurrency.
    pub default_request_max_concurrency: i64,
    /// Default start interval in milliseconds.
    pub default_request_min_start_interval_ms: i64,
    /// Usage outbox flush batch size.
    pub event_flush_batch_size: i64,
    /// Usage outbox flush interval.
    pub event_flush_interval_seconds: i64,
    /// Global image task concurrency.
    pub global_image_concurrency: i64,
    /// Signed link TTL in seconds.
    pub signed_link_ttl_seconds: i64,
    /// ETA averaging window size.
    pub queue_eta_window_size: i64,
    /// Maximum runtime for one claimed image task.
    pub image_task_timeout_seconds: i64,
}

/// A signed link row with only hashed token persisted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedLinkRecord {
    /// Stable signed link id.
    pub id: String,
    /// Hashed plaintext token.
    pub token_hash: String,
    /// Link scope such as image_task or session.
    pub scope: String,
    /// Scoped record id.
    pub scope_id: String,
    /// Expiration epoch seconds.
    pub expires_at: i64,
    /// Revocation epoch seconds.
    pub revoked_at: Option<i64>,
    /// Creation epoch seconds.
    pub created_at: i64,
    /// Last observed use epoch seconds.
    pub used_at: Option<i64>,
}

/// Created signed link including the one-time plaintext token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreatedSignedLink {
    /// Persisted signed link row.
    #[serde(flatten)]
    pub record: SignedLinkRecord,
    /// Plaintext token to embed in the outgoing URL.
    pub plaintext_token: String,
}

impl std::ops::Deref for CreatedSignedLink {
    type Target = SignedLinkRecord;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

/// Persisted downstream API-key record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    /// Stable API key identifier.
    pub id: String,
    /// Human-readable key label.
    pub name: String,
    /// Stored secret hash for the downstream key.
    pub secret_hash: String,
    /// Stored plaintext secret for admin visibility and direct reuse.
    #[serde(default)]
    pub secret_plaintext: Option<String>,
    /// Current key status.
    pub status: String,
    /// Maximum billable call quota assigned to the key.
    #[serde(alias = "quota_total_images")]
    pub quota_total_calls: i64,
    /// Already consumed billable calls.
    #[serde(alias = "quota_used_images")]
    pub quota_used_calls: i64,
    /// Route strategy string stored in SQLite.
    pub route_strategy: String,
    /// Optional bound account-group id.
    pub account_group_id: Option<String>,
    /// Optional directly bound upstream account name for fixed routing.
    pub fixed_account_name: Option<String>,
    /// Optional per-key concurrency cap.
    pub request_max_concurrency: Option<u64>,
    /// Optional per-key minimum start interval in milliseconds.
    pub request_min_start_interval_ms: Option<u64>,
    /// Product role.
    pub role: ApiKeyRole,
    /// Optional default notification email.
    pub notification_email: Option<String>,
    /// Whether completion email notifications are enabled.
    pub notification_enabled: bool,
}

/// Reusable account group used by downstream API-key routing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountGroupRecord {
    /// Stable account-group identifier.
    pub id: String,
    /// Human-readable group label.
    pub name: String,
    /// Member upstream account names.
    #[serde(default)]
    pub account_names: Vec<String>,
}

impl ApiKeyRecord {
    /// Builds a minimal active API-key record for tests.
    #[must_use]
    pub fn minimal(id: &str, name: &str, quota_total_calls: i64) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            secret_hash: "hash".to_string(),
            secret_plaintext: None,
            status: "active".to_string(),
            quota_total_calls,
            quota_used_calls: 0,
            route_strategy: "auto".to_string(),
            account_group_id: None,
            fixed_account_name: None,
            request_max_concurrency: None,
            request_min_start_interval_ms: None,
            role: ApiKeyRole::User,
            notification_email: None,
            notification_enabled: false,
        }
    }
}

/// Route policy used by downstream API keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteStrategy {
    /// Pick the best currently available account from a candidate set.
    Auto,
    /// Always route to the first configured candidate.
    Fixed,
}

/// Lightweight routing candidate state used during account selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRouteCandidate {
    /// Candidate account name.
    pub name: String,
    /// Remaining local image quota for the account.
    pub quota_remaining: i64,
    /// Whether the remaining quota is explicitly known.
    pub quota_known: bool,
    /// Monotonic last-routed timestamp in milliseconds.
    pub last_routed_at_ms: i64,
}

impl AccountRouteCandidate {
    /// Builds a new routing candidate for tests and selection logic.
    #[must_use]
    pub fn new(
        name: &str,
        quota_remaining: i64,
        quota_known: bool,
        last_routed_at_ms: i64,
    ) -> Self {
        Self { name: name.to_string(), quota_remaining, quota_known, last_routed_at_ms }
    }
}

/// Refreshed upstream metadata for one imported account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountMetadata {
    /// Known ChatGPT email.
    pub email: Option<String>,
    /// Known upstream user id.
    pub user_id: Option<String>,
    /// Observed plan type if available.
    pub plan_type: Option<String>,
    /// Default model slug returned by conversation init.
    pub default_model_slug: Option<String>,
    /// Remaining image quota from the latest refresh.
    pub quota_remaining: i64,
    /// Whether the upstream explicitly exposed the remaining image quota.
    pub quota_known: bool,
    /// Reset timestamp string for the image quota window.
    pub restore_at: Option<String>,
}

/// One usage-event summary written to the outbox and later flushed into DuckDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageEventRecord {
    /// Stable event identifier.
    pub event_id: String,
    /// Request-scoped correlation identifier.
    pub request_id: String,
    /// Downstream API-key id.
    pub key_id: String,
    /// Downstream API-key display name.
    pub key_name: String,
    /// Selected upstream account name.
    pub account_name: String,
    /// Public endpoint that received the request.
    pub endpoint: String,
    /// Downstream HTTP method.
    #[serde(default)]
    pub request_method: String,
    /// Downstream request URL path.
    #[serde(default)]
    pub request_url: String,
    /// Requested external model name.
    pub requested_model: String,
    /// Resolved upstream model slug.
    pub resolved_upstream_model: String,
    /// Product/API session id associated with this event.
    #[serde(default)]
    pub session_id: Option<String>,
    /// Image task id associated with this event.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Request mode, for example generation, edit, or text.
    #[serde(default)]
    pub mode: String,
    /// Requested image size such as 1024x1024.
    #[serde(default)]
    pub image_size: Option<String>,
    /// Requested image count.
    pub requested_n: i64,
    /// Generated image count.
    pub generated_n: i64,
    /// Billable image count kept for OpenAI-compatible image semantics.
    pub billable_images: i64,
    /// Billable credit units charged to the downstream key.
    #[serde(default)]
    pub billable_credits: i64,
    /// Per-image credit units derived from requested size.
    #[serde(default)]
    pub size_credit_units: i64,
    /// Number of prior text snippets injected into image context.
    #[serde(default)]
    pub context_text_count: i64,
    /// Number of prior generated images summarized into image context.
    #[serde(default)]
    pub context_image_count: i64,
    /// Extra credit units charged because session context was used.
    #[serde(default)]
    pub context_credit_surcharge: i64,
    /// Client IP derived from trusted proxy headers or direct metadata.
    #[serde(default)]
    pub client_ip: String,
    /// Sanitized request headers JSON.
    #[serde(default)]
    pub request_headers_json: Option<String>,
    /// Prompt preview for search/debugging.
    #[serde(default)]
    pub prompt_preview: Option<String>,
    /// Last user-visible message or prompt extracted from the request.
    #[serde(default)]
    pub last_message_content: Option<String>,
    /// Full request body retained only for failed requests.
    #[serde(default)]
    pub request_body_json: Option<String>,
    /// Raw user prompt length.
    #[serde(default)]
    pub prompt_chars: i64,
    /// Effective upstream prompt length after session context injection.
    #[serde(default)]
    pub effective_prompt_chars: i64,
    /// Final HTTP status code exposed to the caller.
    pub status_code: i64,
    /// End-to-end latency in milliseconds.
    pub latency_ms: i64,
    /// Optional normalized error code.
    pub error_code: Option<String>,
    /// Optional error message summary.
    pub error_message: Option<String>,
    /// Optional sidecar detail path reference.
    pub detail_ref: Option<String>,
    /// Event creation timestamp.
    pub created_at: i64,
}

impl UsageEventRecord {
    /// Builds a minimal success event used in tests and initial settlement flows.
    #[must_use]
    pub fn success(
        event_id: &str,
        request_id: &str,
        key_id: &str,
        key_name: &str,
        account_name: &str,
        billable_images: i64,
    ) -> Self {
        Self {
            event_id: event_id.to_string(),
            request_id: request_id.to_string(),
            key_id: key_id.to_string(),
            key_name: key_name.to_string(),
            account_name: account_name.to_string(),
            endpoint: "/v1/images/generations".to_string(),
            request_method: "POST".to_string(),
            request_url: "/v1/images/generations".to_string(),
            requested_model: "gpt-image-1".to_string(),
            resolved_upstream_model: "auto".to_string(),
            session_id: None,
            task_id: None,
            mode: "generation".to_string(),
            image_size: Some("1024x1024".to_string()),
            requested_n: billable_images,
            generated_n: billable_images,
            billable_images,
            billable_credits: billable_images,
            size_credit_units: 1,
            context_text_count: 0,
            context_image_count: 0,
            context_credit_surcharge: 0,
            client_ip: String::new(),
            request_headers_json: None,
            prompt_preview: None,
            last_message_content: None,
            request_body_json: None,
            prompt_chars: 0,
            effective_prompt_chars: 0,
            status_code: 200,
            latency_ms: 0,
            error_code: None,
            error_message: None,
            detail_ref: None,
            created_at: 0,
        }
    }
}
