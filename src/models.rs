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
            browser_profile_json: "{}".to_string(),
        }
    }

    /// Deserializes the persisted browser profile JSON, returning defaults on invalid data.
    #[must_use]
    pub fn browser_profile(&self) -> BrowserProfile {
        serde_json::from_str(&self.browser_profile_json).unwrap_or_default()
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
    /// Current key status.
    pub status: String,
    /// Maximum billable image quota assigned to the key.
    pub quota_total_images: i64,
    /// Already consumed billable images.
    pub quota_used_images: i64,
    /// Route strategy string stored in SQLite.
    pub route_strategy: String,
    /// Optional bound account-group id.
    pub account_group_id: Option<String>,
    /// Optional per-key concurrency cap.
    pub request_max_concurrency: Option<u64>,
    /// Optional per-key minimum start interval in milliseconds.
    pub request_min_start_interval_ms: Option<u64>,
}

impl ApiKeyRecord {
    /// Builds a minimal active API-key record for tests.
    #[must_use]
    pub fn minimal(id: &str, name: &str, quota_total_images: i64) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            secret_hash: "hash".to_string(),
            status: "active".to_string(),
            quota_total_images,
            quota_used_images: 0,
            route_strategy: "auto".to_string(),
            account_group_id: None,
            request_max_concurrency: None,
            request_min_start_interval_ms: None,
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
    /// Requested external model name.
    pub requested_model: String,
    /// Resolved upstream model slug.
    pub resolved_upstream_model: String,
    /// Requested image count.
    pub requested_n: i64,
    /// Generated image count.
    pub generated_n: i64,
    /// Billable image count.
    pub billable_images: i64,
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
            requested_model: "gpt-image-1".to_string(),
            resolved_upstream_model: "auto".to_string(),
            requested_n: billable_images,
            generated_n: billable_images,
            billable_images,
            status_code: 200,
            latency_ms: 0,
            error_code: None,
            error_message: None,
            detail_ref: None,
            created_at: 0,
        }
    }
}
