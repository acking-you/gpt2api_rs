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
    /// Monotonic last-routed timestamp in milliseconds.
    pub last_routed_at_ms: i64,
}

impl AccountRouteCandidate {
    /// Builds a new routing candidate for tests and selection logic.
    #[must_use]
    pub fn new(name: &str, quota_remaining: i64, last_routed_at_ms: i64) -> Self {
        Self { name: name.to_string(), quota_remaining, last_routed_at_ms }
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
    /// Reset timestamp string for the image quota window.
    pub restore_at: Option<String>,
}
