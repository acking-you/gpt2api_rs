//! Import parsing helpers for token, session JSON, and CPA JSON.

use anyhow::{anyhow, Result};
use serde_json::Value;
use sha1::{Digest, Sha1};

use crate::models::{AccountRecord, AccountSourceKind, BrowserProfile};

/// Normalized import seed extracted from one operator-supplied payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedAccountSeed {
    /// Stable account name derived from the access token.
    pub name: String,
    /// Imported upstream access token.
    pub access_token: String,
    /// Original import source kind.
    pub source_kind: AccountSourceKind,
    /// Persisted browser/session hints.
    pub browser_profile: BrowserProfile,
}

/// Extracts the `accessToken` field from a copied session JSON blob.
pub fn extract_session_access_token(raw: &str) -> Result<String> {
    Ok(parse_session_seed(raw)?.access_token)
}

/// Extracts the CPA access token from either supported field alias.
pub fn extract_cpa_access_token(raw: &str) -> Result<String> {
    let value: Value = serde_json::from_str(raw)?;
    value
        .get("access_token")
        .or_else(|| value.get("accessToken"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("CPA JSON missing access_token/accessToken"))
}

/// Parses one raw access token into a normalized account seed.
pub fn parse_access_token_seed(raw: &str) -> Result<ImportedAccountSeed> {
    let access_token = raw.trim();
    if access_token.is_empty() {
        return Err(anyhow!("access token is empty"));
    }
    Ok(ImportedAccountSeed {
        name: derive_account_name(access_token),
        access_token: access_token.to_string(),
        source_kind: AccountSourceKind::Token,
        browser_profile: BrowserProfile::default(),
    })
}

/// Parses one copied ChatGPT session JSON blob into a normalized account seed.
pub fn parse_session_seed(raw: &str) -> Result<ImportedAccountSeed> {
    let value: Value = serde_json::from_str(raw)?;
    let access_token = value
        .get("accessToken")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("session JSON missing accessToken"))?;
    let session_token = value
        .get("sessionToken")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let browser_profile = BrowserProfile {
        session_token,
        user_agent: None,
        impersonate_browser: Some("edge".to_string()),
        oai_device_id: None,
        sec_ch_ua: None,
        sec_ch_ua_mobile: None,
        sec_ch_ua_platform: None,
    };
    Ok(ImportedAccountSeed {
        name: derive_account_name(access_token),
        access_token: access_token.to_string(),
        source_kind: AccountSourceKind::SessionJson,
        browser_profile,
    })
}

/// Converts one normalized import seed into an account record with active defaults.
#[must_use]
pub fn build_account_record(seed: ImportedAccountSeed) -> AccountRecord {
    AccountRecord {
        name: seed.name,
        access_token: seed.access_token,
        source_kind: seed.source_kind,
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
        browser_profile_json: serde_json::to_string(&seed.browser_profile)
            .expect("browser profile serialization should not fail"),
    }
}

/// Derives a stable synthetic account name from an imported access token.
#[must_use]
pub fn derive_account_name(access_token: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(access_token.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!("acct_{}", &digest[..12])
}
