//! Import parsing helpers for token, session JSON, and CPA JSON.

use anyhow::{anyhow, Result};
use serde_json::Value;
use sha1::{Digest, Sha1};

/// Extracts the `accessToken` field from a copied session JSON blob.
pub fn extract_session_access_token(raw: &str) -> Result<String> {
    let value: Value = serde_json::from_str(raw)?;
    value
        .get("accessToken")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("session JSON missing accessToken"))
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

/// Derives a stable synthetic account name from an imported access token.
#[must_use]
pub fn derive_account_name(access_token: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(access_token.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!("acct_{}", &digest[..12])
}
