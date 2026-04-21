//! Lightweight REST client used by the local admin CLI.

use anyhow::{Context, Result};
use reqwest::Client;

use crate::models::{AccountRecord, ApiKeyRecord, UsageEventRecord};

/// Lists imported accounts from the running service.
pub async fn list_accounts(base_url: &str, admin_token: &str) -> Result<Vec<AccountRecord>> {
    let url = format!("{}/admin/accounts", base_url.trim_end_matches('/'));
    fetch_json(&url, admin_token).await
}

/// Lists configured downstream API keys from the running service.
pub async fn list_keys(base_url: &str, admin_token: &str) -> Result<Vec<ApiKeyRecord>> {
    let url = format!("{}/admin/keys", base_url.trim_end_matches('/'));
    fetch_json(&url, admin_token).await
}

/// Lists recent usage events from the running service.
pub async fn list_usage(
    base_url: &str,
    admin_token: &str,
    limit: u64,
) -> Result<Vec<UsageEventRecord>> {
    let url = format!("{}/admin/usage?limit={limit}", base_url.trim_end_matches('/'));
    fetch_json(&url, admin_token).await
}

async fn fetch_json<T>(url: &str, admin_token: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    Client::new()
        .get(url)
        .bearer_auth(admin_token)
        .send()
        .await
        .with_context(|| format!("request failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("non-success response: {url}"))?
        .json::<T>()
        .await
        .with_context(|| format!("invalid JSON payload: {url}"))
}
