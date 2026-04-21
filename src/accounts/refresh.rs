//! Upstream metadata refresh for imported accounts.

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde_json::Value;

use crate::models::AccountMetadata;

/// Fetches account metadata from the upstream ChatGPT web backend.
pub async fn fetch_account_metadata(
    base_url: String,
    access_token: &str,
) -> Result<AccountMetadata> {
    let client = Client::new();
    let me: Value = client
        .get(format!("{base_url}/backend-api/me"))
        .bearer_auth(access_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let init: Value = client
        .post(format!("{base_url}/backend-api/conversation/init"))
        .bearer_auth(access_token)
        .json(&serde_json::json!({
            "gizmo_id": null,
            "requested_default_model": null,
            "conversation_id": null,
            "timezone_offset_min": 0
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let image_limit = init
        .get("limits_progress")
        .and_then(Value::as_array)
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.get("feature_name").and_then(Value::as_str) == Some("image_gen"))
        })
        .ok_or_else(|| anyhow!("missing image_gen limits_progress entry"))?;

    Ok(AccountMetadata {
        email: me.get("email").and_then(Value::as_str).map(ToString::to_string),
        user_id: me.get("id").and_then(Value::as_str).map(ToString::to_string),
        plan_type: None,
        default_model_slug: init
            .get("default_model_slug")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        quota_remaining: image_limit.get("remaining").and_then(Value::as_i64).unwrap_or(0),
        restore_at: image_limit.get("reset_after").and_then(Value::as_str).map(ToString::to_string),
    })
}
