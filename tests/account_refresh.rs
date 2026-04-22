//! Account metadata refresh and status-cache tests.

use std::time::Duration;

use gpt2api_rs::accounts::refresh::fetch_account_metadata;
use gpt2api_rs::accounts::status_cache::{next_refresh_delay, spawn_status_refresher};
use tokio::sync::watch;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

/// Reads metadata from `/backend-api/me` and `/backend-api/conversation/init`.
#[tokio::test]
async fn refresh_reads_me_and_conversation_init() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/backend-api/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "user_123",
            "email": "demo@example.com"
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/backend-api/conversation/init"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "default_model_slug": "gpt-4o",
            "limits_progress": [
                {
                    "feature_name": "image_gen",
                    "remaining": 12,
                    "reset_after": "2026-04-22T00:00:00Z"
                }
            ]
        })))
        .mount(&server)
        .await;

    let meta = fetch_account_metadata(server.uri(), "token_123").await.expect("metadata fetch");
    assert_eq!(meta.user_id.as_deref(), Some("user_123"));
    assert_eq!(meta.email.as_deref(), Some("demo@example.com"));
    assert_eq!(meta.default_model_slug.as_deref(), Some("gpt-4o"));
    assert_eq!(meta.quota_remaining, 12);
    assert!(meta.quota_known);
    assert_eq!(meta.restore_at.as_deref(), Some("2026-04-22T00:00:00Z"));
}

/// Leaves quota unknown when the upstream omits image_gen from the limits payload.
#[tokio::test]
async fn refresh_accepts_missing_image_quota() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/backend-api/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "user_123",
            "email": "demo@example.com"
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/backend-api/conversation/init"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "default_model_slug": "gpt-5-4-pro",
            "limits_progress": [
                {
                    "feature_name": "deep_research",
                    "remaining": 250
                }
            ]
        })))
        .mount(&server)
        .await;

    let meta = fetch_account_metadata(server.uri(), "token_123").await.expect("metadata fetch");
    assert_eq!(meta.default_model_slug.as_deref(), Some("gpt-5-4-pro"));
    assert_eq!(meta.quota_remaining, 0);
    assert!(!meta.quota_known);
    assert_eq!(meta.restore_at, None);
}

/// Clamps to the minimum delay when the configured refresh window is inverted.
#[test]
fn refresh_delay_clamps_when_min_is_greater_than_max() {
    assert_eq!(next_refresh_delay(600, 300), Duration::from_secs(600));
}

/// Stops the background refresher task when shutdown is signaled.
#[tokio::test(start_paused = true)]
async fn status_refresher_stops_on_shutdown() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = spawn_status_refresher(shutdown_rx);
    shutdown_tx.send(true).expect("send shutdown");
    handle.await.expect("join refresher");
}
