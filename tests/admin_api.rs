//! Admin API integration tests.

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gpt2api_rs::{
    app::build_router,
    config::ResolvedPaths,
    service::AppService,
    storage::Storage,
    upstream::chatgpt::ChatgptUpstreamClient,
};
use serde_json::Value;
use tempfile::TempDir;
use tower::ServiceExt;

async fn build_test_app(admin_token: &str) -> (TempDir, axum::Router) {
    let temp = tempfile::tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    let service = Arc::new(
        AppService::new(
            storage,
            admin_token.to_string(),
            ChatgptUpstreamClient::new("http://127.0.0.1:9", None),
        )
        .await
        .expect("service init"),
    );
    (temp, build_router(service))
}

/// Requires a bearer token before serving the admin status endpoint.
#[tokio::test]
async fn admin_status_requires_bearer_token() {
    let (_temp, app) = build_test_app("secret").await;
    let response = app
        .oneshot(Request::builder().uri("/admin/status").body(Body::empty()).expect("request"))
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Accepts the configured admin bearer token.
#[tokio::test]
async fn admin_status_accepts_configured_bearer_token() {
    let (_temp, app) = build_test_app("custom-secret").await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/admin/status")
                .header("authorization", "Bearer custom-secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
}

/// Returns an empty account list when the router has no attached runtime state yet.
#[tokio::test]
async fn admin_accounts_return_empty_list_without_runtime_state() {
    let (_temp, app) = build_test_app("secret").await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/admin/accounts")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.expect("body bytes");
    let json: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(json, Value::Array(vec![]));
}

/// Returns the bootstrapped default API key for public authentication.
#[tokio::test]
async fn admin_keys_return_empty_list_without_runtime_state() {
    let (_temp, app) = build_test_app("secret").await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/admin/keys")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.expect("body bytes");
    let json: Value = serde_json::from_slice(&body).expect("json body");
    let items = json.as_array().expect("array payload");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].get("id").and_then(Value::as_str), Some("default"));
}

/// Returns an empty usage list when the router has no attached runtime state yet.
#[tokio::test]
async fn admin_usage_returns_empty_list_without_runtime_state() {
    let (_temp, app) = build_test_app("secret").await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/admin/usage?limit=25")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.expect("body bytes");
    let json: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(json, Value::Array(vec![]));
}
