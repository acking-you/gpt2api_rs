//! Admin API integration tests.

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gpt2api_rs::app::{build_router, build_router_for_tests};
use serde_json::Value;
use tower::ServiceExt;

/// Requires a bearer token before serving the admin status endpoint.
#[tokio::test]
async fn admin_status_requires_bearer_token() {
    let app = build_router_for_tests();
    let response = app
        .oneshot(Request::builder().uri("/admin/status").body(Body::empty()).expect("request"))
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Accepts the configured admin bearer token.
#[tokio::test]
async fn admin_status_accepts_configured_bearer_token() {
    let app = build_router("custom-secret".to_string());
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
    let app = build_router_for_tests();
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

/// Returns an empty API-key list when the router has no attached runtime state yet.
#[tokio::test]
async fn admin_keys_return_empty_list_without_runtime_state() {
    let app = build_router_for_tests();
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
    assert_eq!(json, Value::Array(vec![]));
}

/// Returns an empty usage list when the router has no attached runtime state yet.
#[tokio::test]
async fn admin_usage_returns_empty_list_without_runtime_state() {
    let app = build_router_for_tests();
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
