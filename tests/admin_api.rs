//! Admin API integration tests.

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
};
use gpt2api_rs::{
    app::build_router, config::ResolvedPaths, service::AppService, storage::Storage,
    upstream::chatgpt::ChatgptUpstreamClient,
};
use serde_json::{json, Value};
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
    assert_eq!(items[0].get("secret_plaintext").and_then(Value::as_str), Some("secret"));
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

async fn send_json(
    app: axum::Router,
    method: Method,
    uri: &str,
    admin_token: &str,
    body: Value,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {admin_token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request"),
    )
    .await
    .expect("router response")
}

#[tokio::test]
async fn admin_key_lifecycle_supports_create_patch_rotate_and_delete() {
    let (_temp, app) = build_test_app("secret").await;

    let create = send_json(
        app.clone(),
        Method::POST,
        "/admin/keys",
        "secret",
        json!({
            "name": "demo",
            "quota_total_calls": 7,
            "route_strategy": "fixed",
            "request_max_concurrency": 2,
            "request_min_start_interval_ms": 50
        }),
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let create_body = to_bytes(create.into_body(), usize::MAX).await.expect("body bytes");
    let created: Value = serde_json::from_slice(&create_body).expect("json body");
    let key_id = created.get("id").and_then(Value::as_str).expect("id").to_string();
    let plaintext = created
        .get("secret_plaintext")
        .and_then(Value::as_str)
        .expect("secret_plaintext")
        .to_string();
    assert!(plaintext.starts_with("sk-"));
    assert_eq!(created.get("quota_total_calls").and_then(Value::as_i64), Some(7));
    assert_eq!(created.get("quota_used_calls").and_then(Value::as_i64), Some(0));

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin/keys")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = to_bytes(list.into_body(), usize::MAX).await.expect("body bytes");
    let items: Value = serde_json::from_slice(&list_body).expect("json body");
    let created_row = items
        .as_array()
        .expect("array payload")
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(key_id.as_str()))
        .expect("created key row");
    assert_eq!(
        created_row.get("secret_plaintext").and_then(Value::as_str),
        Some(plaintext.as_str())
    );
    assert_eq!(created_row.get("quota_total_calls").and_then(Value::as_i64), Some(7));

    let patch = send_json(
        app.clone(),
        Method::PATCH,
        &format!("/admin/keys/{key_id}"),
        "secret",
        json!({
            "name": "demo-renamed",
            "status": "disabled",
            "quota_total_calls": 9
        }),
    )
    .await;
    assert_eq!(patch.status(), StatusCode::OK);
    let patch_body = to_bytes(patch.into_body(), usize::MAX).await.expect("body bytes");
    let patched: Value = serde_json::from_slice(&patch_body).expect("json body");
    assert_eq!(patched.get("name").and_then(Value::as_str), Some("demo-renamed"));
    assert_eq!(patched.get("status").and_then(Value::as_str), Some("disabled"));
    assert_eq!(patched.get("quota_total_calls").and_then(Value::as_i64), Some(9));
    assert_eq!(patched.get("secret_plaintext").and_then(Value::as_str), Some(plaintext.as_str()));

    let rotate = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/admin/keys/{key_id}/rotate"))
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");
    assert_eq!(rotate.status(), StatusCode::OK);
    let rotate_body = to_bytes(rotate.into_body(), usize::MAX).await.expect("body bytes");
    let rotated: Value = serde_json::from_slice(&rotate_body).expect("json body");
    let rotated_plaintext =
        rotated.get("secret_plaintext").and_then(Value::as_str).expect("rotated plaintext");
    assert_ne!(rotated_plaintext, plaintext);

    let delete = app
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("/admin/keys/{key_id}"))
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");
    assert_eq!(delete.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_proxy_config_lifecycle_supports_create_patch_check_and_delete() {
    let (_temp, app) = build_test_app("secret").await;

    let create = send_json(
        app.clone(),
        Method::POST,
        "/admin/proxy-configs",
        "secret",
        json!({
            "name": "proxy-one",
            "proxy_url": "http://127.0.0.1:11118",
            "proxy_username": "alice",
            "proxy_password": "pw",
            "status": "active"
        }),
    )
    .await;

    assert_eq!(create.status(), StatusCode::OK);
}
