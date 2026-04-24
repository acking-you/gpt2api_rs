//! Product API tests for key login, sessions, and role checks.

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gpt2api_rs::{
    app::build_router,
    config::ResolvedPaths,
    models::{ApiKeyRecord, ApiKeyRole},
    service::AppService,
    storage::Storage,
    upstream::chatgpt::ChatgptUpstreamClient,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;

async fn build_app() -> (TempDir, axum::Router, Storage) {
    let temp = tempdir().expect("tempdir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    seed_key(&storage, "user-a", "User A", "sk-user-a", ApiKeyRole::User).await;
    seed_key(&storage, "user-b", "User B", "sk-user-b", ApiKeyRole::User).await;
    seed_key(&storage, "admin-a", "Admin A", "sk-admin-a", ApiKeyRole::Admin).await;
    let service = Arc::new(
        AppService::new(
            storage.clone(),
            "service-admin".to_string(),
            ChatgptUpstreamClient::default(),
        )
        .await
        .expect("service"),
    );
    (temp, build_router(service), storage)
}

async fn seed_key(storage: &Storage, id: &str, name: &str, secret: &str, role: ApiKeyRole) {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    let secret_hash = format!("{:x}", hasher.finalize());
    storage
        .control
        .upsert_api_key(&ApiKeyRecord {
            id: id.to_string(),
            name: name.to_string(),
            secret_hash,
            secret_plaintext: Some(secret.to_string()),
            status: "active".to_string(),
            quota_total_calls: 100,
            quota_used_calls: 0,
            route_strategy: "auto".to_string(),
            account_group_id: None,
            request_max_concurrency: None,
            request_min_start_interval_ms: None,
            role,
            notification_email: None,
            notification_enabled: false,
        })
        .await
        .expect("seed key");
}

async fn json_request(
    app: axum::Router,
    method: &str,
    uri: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({}));
    (status, value)
}

#[tokio::test]
async fn auth_verify_returns_role_and_notification_settings() {
    let (_temp, app, _storage) = build_app().await;
    let (status, value) = json_request(app, "POST", "/auth/verify", "sk-admin-a", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["ok"], true);
    assert_eq!(value["key"]["id"], "admin-a");
    assert_eq!(value["key"]["role"], "admin");
    assert_eq!(value["key"]["notification_enabled"], false);
}

#[tokio::test]
async fn user_sessions_are_scoped_to_own_key() {
    let (_temp, app, _storage) = build_app().await;
    let (created_status, created) =
        json_request(app.clone(), "POST", "/sessions", "sk-user-a", json!({"title":"Lake image"}))
            .await;
    assert_eq!(created_status, StatusCode::OK);
    let session_id = created["session"]["id"].as_str().expect("session id");

    let (own_status, _own) = json_request(
        app.clone(),
        "GET",
        &format!("/sessions/{session_id}"),
        "sk-user-a",
        json!({}),
    )
    .await;
    assert_eq!(own_status, StatusCode::OK);

    let (other_status, _other) =
        json_request(app, "GET", &format!("/sessions/{session_id}"), "sk-user-b", json!({})).await;
    assert_eq!(other_status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn admin_key_can_search_all_sessions_without_service_admin_token() {
    let (_temp, app, _storage) = build_app().await;
    let (_created_status, _created) =
        json_request(app.clone(), "POST", "/sessions", "sk-user-a", json!({"title":"Lake image"}))
            .await;

    let (status, value) =
        json_request(app, "GET", "/admin/sessions?q=Lake&limit=20", "sk-admin-a", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["items"].as_array().expect("items").len(), 1);
}

#[tokio::test]
async fn normal_key_cannot_use_admin_product_api() {
    let (_temp, app, _storage) = build_app().await;
    let (status, _value) =
        json_request(app, "GET", "/admin/sessions?limit=20", "sk-user-a", json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}
