//! Product API tests for key login, sessions, and role checks.

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use gpt2api_rs::{
    app::build_router,
    config::ResolvedPaths,
    models::{ApiKeyRecord, ApiKeyRole},
    service::AppService,
    storage::Storage,
    upstream::chatgpt::{ChatgptUpstreamClient, GeneratedImageItem},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;

const ONE_PIXEL_PNG: &[u8] = &[
    0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, b'I', b'H', b'D', b'R',
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89,
];

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
            fixed_account_name: None,
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

#[tokio::test]
async fn image_message_creates_pending_assistant_message_and_queued_task() {
    let (_temp, app, _storage) = build_app().await;
    let (_status, created) =
        json_request(app.clone(), "POST", "/sessions", "sk-user-a", json!({"title":"Image"})).await;
    let session_id = created["session"]["id"].as_str().expect("session id");

    let (status, value) = json_request(
        app,
        "POST",
        &format!("/sessions/{session_id}/messages"),
        "sk-user-a",
        json!({"kind":"image_generation","prompt":"draw a lake","model":"gpt-image-1","n":1,"size":"1536x1024"}),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["task"]["status"], "queued");
    assert_eq!(value["assistant_message"]["status"], "pending");
    assert_eq!(value["queue"]["position_ahead"], 0);
    let request_json = value["task"]["request_json"].as_str().expect("request json");
    let request: Value = serde_json::from_str(request_json).expect("task request json");
    assert_eq!(request["size"], "1536x1024");
    assert_eq!(request["billing"]["size_credit_units"], 2);
    assert_eq!(request["billing"]["billable_credits"], 2);
}

#[tokio::test]
async fn session_delete_removes_conversation_tasks_artifacts_and_records_api_event() {
    let (_temp, app, storage) = build_app().await;
    let (_status, created) =
        json_request(app.clone(), "POST", "/sessions", "sk-user-a", json!({"title":"Trash me"}))
            .await;
    let session_id = created["session"]["id"].as_str().expect("session id");
    let (_status, submitted) = json_request(
        app.clone(),
        "POST",
        &format!("/sessions/{session_id}/messages"),
        "sk-user-a",
        json!({"kind":"image_generation","prompt":"draw a lake","model":"gpt-image-2","n":1}),
    )
    .await;
    let task_id = submitted["task"]["id"].as_str().expect("task id");
    let assistant_message_id =
        submitted["assistant_message"]["id"].as_str().expect("assistant message id");
    let artifact = storage
        .artifacts
        .write_generated_image(
            task_id,
            session_id,
            assistant_message_id,
            "user-a",
            &GeneratedImageItem {
                b64_json: BASE64.encode(ONE_PIXEL_PNG),
                revised_prompt: "lake".to_string(),
            },
            0,
        )
        .await
        .expect("artifact file written");
    let artifact_path = _temp.path().join(&artifact.relative_path);
    storage.control.insert_image_artifact(&artifact).await.expect("artifact row");
    storage
        .control
        .append_task_event(task_id, "phase", json!({"phase":"saving"}))
        .await
        .expect("task event");
    let signed_link = storage
        .control
        .create_signed_link("image_task", task_id, 100, 3600)
        .await
        .expect("signed link");
    assert!(artifact_path.is_file());
    assert!(storage
        .control
        .resolve_signed_link(&signed_link.plaintext_token, 101)
        .await
        .expect("link lookup")
        .is_some());

    let (delete_status, value) = json_request(
        app.clone(),
        "DELETE",
        &format!("/sessions/{session_id}"),
        "sk-user-a",
        json!({}),
    )
    .await;
    assert_eq!(delete_status, StatusCode::OK);
    assert_eq!(value["deleted"], true);

    let (_list_status, list) =
        json_request(app, "GET", "/sessions?limit=20", "sk-user-a", json!({})).await;
    assert!(list["items"].as_array().expect("items").is_empty());
    assert!(storage
        .control
        .get_session_for_admin(session_id)
        .await
        .expect("admin session lookup")
        .is_none());
    assert!(storage
        .control
        .list_messages_for_session(session_id)
        .await
        .expect("messages")
        .is_empty());
    assert!(storage.control.list_tasks_for_session(session_id).await.expect("tasks").is_empty());
    assert!(storage
        .control
        .list_artifacts_for_session(session_id)
        .await
        .expect("artifacts")
        .is_empty());
    assert!(storage
        .control
        .list_task_events_for_session_after(session_id, 0, 20)
        .await
        .expect("task events")
        .is_empty());
    assert!(storage
        .control
        .get_image_artifact(&artifact.id)
        .await
        .expect("artifact lookup")
        .is_none());
    assert!(storage.control.get_image_task(task_id).await.expect("task lookup").is_none());
    assert!(!artifact_path.exists());
    assert!(storage
        .control
        .resolve_signed_link(&signed_link.plaintext_token, 101)
        .await
        .expect("link lookup after delete")
        .is_none());

    let outbox = storage.control.list_pending_outbox_rows(20).await.expect("outbox");
    assert!(outbox.iter().any(|row| {
        row.payload.request_method == "DELETE"
            && row.payload.request_url == format!("/sessions/{session_id}")
            && row.payload.status_code == 200
            && row.payload.billable_credits == 0
    }));
}

#[tokio::test]
async fn queued_task_can_be_cancelled_by_owner() {
    let (_temp, app, _storage) = build_app().await;
    let (_status, created) =
        json_request(app.clone(), "POST", "/sessions", "sk-user-a", json!({"title":"Image"})).await;
    let session_id = created["session"]["id"].as_str().expect("session id");
    let (_status, submitted) = json_request(
        app.clone(),
        "POST",
        &format!("/sessions/{session_id}/messages"),
        "sk-user-a",
        json!({"kind":"image_generation","prompt":"draw a lake","model":"gpt-image-1","n":1}),
    )
    .await;
    let task_id = submitted["task"]["id"].as_str().expect("task id");

    let (status, value) =
        json_request(app, "POST", &format!("/tasks/{task_id}/cancel"), "sk-user-a", json!({}))
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["cancelled"], true);
}

#[tokio::test]
async fn admin_key_can_update_global_queue_concurrency() {
    let (_temp, app, _storage) = build_app().await;
    let (status, value) = json_request(
        app.clone(),
        "PATCH",
        "/admin/queue/config",
        "sk-admin-a",
        json!({"global_image_concurrency":2,"image_task_timeout_seconds":600}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["config"]["global_image_concurrency"], 2);
    assert_eq!(value["config"]["image_task_timeout_seconds"], 600);

    let (user_status, _user_value) = json_request(
        app,
        "PATCH",
        "/admin/queue/config",
        "sk-user-a",
        json!({"global_image_concurrency":3}),
    )
    .await;
    assert_eq!(user_status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn product_admin_can_list_and_patch_keys_without_plaintext_secrets() {
    let (_temp, app, _storage) = build_app().await;
    let (list_status, list_value) =
        json_request(app.clone(), "GET", "/admin/keys", "sk-admin-a", json!({})).await;
    assert_eq!(list_status, StatusCode::OK);
    let items = list_value.as_array().expect("key array");
    let user_key = items.iter().find(|item| item["id"] == "user-a").expect("user-a key");
    assert!(user_key["secret_plaintext"].is_null());

    let (patch_status, patched) = json_request(
        app,
        "PATCH",
        "/admin/keys/user-a",
        "sk-admin-a",
        json!({"role":"admin","notification_enabled":true}),
    )
    .await;
    assert_eq!(patch_status, StatusCode::OK);
    assert_eq!(patched["role"], "admin");
    assert_eq!(patched["notification_enabled"], true);
    assert!(patched["secret_plaintext"].is_null());
}
