//! Public API integration tests.

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gpt2api_rs::{
    app::build_router,
    config::ResolvedPaths,
    models::{AccountRecord, ApiKeyRecord, ApiKeyRole},
    service::AppService,
    storage::Storage,
    upstream::chatgpt::ChatgptUpstreamClient,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tower::ServiceExt;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

const ONE_PIXEL_PNG: &[u8] = &[
    0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, b'I', b'H', b'D', b'R',
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89,
];

async fn build_test_app() -> (TempDir, axum::Router) {
    let temp = tempfile::tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    let service = Arc::new(
        AppService::new(
            storage,
            "secret".to_string(),
            ChatgptUpstreamClient::new("http://127.0.0.1:9", None),
        )
        .await
        .expect("service init"),
    );
    (temp, build_router(service))
}

/// Returns the OpenAI-compatible image model listing.
#[tokio::test]
async fn models_endpoint_returns_image_models() {
    let (_temp, app) = build_test_app().await;

    let response = app
        .oneshot(Request::builder().uri("/v1/models").body(Body::empty()).expect("request"))
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
}

async fn build_chat_test_app() -> (TempDir, axum::Router, MockServer) {
    let temp = tempfile::tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    storage
        .control
        .upsert_account(&AccountRecord::minimal("acct-1", "access-token-1"))
        .await
        .expect("seed account");
    let mock = MockServer::start().await;
    let service = Arc::new(
        AppService::new(
            storage,
            "secret".to_string(),
            ChatgptUpstreamClient::new(mock.uri(), None),
        )
        .await
        .expect("service init"),
    );
    (temp, build_router(service), mock)
}

#[tokio::test]
async fn login_returns_key_metadata_for_valid_bearer() {
    let (_temp, app) = build_test_app().await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/auth/login")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.expect("body bytes");
    let json: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(json.get("ok").and_then(Value::as_bool), Some(true));
    assert_eq!(
        json.get("key").and_then(|key| key.get("name")).and_then(Value::as_str),
        Some("default")
    );
    assert!(json
        .get("key")
        .and_then(|key| key.get("quota_total_calls"))
        .and_then(Value::as_i64)
        .is_some());
}

#[tokio::test]
async fn login_backfills_plaintext_for_legacy_hash_only_keys() {
    let temp = tempfile::tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    let legacy_secret = "sk-legacy-secret";
    storage
        .control
        .upsert_api_key(&ApiKeyRecord {
            id: "legacy".to_string(),
            name: "legacy".to_string(),
            secret_hash: format!("{:x}", Sha256::digest(legacy_secret.as_bytes())),
            secret_plaintext: None,
            status: "active".to_string(),
            quota_total_calls: 10,
            quota_used_calls: 0,
            route_strategy: "auto".to_string(),
            account_group_id: None,
            fixed_account_name: None,
            request_max_concurrency: None,
            request_min_start_interval_ms: None,
            role: ApiKeyRole::User,
            notification_email: None,
            notification_enabled: false,
        })
        .await
        .expect("seed legacy key");

    let service = Arc::new(
        AppService::new(
            storage,
            "admin-secret".to_string(),
            ChatgptUpstreamClient::new("http://127.0.0.1:9", None),
        )
        .await
        .expect("service init"),
    );
    let app = build_router(service.clone());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/auth/login")
                .header("authorization", format!("Bearer {legacy_secret}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    let key = service
        .storage()
        .control
        .get_api_key("legacy")
        .await
        .expect("api key fetch")
        .expect("legacy key exists");
    assert_eq!(key.secret_plaintext.as_deref(), Some(legacy_secret));
}

#[tokio::test]
async fn text_chat_completion_returns_non_stream_payload() {
    let (_temp, app, mock) = build_chat_test_app().await;

    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html></html>"))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/sentinel/chat-requirements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "token": "chat-token",
            "proofofwork": { "required": false }
        })))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/conversation"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            concat!(
                "data: {\"conversation_id\":\"conv-1\",\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"content_type\":\"text\",\"parts\":[\"Hello\"]}}}\n\n",
                "data: {\"conversation_id\":\"conv-1\",\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"content_type\":\"text\",\"parts\":[\"Hello world\"]}}}\n\n",
                "data: [DONE]\n\n"
            ),
            "text/event-stream",
        ))
        .mount(&mock)
        .await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "auto",
                        "messages": [{ "role": "user", "content": "Say hello" }]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.expect("body bytes");
    let json: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(
        json.pointer("/choices/0/message/content").and_then(Value::as_str),
        Some("Hello world")
    );
}

#[tokio::test]
async fn text_chat_completion_streams_openai_sse_chunks() {
    let (_temp, app, mock) = build_chat_test_app().await;

    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html></html>"))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/sentinel/chat-requirements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "token": "chat-token",
            "proofofwork": { "required": false }
        })))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/conversation"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            concat!(
                "data: {\"conversation_id\":\"conv-1\",\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"content_type\":\"text\",\"parts\":[\"Hello\"]}}}\n\n",
                "data: {\"conversation_id\":\"conv-1\",\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"content_type\":\"text\",\"parts\":[\"Hello world\"]}}}\n\n",
                "data: [DONE]\n\n"
            ),
            "text/event-stream",
        ))
        .mount(&mock)
        .await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "model": "auto",
                        "stream": true,
                        "messages": [{ "role": "user", "content": "Say hello" }]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(content_type.starts_with("text/event-stream"));

    let body = to_bytes(response.into_body(), usize::MAX).await.expect("body bytes");
    let text = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(text.contains("\"role\":\"assistant\""));
    assert!(text.contains("\"content\":\"Hello\""));
    assert!(text.contains("\"content\":\" world\""));
    assert!(text.contains("data: [DONE]"));
}

#[tokio::test]
async fn compatible_image_generation_writes_api_session_history() {
    let (_temp, app, mock) = build_chat_test_app().await;
    mount_image_generation_mocks(&mock).await;

    let first_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/images/generations")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"model":"gpt-image-1","prompt":"draw a lake","n":1}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(first_response.status(), StatusCode::OK);

    let second_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/images/generations")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"model":"gpt-image-1","prompt":"draw a forest","n":1}"#))
                .expect("request"),
        )
        .await
        .expect("second response");
    assert_eq!(second_response.status(), StatusCode::OK);

    let sessions = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/sessions?limit=20")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("sessions response");
    assert_eq!(sessions.status(), StatusCode::OK);
    let bytes = to_bytes(sessions.into_body(), 1024 * 1024).await.expect("body");
    let value: Value = serde_json::from_slice(&bytes).expect("json");
    let api_session_count = value["items"]
        .as_array()
        .expect("items")
        .iter()
        .filter(|item| item["source"] == "api")
        .count();
    assert_eq!(api_session_count, 2);
}

async fn mount_image_generation_mocks(mock: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html></html>"))
        .mount(mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/sentinel/chat-requirements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "token": "chat-token",
            "proofofwork": { "required": false }
        })))
        .mount(mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/conversation"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            "data: {\"conversation_id\":\"conv-1\",\"message\":{\"author\":{\"role\":\"tool\"},\"content\":{\"content_type\":\"multimodal_text\",\"parts\":[{\"asset_pointer\":\"file-service://file-1\"}]}}}\n\ndata: [DONE]\n\n",
            "text/event-stream",
        ))
        .mount(mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/backend-api/files/file-1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "download_url": format!("{}/image.png", mock.uri())
        })))
        .mount(mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/image.png"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(ONE_PIXEL_PNG, "image/png"))
        .mount(mock)
        .await;
}
