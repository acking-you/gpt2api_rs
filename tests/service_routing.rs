//! Service-level routing tests that exercise the real account-selection path.

use std::sync::Arc;

use gpt2api_rs::{
    config::ResolvedPaths,
    models::{AccountRecord, ApiKeyRecord},
    service::AppService,
    storage::Storage,
    upstream::chatgpt::ChatgptUpstreamClient,
};
use rusqlite::{params, Connection};
use serde_json::json;
use tempfile::TempDir;
use wiremock::{
    matchers::{header, method, path},
    Mock, MockServer, ResponseTemplate,
};

async fn build_service_test_app() -> (TempDir, ResolvedPaths, Storage, Arc<AppService>, MockServer)
{
    let temp = tempfile::tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    let mock = MockServer::start().await;
    let service = Arc::new(
        AppService::new(
            storage.clone(),
            "admin-secret".to_string(),
            ChatgptUpstreamClient::new(mock.uri(), None),
        )
        .await
        .expect("service init"),
    );
    (temp, paths, storage, service, mock)
}

fn insert_account_group(paths: &ResolvedPaths, group_id: &str, name: &str, account_names: &[&str]) {
    let conn = Connection::open(&paths.control_db).expect("open control db");
    conn.execute("INSERT INTO account_groups (id, name) VALUES (?1, ?2)", params![group_id, name])
        .expect("insert group");
    for account_name in account_names {
        conn.execute(
            "INSERT INTO account_group_members (group_id, account_name) VALUES (?1, ?2)",
            params![group_id, account_name],
        )
        .expect("insert group member");
    }
}

#[tokio::test]
async fn auto_route_honors_account_group_subset_before_global_quota_ordering() {
    let (_temp, paths, storage, service, mock) = build_service_test_app().await;

    let mut alpha = AccountRecord::minimal("alpha", "alpha-token");
    alpha.quota_known = true;
    alpha.quota_remaining = 100;
    storage.control.upsert_account(&alpha).await.expect("seed alpha");

    let mut beta = AccountRecord::minimal("beta", "beta-token");
    beta.quota_known = true;
    beta.quota_remaining = 1;
    storage.control.upsert_account(&beta).await.expect("seed beta");

    insert_account_group(&paths, "group-beta", "beta-only", &["beta"]);

    let key = ApiKeyRecord {
        id: "subset-key".to_string(),
        name: "subset-key".to_string(),
        secret_hash: "hash".to_string(),
        secret_plaintext: None,
        status: "active".to_string(),
        quota_total_calls: 10,
        quota_used_calls: 0,
        route_strategy: "auto".to_string(),
        account_group_id: Some("group-beta".to_string()),
        request_max_concurrency: None,
        request_min_start_interval_ms: None,
    };
    storage.control.upsert_api_key(&key).await.expect("seed key");

    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html></html>"))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/sentinel/chat-requirements"))
        .and(header("authorization", "Bearer alpha-token"))
        .respond_with(ResponseTemplate::new(500).set_body_string("alpha should not be selected"))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/sentinel/chat-requirements"))
        .and(header("authorization", "Bearer beta-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "token": "chat-token",
            "proofofwork": { "required": false }
        })))
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/conversation"))
        .and(header("authorization", "Bearer beta-token"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            concat!(
                "data: {\"conversation_id\":\"conv-1\",\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"content_type\":\"text\",\"parts\":[\"beta\"]}}}\n\n",
                "data: {\"conversation_id\":\"conv-1\",\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"content_type\":\"text\",\"parts\":[\"beta wins\"]}}}\n\n",
                "data: [DONE]\n\n"
            ),
            "text/event-stream",
        ))
        .mount(&mock)
        .await;

    let result = service
        .complete_text_for_key(&key, "Say hello", "auto", "/v1/chat/completions")
        .await
        .expect("request should stay inside beta subset");

    assert_eq!(result.text, "beta wins");
}
