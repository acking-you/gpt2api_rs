//! Admin REST client integration tests.

use gpt2api_rs::{
    admin_client,
    models::{AccountRecord, ApiKeyRecord, ProxyConfigRecord, UsageEventRecord},
};
use wiremock::{
    matchers::{header, method, path, query_param},
    Mock, MockServer, ResponseTemplate,
};

/// Fetches accounts from the admin REST surface.
#[tokio::test]
async fn list_accounts_calls_admin_rest() {
    let server = MockServer::start().await;
    let payload = vec![AccountRecord::minimal("acct-1", "token-1")];

    Mock::given(method("GET"))
        .and(path("/admin/accounts"))
        .and(header("authorization", "Bearer secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&payload))
        .mount(&server)
        .await;

    let accounts =
        admin_client::list_accounts(&server.uri(), "secret").await.expect("accounts response");

    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].name, "acct-1");
}

/// Fetches keys from the admin REST surface.
#[tokio::test]
async fn list_keys_calls_admin_rest() {
    let server = MockServer::start().await;
    let payload = vec![ApiKeyRecord::minimal("key-1", "default", 100)];

    Mock::given(method("GET"))
        .and(path("/admin/keys"))
        .and(header("authorization", "Bearer secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&payload))
        .mount(&server)
        .await;

    let keys = admin_client::list_keys(&server.uri(), "secret").await.expect("keys response");

    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].id, "key-1");
}

/// Fetches usage with the requested limit from the admin REST surface.
#[tokio::test]
async fn list_usage_calls_admin_rest() {
    let server = MockServer::start().await;
    let payload =
        vec![UsageEventRecord::success("event-1", "request-1", "key-1", "default", "acct-1", 1)];

    Mock::given(method("GET"))
        .and(path("/admin/usage"))
        .and(query_param("limit", "25"))
        .and(header("authorization", "Bearer secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&payload))
        .mount(&server)
        .await;

    let usage =
        admin_client::list_usage(&server.uri(), "secret", 25).await.expect("usage response");

    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].event_id, "event-1");
}

/// Fetches proxy configs from the admin REST surface.
#[tokio::test]
async fn list_proxy_configs_calls_admin_rest() {
    let server = MockServer::start().await;
    let payload = vec![ProxyConfigRecord {
        id: "proxy-1".to_string(),
        name: "proxy-1".to_string(),
        proxy_url: "http://127.0.0.1:11118".to_string(),
        proxy_username: None,
        proxy_password: None,
        status: "active".to_string(),
        created_at: 1,
        updated_at: 1,
    }];

    Mock::given(method("GET"))
        .and(path("/admin/proxy-configs"))
        .and(header("authorization", "Bearer secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&payload))
        .mount(&server)
        .await;

    let proxy_configs =
        admin_client::list_proxy_configs(&server.uri(), "secret").await.expect("proxy response");

    assert_eq!(proxy_configs.len(), 1);
    assert_eq!(proxy_configs[0].id, "proxy-1");
}
