//! Control-plane proxy storage integration tests.

use gpt2api_rs::{
    config::ResolvedPaths,
    models::{AccountProxyMode, AccountRecord, ProxyConfigRecord},
    storage::Storage,
};

#[tokio::test]
async fn account_and_proxy_config_round_trip_through_control_db() {
    let temp = tempfile::tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");

    let proxy = ProxyConfigRecord {
        id: "proxy-1".to_string(),
        name: "proxy-one".to_string(),
        proxy_url: "http://127.0.0.1:11118".to_string(),
        proxy_username: Some("alice".to_string()),
        proxy_password: Some("secret".to_string()),
        status: "active".to_string(),
        created_at: 100,
        updated_at: 100,
    };
    storage.control.upsert_proxy_config(&proxy).await.expect("proxy saved");

    let mut account = AccountRecord::minimal("acct-1", "token-1");
    account.proxy_mode = AccountProxyMode::Fixed;
    account.proxy_config_id = Some("proxy-1".to_string());
    storage.control.upsert_account(&account).await.expect("account saved");

    let saved_proxy = storage
        .control
        .get_proxy_config("proxy-1")
        .await
        .expect("proxy lookup")
        .expect("proxy row");
    let saved_account =
        storage.control.get_account("acct-1").await.expect("account lookup").expect("account row");

    assert_eq!(saved_proxy.proxy_username.as_deref(), Some("alice"));
    assert_eq!(saved_account.proxy_mode, AccountProxyMode::Fixed);
    assert_eq!(saved_account.proxy_config_id.as_deref(), Some("proxy-1"));
}
