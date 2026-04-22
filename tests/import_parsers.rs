//! Parser tests for supported account import payloads.

use gpt2api_rs::accounts::import::{
    derive_account_name, extract_cpa_access_token, extract_session_access_token,
};
use gpt2api_rs::config::ResolvedPaths;
use gpt2api_rs::models::{AccountRecord, AccountSourceKind};
use gpt2api_rs::storage::Storage;
use rusqlite::Connection;
use tempfile::tempdir;

/// Extracts the session access token from the copied ChatGPT session JSON.
#[test]
fn extract_session_json_access_token() {
    let raw = r#"{"user":{"id":"u_1"},"accessToken":"sess_token_123"}"#;
    assert_eq!(extract_session_access_token(raw).unwrap(), "sess_token_123");
}

/// Accepts both snake_case and camelCase CPA access token aliases.
#[test]
fn extract_cpa_access_token_from_both_aliases() {
    let snake = r#"{"access_token":"snake_token"}"#;
    let camel = r#"{"accessToken":"camel_token"}"#;
    assert_eq!(extract_cpa_access_token(snake).unwrap(), "snake_token");
    assert_eq!(extract_cpa_access_token(camel).unwrap(), "camel_token");
}

/// Derives a stable synthetic account name from a raw access token.
#[test]
fn derived_account_name_is_stable() {
    let first = derive_account_name("same_token_value");
    let second = derive_account_name("same_token_value");
    assert_eq!(first, second);
    assert!(first.starts_with("acct_"));
    assert_eq!(first.len(), "acct_".len() + 12);
}

/// Persists imported accounts into the control-plane SQLite database.
#[tokio::test]
async fn upsert_account_persists_imported_account() {
    let temp = tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    let account = AccountRecord {
        name: "acct_demo".to_string(),
        access_token: "tok_123".to_string(),
        source_kind: AccountSourceKind::SessionJson,
        email: Some("demo@example.com".to_string()),
        user_id: Some("user_123".to_string()),
        plan_type: Some("plus".to_string()),
        default_model_slug: Some("auto".to_string()),
        status: "active".to_string(),
        quota_remaining: 12,
        quota_known: true,
        restore_at: None,
        last_refresh_at: Some(123),
        last_used_at: None,
        last_error: None,
        success_count: 1,
        fail_count: 0,
        request_max_concurrency: Some(2),
        request_min_start_interval_ms: Some(500),
        browser_profile_json: "{}".to_string(),
    };

    storage.control.upsert_account(&account).await.expect("account upsert");

    let conn = Connection::open(&paths.control_db).expect("open control db");
    let row = conn
        .query_row(
            "SELECT source_kind, status, quota_remaining FROM accounts WHERE name = ?1",
            [&account.name],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?)),
        )
        .expect("select persisted account");
    assert_eq!(row.0, "session_json");
    assert_eq!(row.1, "active");
    assert_eq!(row.2, 12);
}
