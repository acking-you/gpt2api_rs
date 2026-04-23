//! Storage bootstrap tests for control and event databases.

use gpt2api_rs::config::ResolvedPaths;
use gpt2api_rs::models::ApiKeyRecord;
use gpt2api_rs::storage::Storage;
use rusqlite::Connection;
use tempfile::tempdir;

/// Creates the SQLite control DB, DuckDB event store, and blob directory.
#[tokio::test]
async fn open_storage_creates_sqlite_duckdb_and_blob_dirs() {
    let temp = tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());

    let storage = Storage::open(&paths).await.expect("storage opens");
    let tables = storage.control.list_table_names().await.expect("table listing");

    assert!(paths.control_db.is_file());
    assert!(paths.events_duckdb.is_file());
    assert!(paths.event_blobs_dir.is_dir());
    assert!(tables.iter().any(|name| name == "accounts"));
    assert!(tables.iter().any(|name| name == "api_keys"));
    assert!(tables.iter().any(|name| name == "event_outbox"));
}

/// Migrates old image-quota API-key schema to the canonical call-quota schema
/// so new upserts work against existing storage roots.
#[tokio::test]
async fn open_storage_rebuilds_legacy_api_keys_schema() {
    let temp = tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());

    let conn = Connection::open(&paths.control_db).expect("open legacy control db");
    conn.execute_batch(
        r#"
        CREATE TABLE api_keys (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            secret_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            quota_total_images INTEGER NOT NULL,
            quota_used_images INTEGER NOT NULL DEFAULT 0,
            route_strategy TEXT NOT NULL,
            account_group_id TEXT,
            request_max_concurrency INTEGER,
            request_min_start_interval_ms INTEGER
        );
        INSERT INTO api_keys (
            id, name, secret_hash, status, quota_total_images, quota_used_images, route_strategy
        ) VALUES (
            'legacy', 'legacy', 'hash', 'active', 12, 3, 'auto'
        );
        "#,
    )
    .expect("write legacy schema");
    drop(conn);

    let storage = Storage::open(&paths).await.expect("storage opens");
    storage
        .control
        .upsert_api_key(&ApiKeyRecord::minimal("fresh", "fresh", 20))
        .await
        .expect("new upsert succeeds");

    let keys = storage.control.list_api_keys().await.expect("keys list");
    let legacy = keys.iter().find(|key| key.id == "legacy").expect("legacy key preserved");
    assert_eq!(legacy.quota_total_calls, 12);
    assert_eq!(legacy.quota_used_calls, 3);
    assert_eq!(legacy.secret_plaintext, None);
    assert!(keys.iter().any(|key| key.id == "fresh"));

    let conn = Connection::open(&paths.control_db).expect("reopen migrated db");
    let mut stmt = conn.prepare("PRAGMA table_info(api_keys)").expect("table info");
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query columns")
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("collect columns");
    assert!(columns.iter().any(|column| column == "quota_total_calls"));
    assert!(columns.iter().any(|column| column == "quota_used_calls"));
    assert!(columns.iter().any(|column| column == "secret_plaintext"));
    assert!(!columns.iter().any(|column| column == "quota_total_images"));
    assert!(!columns.iter().any(|column| column == "quota_used_images"));
}
