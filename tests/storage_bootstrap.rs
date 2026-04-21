//! Storage bootstrap tests for control and event databases.

use gpt2api_rs::config::ResolvedPaths;
use gpt2api_rs::storage::Storage;
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
