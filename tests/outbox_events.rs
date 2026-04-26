//! Usage-event ledger enqueue tests.

use gpt2api_rs::config::ResolvedPaths;
use gpt2api_rs::models::{AccountRecord, ApiKeyRecord, UsageEventRecord};
use gpt2api_rs::storage::Storage;
use gpt2api_rs::usage::record_successful_generation;
use tempfile::tempdir;

/// Leaves mutable key counters unchanged and enqueues the billable usage event.
#[tokio::test]
async fn successful_generation_enqueues_outbox_without_mutating_key_counter() {
    let temp = tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");

    storage
        .control
        .upsert_account(&AccountRecord::minimal("acct_1", "tok"))
        .await
        .expect("account upsert");
    storage
        .control
        .upsert_api_key(&ApiKeyRecord::minimal("key_1", "demo", 20))
        .await
        .expect("api key upsert");

    record_successful_generation(
        &storage.control,
        &UsageEventRecord::success("evt_1", "req_1", "key_1", "demo", "acct_1", 2),
    )
    .await
    .expect("usage settlement");

    let key =
        storage.control.get_api_key("key_1").await.expect("api key fetch").expect("api key exists");
    let outbox = storage.control.list_pending_outbox_rows(10).await.expect("list outbox");

    assert_eq!(key.quota_used_calls, 0);
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].payload.billable_credits, 2);
}
