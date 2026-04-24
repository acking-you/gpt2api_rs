//! Product storage tests for sessions, image tasks, artifacts, and signed links.

use gpt2api_rs::{
    config::ResolvedPaths,
    models::{ApiKeyRecord, MessageStatus, SessionSource},
    storage::Storage,
};
use tempfile::tempdir;

#[tokio::test]
async fn bootstrap_adds_product_tables_and_defaults() {
    let temp = tempdir().expect("tempdir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");

    let tables = storage.control.list_table_names().await.expect("table names");
    for expected in [
        "api_keys",
        "sessions",
        "messages",
        "image_tasks",
        "task_events",
        "image_artifacts",
        "signed_links",
        "runtime_config",
    ] {
        assert!(tables.iter().any(|name| name == expected), "missing table {expected}");
    }

    storage
        .control
        .upsert_api_key(&ApiKeyRecord::minimal("default", "default", 100))
        .await
        .expect("seed key");
    let key = storage.control.get_api_key("default").await.expect("key read").expect("default key");
    assert_eq!(key.role.as_str(), "user");
    assert_eq!(key.notification_email, None);
    assert!(!key.notification_enabled);

    let config = storage.control.get_runtime_config().await.expect("runtime config");
    assert_eq!(config.global_image_concurrency, 1);
    assert_eq!(config.signed_link_ttl_seconds, 604_800);
    assert_eq!(config.queue_eta_window_size, 20);
}

#[tokio::test]
async fn session_and_message_crud_is_key_scoped() {
    let temp = tempdir().expect("tempdir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");

    let session = storage
        .control
        .create_session("key-a", "First image", SessionSource::Web)
        .await
        .expect("session created");
    assert_eq!(session.key_id, "key-a");

    let user_message = storage
        .control
        .append_message(
            &session.id,
            "key-a",
            "user",
            serde_json::json!({"type":"text","text":"draw a lake"}),
            MessageStatus::Done,
        )
        .await
        .expect("message created");
    assert_eq!(user_message.session_id, session.id);

    assert!(storage
        .control
        .get_session_for_key(&session.id, "key-a")
        .await
        .expect("own session lookup")
        .is_some());
    assert!(storage
        .control
        .get_session_for_key(&session.id, "key-b")
        .await
        .expect("other session lookup")
        .is_none());
}

#[tokio::test]
async fn signed_link_hash_validation_rejects_expired_and_revoked_links() {
    let temp = tempdir().expect("tempdir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");

    let link = storage
        .control
        .create_signed_link("image_task", "task-1", 100, 10)
        .await
        .expect("link created");
    assert!(storage
        .control
        .resolve_signed_link(&link.plaintext_token, 50)
        .await
        .expect("valid link lookup")
        .is_some());
    assert!(storage
        .control
        .resolve_signed_link(&link.plaintext_token, 200)
        .await
        .expect("expired link lookup")
        .is_none());

    storage.control.revoke_signed_link(&link.id, 60).await.expect("revoke link");
    assert!(storage
        .control
        .resolve_signed_link(&link.plaintext_token, 61)
        .await
        .expect("revoked link lookup")
        .is_none());
}
