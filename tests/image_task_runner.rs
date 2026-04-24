//! Image task runner tests.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use gpt2api_rs::{
    config::ResolvedPaths,
    models::{ImageTaskStatus, SessionSource},
    storage::{control::CreateImageTaskInput, Storage},
};
use tempfile::tempdir;

const ONE_PIXEL_PNG: &[u8] = &[
    0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, b'I', b'H', b'D', b'R',
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89,
];

#[tokio::test]
async fn artifact_store_writes_under_key_session_message_path() {
    let temp = tempdir().expect("tempdir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    let session = storage
        .control
        .create_session("key-a", "Image", SessionSource::Web)
        .await
        .expect("session");
    let message = storage
        .control
        .append_message(
            &session.id,
            "key-a",
            "assistant",
            serde_json::json!({"blocks":[]}),
            gpt2api_rs::models::MessageStatus::Pending,
        )
        .await
        .expect("message");

    let item = gpt2api_rs::upstream::chatgpt::GeneratedImageItem {
        b64_json: BASE64.encode(ONE_PIXEL_PNG),
        revised_prompt: "lake".to_string(),
    };
    let artifact = storage
        .artifacts
        .write_generated_image("task-1", &session.id, &message.id, "key-a", &item, 0)
        .await
        .expect("artifact written");

    assert!(artifact
        .relative_path
        .starts_with(&format!("artifacts/images/key-a/{}/{}", session.id, message.id)));
    assert!(temp.path().join(&artifact.relative_path).is_file());
    assert_eq!(artifact.mime_type, "image/png");
    assert_eq!(artifact.width, Some(1));
    assert_eq!(artifact.height, Some(1));
}

#[tokio::test]
async fn queued_task_position_counts_tasks_ahead() {
    let temp = tempdir().expect("tempdir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    let first = storage
        .control
        .create_image_task(CreateImageTaskInput {
            session_id: "session-1",
            message_id: "message-1",
            key_id: "key-a",
            mode: "generation",
            prompt: "one",
            model: "gpt-image-1",
            n: 1,
            request_json: serde_json::json!({}),
        })
        .await
        .expect("first task");
    let second = storage
        .control
        .create_image_task(CreateImageTaskInput {
            session_id: "session-1",
            message_id: "message-2",
            key_id: "key-a",
            mode: "generation",
            prompt: "two",
            model: "gpt-image-1",
            n: 1,
            request_json: serde_json::json!({}),
        })
        .await
        .expect("second task");

    let snapshot = storage.control.queue_snapshot_for_task(&second.id).await.expect("snapshot");
    assert_eq!(snapshot.position_ahead, 1);
    assert_eq!(snapshot.task.id, second.id);

    let claimed =
        storage.control.claim_next_image_task(1, 123).await.expect("claim").expect("task");
    assert_eq!(claimed.id, first.id);
    assert_eq!(claimed.status, ImageTaskStatus::Running);
}
