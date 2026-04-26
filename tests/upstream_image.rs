//! Upstream ChatGPT SSE parsing tests.

use gpt2api_rs::upstream::chatgpt::parse_conversation_sse;

/// Extracts the conversation id and generated file ids from the SSE stream.
#[test]
fn parse_sse_extracts_conversation_id_and_file_ids() {
    let raw = include_str!("fixtures/chatgpt/conversation_sse.txt");
    let parsed = parse_conversation_sse(raw);

    assert_eq!(parsed.conversation_id, "conv_1");
    assert_eq!(parsed.file_ids, vec!["file_123".to_string()]);
}

/// Assistant text events are snapshots, so the parser must keep the latest
/// value instead of appending every intermediate frame.
#[test]
fn parse_sse_keeps_latest_assistant_text_snapshot() {
    let raw = r#"data: {"conversation_id":"conv_2","message":{"author":{"role":"assistant"},"content":{"content_type":"text","parts":["我可以帮你"]}}}

data: {"conversation_id":"conv_2","message":{"author":{"role":"assistant"},"content":{"content_type":"text","parts":["我可以帮你生成一张图片"]}}}

data: [DONE]
"#;
    let parsed = parse_conversation_sse(raw);

    assert_eq!(parsed.conversation_id, "conv_2");
    assert_eq!(parsed.text, "我可以帮你生成一张图片");
}
