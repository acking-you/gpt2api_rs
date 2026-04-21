//! Upstream ChatGPT SSE parsing tests.

use gpt2api_rs::upstream::chatgpt::parse_conversation_sse;

/// Extracts the conversation id and generated file ids from the SSE stream.
#[test]
fn parse_sse_extracts_conversation_id_and_file_ids() {
    let raw = include_str!("fixtures/chatgpt/conversation_sse.txt");
    let parsed = parse_conversation_sse(raw);

    assert_eq!(parsed.conversation_id.as_deref(), Some("conv_1"));
    assert_eq!(parsed.file_ids, vec!["file_123".to_string()]);
}
