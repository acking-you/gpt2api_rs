//! Unofficial ChatGPT image-generation transport helpers.

use serde_json::Value;

/// Parsed highlights extracted from a ChatGPT conversation SSE stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedConversationSse {
    /// Conversation id observed in the stream.
    pub conversation_id: Option<String>,
    /// File ids emitted by the image generation tool call.
    pub file_ids: Vec<String>,
}

/// Parses the conversation SSE response and extracts file-service ids.
#[must_use]
pub fn parse_conversation_sse(raw: &str) -> ParsedConversationSse {
    let mut parsed = ParsedConversationSse::default();

    for line in raw.lines() {
        let line = line.trim();
        if !line.starts_with("data:") {
            continue;
        }

        let payload = line.trim_start_matches("data:").trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }

        if let Ok(value) = serde_json::from_str::<Value>(payload) {
            if parsed.conversation_id.is_none() {
                parsed.conversation_id =
                    value.get("conversation_id").and_then(Value::as_str).map(ToString::to_string);
            }

            if let Some(parts) = value
                .get("message")
                .and_then(|message| message.get("content"))
                .and_then(|content| content.get("parts"))
                .and_then(Value::as_array)
            {
                for part in parts {
                    if let Some(pointer) = part.get("asset_pointer").and_then(Value::as_str) {
                        if let Some(file_id) = pointer.strip_prefix("file-service://") {
                            parsed.file_ids.push(file_id.to_string());
                        }
                    }
                }
            }
        }
    }

    parsed
}
