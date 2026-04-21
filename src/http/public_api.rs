//! Public OpenAI-compatible image endpoints.

use axum::Json;
use serde_json::json;

/// Returns the image-model listing compatible with the OpenAI models API shape.
pub async fn list_models() -> Json<serde_json::Value> {
    Json(json!({
        "object": "list",
        "data": [
            {"id": "gpt-image-1", "object": "model", "created": 0, "owned_by": "gpt2api-rs"},
            {"id": "gpt-image-2", "object": "model", "created": 0, "owned_by": "gpt2api-rs"}
        ]
    }))
}
