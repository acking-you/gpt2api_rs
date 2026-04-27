//! Public OpenAI-compatible image endpoints.

use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{
    body::{Body, Bytes},
    extract::{Multipart, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    error::AppError,
    models::ApiKeyRecord,
    service::{
        AppService, ImageEditInput, ImageEditSubmission, ImageGenerationSubmission,
        PublicAuthError, PublicAuthFailure, RequestLogContext,
    },
    upstream::chatgpt::{ChatgptImageResult, ChatgptTextResult, ChatgptTextStream},
};

/// Request body for `/v1/images/generations`.
#[derive(Debug, Deserialize)]
pub struct ImageGenerationRequest {
    /// Prompt text.
    pub prompt: String,
    /// Requested image model.
    #[serde(default = "default_image_model")]
    pub model: String,
    /// Requested image count.
    #[serde(default = "default_image_count")]
    pub n: usize,
    /// Requested output size.
    #[serde(default = "default_image_size")]
    pub size: String,
}

fn default_image_model() -> String {
    "gpt-image-2".to_string()
}

const fn default_image_count() -> usize {
    1
}

fn default_image_size() -> String {
    "1024x1024".to_string()
}

/// Returns the binary version string.
pub async fn version() -> Json<Value> {
    Json(json!({ "version": env!("CARGO_PKG_VERSION") }))
}

/// Returns the image-model listing compatible with the OpenAI models API shape.
pub async fn list_models() -> Json<Value> {
    Json(json!({
        "object": "list",
        "data": [
            {"id": "auto", "object": "model", "created": 0, "owned_by": "gpt2api-rs"},
            {"id": "gpt-5", "object": "model", "created": 0, "owned_by": "gpt2api-rs"},
            {"id": "gpt-5-mini", "object": "model", "created": 0, "owned_by": "gpt2api-rs"},
            {"id": "gpt-image-1", "object": "model", "created": 0, "owned_by": "gpt2api-rs"},
            {"id": "gpt-image-2", "object": "model", "created": 0, "owned_by": "gpt2api-rs"}
        ]
    }))
}

/// Validates the downstream auth key and returns a success marker.
pub async fn login(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_key(&service, &headers).await?;
    let key = service.key_with_ledger_usage(&key).await.map_err(AppError::internal)?;
    Ok(Json(json!({
    "ok": true,
    "version": env!("CARGO_PKG_VERSION"),
    "key": {
        "id": key.id,
        "name": key.name,
        "status": key.status,
        "quota_total_calls": key.quota_total_calls,
            "quota_used_calls": key.quota_used_calls,
            "route_strategy": key.route_strategy,
            "role": key.role.as_str(),
            "notification_email": key.notification_email,
            "notification_enabled": key.notification_enabled,
        }
    })))
}

/// Handles `/v1/images/generations`.
pub async fn generate_images(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<ImageGenerationRequest>,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_key(&service, &headers).await?;
    let n = validate_image_count(body.n)?;
    let session = service
        .resolve_direct_image_api_session(&key, extract_session_header(&headers).as_deref())
        .await
        .map_err(map_public_request_error)?;
    let submission = service
        .submit_image_generation_message_with_context(
            &key,
            &session.id,
            body.prompt.trim(),
            body.model.trim(),
            n,
            ImageGenerationSubmission {
                size: body.size.trim().to_string(),
                request_context: request_context_from_headers(
                    &headers,
                    "POST",
                    "/v1/images/generations",
                ),
            },
        )
        .await
        .map_err(map_public_request_error)?
        .ok_or_else(|| AppError::bad_request("session not found"))?;
    let completed = service
        .wait_for_image_task(&submission.task.id, Duration::from_secs(180))
        .await
        .map_err(map_public_request_error)?;
    let result =
        service.image_result_for_task(&completed).await.map_err(map_public_request_error)?;
    Ok(Json(image_generation_json(result)))
}

/// Handles `/v1/images/edits`.
pub async fn edit_images(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_key(&service, &headers).await?;
    let mut prompt = String::new();
    let mut model = default_image_model();
    let mut n = default_image_count();
    let mut size = default_image_size();
    let mut image_data = None;
    let mut file_name = "image.png".to_string();
    let mut mime_type = "image/png".to_string();

    while let Some(field) =
        multipart.next_field().await.map_err(|error| AppError::bad_request(error.to_string()))?
    {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "prompt" => {
                prompt =
                    field.text().await.map_err(|error| AppError::bad_request(error.to_string()))?
            }
            "model" => {
                model =
                    field.text().await.map_err(|error| AppError::bad_request(error.to_string()))?
            }
            "n" => {
                n = field
                    .text()
                    .await
                    .map_err(|error| AppError::bad_request(error.to_string()))?
                    .parse::<usize>()
                    .map_err(|_| AppError::bad_request("n must be an integer"))?;
            }
            "size" => {
                size =
                    field.text().await.map_err(|error| AppError::bad_request(error.to_string()))?
            }
            "image" => {
                file_name = field.file_name().unwrap_or("image.png").to_string();
                if let Some(content_type) = field.content_type() {
                    mime_type = content_type.to_string();
                }
                image_data = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|error| AppError::bad_request(error.to_string()))?
                        .to_vec(),
                );
            }
            _ => {}
        }
    }

    let image_data = image_data
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::bad_request("image file is required"))?;
    let session = service
        .resolve_direct_image_api_session(&key, extract_session_header(&headers).as_deref())
        .await
        .map_err(map_public_request_error)?;
    let submission = service
        .submit_image_edit_message_with_context(
            &key,
            &session.id,
            prompt.trim(),
            model.trim(),
            validate_image_count(n)?,
            ImageEditSubmission {
                edit_input: ImageEditInput { image_data, file_name, mime_type },
                size,
                request_context: request_context_from_headers(&headers, "POST", "/v1/images/edits"),
            },
        )
        .await
        .map_err(map_public_request_error)?
        .ok_or_else(|| AppError::bad_request("session not found"))?;
    let completed = service
        .wait_for_image_task(&submission.task.id, Duration::from_secs(180))
        .await
        .map_err(map_public_request_error)?;
    let result =
        service.image_result_for_task(&completed).await.map_err(map_public_request_error)?;
    Ok(Json(image_generation_json(result)))
}

/// Handles `/v1/chat/completions` for image-generation shaped requests.
pub async fn create_chat_completion(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let key = authenticate_key(&service, &headers).await?;
    let stream = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let prompt = extract_chat_prompt(&body);
    if prompt.is_empty() {
        return Err(AppError::bad_request("prompt is required"));
    }
    let model = body.get("model").and_then(Value::as_str).unwrap_or("auto");

    if is_image_chat_request(&body) {
        if stream {
            return Err(AppError::bad_request("stream is not supported for image generation"));
        }
        let n = validate_image_count(body.get("n").and_then(Value::as_u64).unwrap_or(1) as usize)?;
        let size = body.get("size").and_then(Value::as_str).unwrap_or("1024x1024");
        let session = service
            .resolve_direct_image_api_session(&key, extract_session_header(&headers).as_deref())
            .await
            .map_err(map_public_request_error)?;
        let submission = match extract_chat_image(&body)? {
            Some(edit_input) => {
                service
                    .submit_image_edit_message_with_context(
                        &key,
                        &session.id,
                        &prompt,
                        model,
                        n,
                        ImageEditSubmission {
                            edit_input,
                            size: size.to_string(),
                            request_context: request_context_from_headers(
                                &headers,
                                "POST",
                                "/v1/chat/completions",
                            ),
                        },
                    )
                    .await
            }
            None => {
                service
                    .submit_image_generation_message_with_context(
                        &key,
                        &session.id,
                        &prompt,
                        model,
                        n,
                        ImageGenerationSubmission {
                            size: size.to_string(),
                            request_context: request_context_from_headers(
                                &headers,
                                "POST",
                                "/v1/chat/completions",
                            ),
                        },
                    )
                    .await
            }
        }
        .map_err(map_public_request_error)?
        .ok_or_else(|| AppError::bad_request("session not found"))?;
        let completed = service
            .wait_for_image_task(&submission.task.id, Duration::from_secs(180))
            .await
            .map_err(map_public_request_error)?;
        let result =
            service.image_result_for_task(&completed).await.map_err(map_public_request_error)?;
        return Ok(Json(service.build_chat_completion_response(model, &result)).into_response());
    }

    let session = service
        .resolve_api_session(&key, extract_session_header(&headers).as_deref())
        .await
        .map_err(map_public_request_error)?;
    let user_message = service
        .append_api_user_text_message(&key, &session.id, &prompt, model, "/v1/chat/completions")
        .await
        .map_err(map_public_request_error)?;
    if stream {
        let upstream = match service
            .start_text_stream_for_key_with_context(
                &key,
                &prompt,
                model,
                "/v1/chat/completions",
                request_context_from_headers(&headers, "POST", "/v1/chat/completions"),
            )
            .await
        {
            Ok(upstream) => upstream,
            Err(error) => {
                let _ = service
                    .append_api_failed_assistant_message(
                        &key,
                        &session.id,
                        &user_message.id,
                        &error.to_string(),
                    )
                    .await;
                return Err(map_public_request_error(error));
            }
        };
        return build_streaming_chat_completion_response(
            model,
            upstream,
            Some(StreamPersistence {
                service,
                key,
                session_id: session.id,
                user_message_id: user_message.id,
            }),
        );
    }

    let result = match service
        .complete_text_for_key_with_context(
            &key,
            &prompt,
            model,
            "/v1/chat/completions",
            request_context_from_headers(&headers, "POST", "/v1/chat/completions"),
        )
        .await
    {
        Ok(result) => result,
        Err(error) => {
            let _ = service
                .append_api_failed_assistant_message(
                    &key,
                    &session.id,
                    &user_message.id,
                    &error.to_string(),
                )
                .await;
            return Err(map_public_request_error(error));
        }
    };
    service
        .append_api_assistant_text_message(&key, &session.id, &user_message.id, &result)
        .await
        .map_err(map_public_request_error)?;
    Ok(Json(service.build_text_chat_completion_response(model, &result)).into_response())
}

/// Handles `/v1/responses` for image-generation shaped requests.
pub async fn create_response(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let key = authenticate_key(&service, &headers).await?;
    if body.get("stream").and_then(Value::as_bool).unwrap_or(false) {
        return Err(AppError::bad_request("stream is not supported"));
    }
    let prompt = extract_response_prompt(body.get("input"));
    if prompt.is_empty() {
        return Err(AppError::bad_request("input text is required"));
    }
    let model = body.get("model").and_then(Value::as_str).unwrap_or("gpt-5");
    if has_response_image_generation_tool(&body) {
        let size = body.get("size").and_then(Value::as_str).unwrap_or("1024x1024");
        let session = service
            .resolve_direct_image_api_session(&key, extract_session_header(&headers).as_deref())
            .await
            .map_err(map_public_request_error)?;
        let submission = match extract_response_image(body.get("input"))? {
            Some(edit_input) => {
                service
                    .submit_image_edit_message_with_context(
                        &key,
                        &session.id,
                        &prompt,
                        "gpt-image-2",
                        1,
                        ImageEditSubmission {
                            edit_input,
                            size: size.to_string(),
                            request_context: request_context_from_headers(
                                &headers,
                                "POST",
                                "/v1/responses",
                            ),
                        },
                    )
                    .await
            }
            None => {
                service
                    .submit_image_generation_message_with_context(
                        &key,
                        &session.id,
                        &prompt,
                        "gpt-image-2",
                        1,
                        ImageGenerationSubmission {
                            size: size.to_string(),
                            request_context: request_context_from_headers(
                                &headers,
                                "POST",
                                "/v1/responses",
                            ),
                        },
                    )
                    .await
            }
        }
        .map_err(map_public_request_error)?
        .ok_or_else(|| AppError::bad_request("session not found"))?;
        let completed = service
            .wait_for_image_task(&submission.task.id, Duration::from_secs(180))
            .await
            .map_err(map_public_request_error)?;
        let result =
            service.image_result_for_task(&completed).await.map_err(map_public_request_error)?;
        return Ok(Json(service.build_responses_api_response(model, &result)).into_response());
    }

    let session = service
        .resolve_api_session(&key, extract_session_header(&headers).as_deref())
        .await
        .map_err(map_public_request_error)?;
    let user_message = service
        .append_api_user_text_message(&key, &session.id, &prompt, model, "/v1/responses")
        .await
        .map_err(map_public_request_error)?;
    let result = match service
        .complete_text_for_key_with_context(
            &key,
            &prompt,
            model,
            "/v1/responses",
            request_context_from_headers(&headers, "POST", "/v1/responses"),
        )
        .await
    {
        Ok(result) => result,
        Err(error) => {
            let _ = service
                .append_api_failed_assistant_message(
                    &key,
                    &session.id,
                    &user_message.id,
                    &error.to_string(),
                )
                .await;
            return Err(map_public_request_error(error));
        }
    };
    service
        .append_api_assistant_text_message(&key, &session.id, &user_message.id, &result)
        .await
        .map_err(map_public_request_error)?;
    Ok(Json(service.build_text_responses_api_response(model, &result)).into_response())
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn extract_session_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-gpt2api-session-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn request_context_from_headers(headers: &HeaderMap, method: &str, url: &str) -> RequestLogContext {
    RequestLogContext {
        method: method.to_string(),
        url: url.to_string(),
        client_ip: extract_client_ip(headers),
        request_headers_json: Some(sanitized_headers_json(headers)),
    }
}

fn extract_client_ip(headers: &HeaderMap) -> String {
    for name in ["cf-connecting-ip", "x-real-ip", "x-client-ip"] {
        if let Some(value) = header_value(headers, name) {
            return value;
        }
    }
    header_value(headers, "x-forwarded-for")
        .and_then(|value| value.split(',').next().map(str::trim).map(ToString::to_string))
        .filter(|value| !value.is_empty())
        .or_else(|| header_value(headers, "forwarded").and_then(parse_forwarded_for))
        .unwrap_or_default()
}

fn parse_forwarded_for(value: String) -> Option<String> {
    value.split(';').find_map(|part| {
        let (name, raw) = part.split_once('=')?;
        if !name.trim().eq_ignore_ascii_case("for") {
            return None;
        }
        Some(raw.trim_matches('"').trim().to_string()).filter(|item| !item.is_empty())
    })
}

fn sanitized_headers_json(headers: &HeaderMap) -> String {
    let mut value = serde_json::Map::new();
    for name in [
        "user-agent",
        "x-forwarded-for",
        "x-real-ip",
        "x-client-ip",
        "cf-connecting-ip",
        "forwarded",
        "referer",
        "origin",
    ] {
        if let Some(header) = header_value(headers, name) {
            value.insert(name.to_string(), json!(header));
        }
    }
    Value::Object(value).to_string()
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

async fn authenticate_key(
    service: &Arc<AppService>,
    headers: &HeaderMap,
) -> Result<crate::models::ApiKeyRecord, AppError> {
    let bearer = extract_bearer_token(headers)
        .ok_or_else(|| map_public_auth_error(PublicAuthError::InvalidKey))?;
    service.authenticate_public_key(&bearer).await.map_err(map_public_auth_failure)
}

fn map_public_auth_error(error: PublicAuthError) -> AppError {
    match error {
        PublicAuthError::InvalidKey => {
            AppError::with_status(StatusCode::UNAUTHORIZED, error.to_string())
        }
        PublicAuthError::Disabled | PublicAuthError::QuotaExhausted => {
            AppError::with_status(StatusCode::FORBIDDEN, error.to_string())
        }
    }
}

fn map_public_auth_failure(error: PublicAuthFailure) -> AppError {
    match error {
        PublicAuthFailure::Auth(error) => map_public_auth_error(error),
        PublicAuthFailure::Internal(error) => AppError::internal(error),
    }
}

fn map_public_request_error(error: anyhow::Error) -> AppError {
    match error.to_string().as_str() {
        "invalid_key" => map_public_auth_error(PublicAuthError::InvalidKey),
        "disabled" => map_public_auth_error(PublicAuthError::Disabled),
        "quota_exhausted" => map_public_auth_error(PublicAuthError::QuotaExhausted),
        "x-gpt2api-session-id does not belong to this key" => {
            AppError::bad_request("x-gpt2api-session-id does not belong to this key")
        }
        _ => AppError::upstream(error),
    }
}

fn image_generation_json(result: ChatgptImageResult) -> Value {
    json!({
        "created": result.created,
        "data": result.data.into_iter().map(|item| json!({
            "b64_json": item.b64_json,
            "revised_prompt": item.revised_prompt,
        })).collect::<Vec<_>>(),
    })
}

#[derive(Clone)]
struct StreamPersistence {
    service: Arc<AppService>,
    key: ApiKeyRecord,
    session_id: String,
    user_message_id: String,
}

fn build_streaming_chat_completion_response(
    requested_model: &str,
    upstream: ChatgptTextStream,
    persistence: Option<StreamPersistence>,
) -> Result<Response, AppError> {
    let stream_id = format!("chatcmpl-{}", Uuid::new_v4().simple());
    let created = upstream.created;
    let resolved_model = upstream.resolved_model;
    let model_name = response_model_name(requested_model, &resolved_model);
    let mut upstream_response = upstream.response;
    let stream = async_stream::stream! {
        let mut state = StreamingChatState::new(stream_id, created, model_name);
        let mut buffer = Vec::new();
        let mut stream_error = None;
        loop {
            match upstream_response.chunk().await {
                Ok(Some(chunk)) => {
                    buffer.extend_from_slice(&chunk);
                    while let Some(event) = take_next_sse_event(&mut buffer) {
                        for payload in transcode_upstream_event(&event, &mut state) {
                            yield Ok::<Bytes, Infallible>(Bytes::from(payload));
                        }
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(error) => {
                    stream_error = Some(error.to_string());
                    yield Ok::<Bytes, Infallible>(Bytes::from(format!(
                        "event: error\ndata: {}\n\n",
                        json!({ "error": error.to_string() })
                    )));
                    break;
                }
            }
        }
        if !buffer.is_empty() {
            let event = String::from_utf8_lossy(&buffer).to_string();
            for payload in transcode_upstream_event(&event, &mut state) {
                yield Ok::<Bytes, Infallible>(Bytes::from(payload));
            }
        }
        if !state.finished {
            yield Ok::<Bytes, Infallible>(Bytes::from(state.finish_payload()));
            yield Ok::<Bytes, Infallible>(Bytes::from_static(b"data: [DONE]\n\n"));
        }
        if let Some(persistence) = persistence {
            if let Some(error) = stream_error {
                let _ = persistence
                    .service
                    .append_api_failed_assistant_message(
                        &persistence.key,
                        &persistence.session_id,
                        &persistence.user_message_id,
                        &error,
                    )
                    .await;
            } else {
                let result = ChatgptTextResult {
                    created,
                    text: state.last_text,
                    resolved_model,
                };
                let _ = persistence
                    .service
                    .append_api_assistant_text_message(
                        &persistence.key,
                        &persistence.session_id,
                        &persistence.user_message_id,
                        &result,
                    )
                    .await;
            }
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .body(Body::from_stream(stream))
        .map_err(AppError::internal)
}

fn validate_image_count(value: usize) -> Result<usize, AppError> {
    if !(1..=4).contains(&value) {
        return Err(AppError::bad_request("n must be between 1 and 4"));
    }
    Ok(value)
}

fn is_image_chat_request(body: &Value) -> bool {
    let model = body.get("model").and_then(Value::as_str).unwrap_or_default().trim();
    if matches!(model, "gpt-image-1" | "gpt-image-2") {
        return true;
    }
    body.get("modalities")
        .and_then(Value::as_array)
        .map(|items| {
            items.iter().any(|item| item.as_str().unwrap_or_default().eq_ignore_ascii_case("image"))
        })
        .unwrap_or(false)
}

fn extract_chat_prompt(body: &Value) -> String {
    if let Some(prompt) = body.get("prompt").and_then(Value::as_str) {
        if !prompt.trim().is_empty() {
            return prompt.trim().to_string();
        }
    }
    body.get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|message| {
            message
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .eq_ignore_ascii_case("user")
        })
        .filter_map(|message| extract_prompt_from_message_content(message.get("content")))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

fn extract_response_prompt(input: Option<&Value>) -> String {
    match input {
        Some(Value::String(text)) => text.trim().to_string(),
        Some(Value::Object(message)) => {
            if let Some(role) = message.get("role").and_then(Value::as_str) {
                if !role.eq_ignore_ascii_case("user") {
                    return String::new();
                }
            }
            extract_prompt_from_message_content(message.get("content")).unwrap_or_default()
        }
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| match item {
                Value::Object(object)
                    if object.get("type").and_then(Value::as_str) == Some("input_text") =>
                {
                    object.get("text").and_then(Value::as_str).map(|text| text.trim().to_string())
                }
                Value::Object(object) => {
                    if let Some(role) = object.get("role").and_then(Value::as_str) {
                        if !role.eq_ignore_ascii_case("user") {
                            return None;
                        }
                    }
                    extract_prompt_from_message_content(object.get("content"))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string(),
        _ => String::new(),
    }
}

fn has_response_image_generation_tool(body: &Value) -> bool {
    body.get("tools")
        .and_then(Value::as_array)
        .map(|tools| {
            tools
                .iter()
                .any(|tool| tool.get("type").and_then(Value::as_str) == Some("image_generation"))
        })
        .unwrap_or(false)
        || body
            .get("tool_choice")
            .and_then(Value::as_object)
            .and_then(|tool| tool.get("type"))
            .and_then(Value::as_str)
            == Some("image_generation")
}

fn extract_prompt_from_message_content(content: Option<&Value>) -> Option<String> {
    match content {
        Some(Value::String(text)) => Some(text.trim().to_string()),
        Some(Value::Array(parts)) => {
            let prompt = parts
                .iter()
                .filter_map(|item| {
                    let object = item.as_object()?;
                    match object.get("type").and_then(Value::as_str).unwrap_or_default() {
                        "text" => object
                            .get("text")
                            .and_then(Value::as_str)
                            .map(|text| text.trim().to_string()),
                        "input_text" => object
                            .get("text")
                            .or_else(|| object.get("input_text"))
                            .and_then(Value::as_str)
                            .map(|text| text.trim().to_string()),
                        _ => None,
                    }
                })
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n");
            if prompt.trim().is_empty() {
                None
            } else {
                Some(prompt.trim().to_string())
            }
        }
        _ => None,
    }
}

fn extract_chat_image(body: &Value) -> Result<Option<ImageEditInput>, AppError> {
    let Some(messages) = body.get("messages").and_then(Value::as_array) else {
        return Ok(None);
    };
    for message in messages.iter().rev() {
        if message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .eq_ignore_ascii_case("user")
        {
            if let Some(edit_input) = extract_image_from_message_content(message.get("content"))? {
                return Ok(Some(edit_input));
            }
        }
    }
    Ok(None)
}

fn extract_response_image(input: Option<&Value>) -> Result<Option<ImageEditInput>, AppError> {
    match input {
        Some(Value::Object(object)) => extract_image_from_message_content(object.get("content")),
        Some(Value::Array(items)) => {
            for item in items.iter().rev() {
                if let Some(object) = item.as_object() {
                    if object.get("type").and_then(Value::as_str) == Some("input_image") {
                        let data_url =
                            object.get("image_url").and_then(Value::as_str).unwrap_or_default();
                        return decode_data_url(data_url).map(Some);
                    }
                    if let Some(edit_input) =
                        extract_image_from_message_content(object.get("content"))?
                    {
                        return Ok(Some(edit_input));
                    }
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn extract_image_from_message_content(
    content: Option<&Value>,
) -> Result<Option<ImageEditInput>, AppError> {
    let Some(Value::Array(parts)) = content else {
        return Ok(None);
    };
    for item in parts {
        let Some(object) = item.as_object() else {
            continue;
        };
        match object.get("type").and_then(Value::as_str).unwrap_or_default() {
            "image_url" => {
                let url = object
                    .get("image_url")
                    .and_then(Value::as_object)
                    .and_then(|url| url.get("url"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                return decode_data_url(url).map(Some);
            }
            "input_image" => {
                let url = object.get("image_url").and_then(Value::as_str).unwrap_or_default();
                return decode_data_url(url).map(Some);
            }
            _ => {}
        }
    }
    Ok(None)
}

fn decode_data_url(data_url: &str) -> Result<ImageEditInput, AppError> {
    let Some(rest) = data_url.strip_prefix("data:") else {
        return Err(AppError::bad_request("only data URL images are supported"));
    };
    let (header, data) =
        rest.split_once(',').ok_or_else(|| AppError::bad_request("invalid data URL image"))?;
    let mime_type = header.split(';').next().unwrap_or("image/png").to_string();
    let image_data = BASE64
        .decode(data.as_bytes())
        .map_err(|_| AppError::bad_request("invalid base64 image payload"))?;
    Ok(ImageEditInput { image_data, file_name: "image.png".to_string(), mime_type })
}

struct StreamingChatState {
    id: String,
    created: i64,
    model: String,
    last_text: String,
    sent_role: bool,
    finished: bool,
}

impl StreamingChatState {
    fn new(id: String, created: i64, model: String) -> Self {
        Self { id, created, model, last_text: String::new(), sent_role: false, finished: false }
    }

    fn chunk_payload(&mut self, delta_content: String) -> String {
        let mut delta = serde_json::Map::new();
        if !self.sent_role {
            delta.insert("role".to_string(), Value::String("assistant".to_string()));
            self.sent_role = true;
        }
        delta.insert("content".to_string(), Value::String(delta_content));
        format!(
            "data: {}\n\n",
            json!({
                "id": self.id,
                "object": "chat.completion.chunk",
                "created": self.created,
                "model": self.model,
                "choices": [{
                    "index": 0,
                    "delta": Value::Object(delta),
                    "finish_reason": Value::Null,
                }],
            })
        )
    }

    fn finish_payload(&mut self) -> String {
        self.finished = true;
        format!(
            "data: {}\n\n",
            json!({
                "id": self.id,
                "object": "chat.completion.chunk",
                "created": self.created,
                "model": self.model,
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop",
                }],
            })
        )
    }
}

fn response_model_name(requested_model: &str, resolved_model: &str) -> String {
    let requested_model = requested_model.trim();
    if requested_model.is_empty() {
        resolved_model.to_string()
    } else {
        requested_model.to_string()
    }
}

fn take_next_sse_event(buffer: &mut Vec<u8>) -> Option<String> {
    let (index, delimiter_len) = find_sse_delimiter(buffer)?;
    let event = String::from_utf8_lossy(&buffer[..index]).to_string();
    buffer.drain(..index + delimiter_len);
    Some(event)
}

fn find_sse_delimiter(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n").map(|index| (index, 2));
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n").map(|index| (index, 4));
    match (lf, crlf) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn transcode_upstream_event(event: &str, state: &mut StreamingChatState) -> Vec<String> {
    let mut payloads = Vec::new();
    let payload = extract_sse_data(event);
    if payload.is_empty() {
        return payloads;
    }
    if payload == "[DONE]" {
        if !state.finished {
            payloads.push(state.finish_payload());
        }
        payloads.push("data: [DONE]\n\n".to_string());
        return payloads;
    }
    let Ok(value) = serde_json::from_str::<Value>(&payload) else {
        return payloads;
    };
    let Some(text_snapshot) = extract_assistant_text_snapshot(&value) else {
        return payloads;
    };
    let delta = if state.last_text.is_empty() {
        text_snapshot.clone()
    } else if text_snapshot.starts_with(&state.last_text) {
        text_snapshot[state.last_text.len()..].to_string()
    } else {
        text_snapshot.clone()
    };
    state.last_text = text_snapshot;
    if delta.is_empty() {
        return payloads;
    }
    payloads.push(state.chunk_payload(delta));
    payloads
}

fn extract_sse_data(event: &str) -> String {
    event
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|payload| !payload.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn extract_assistant_text_snapshot(value: &Value) -> Option<String> {
    for message in [value.get("message"), value.get("v").and_then(|inner| inner.get("message"))] {
        let Some(message) = message.and_then(Value::as_object) else {
            continue;
        };
        if message
            .get("author")
            .and_then(Value::as_object)
            .and_then(|author| author.get("role"))
            .and_then(Value::as_str)
            != Some("assistant")
        {
            continue;
        }
        let Some(content) = message.get("content").and_then(Value::as_object) else {
            continue;
        };
        let text = match content.get("content_type").and_then(Value::as_str).unwrap_or_default() {
            "text" | "multimodal_text" => content
                .get("parts")
                .and_then(Value::as_array)
                .map(|parts| extract_text_parts(parts))
                .unwrap_or_default(),
            _ => String::new(),
        };
        if !text.is_empty() {
            return Some(text);
        }
    }
    None
}

fn extract_text_parts(parts: &[Value]) -> String {
    parts
        .iter()
        .filter_map(|part| match part {
            Value::String(text) => Some(text.trim().to_string()),
            Value::Object(object) => object
                .get("text")
                .or_else(|| object.get("content"))
                .and_then(Value::as_str)
                .map(|text| text.trim().to_string()),
            _ => None,
        })
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}
