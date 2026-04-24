//! Product workspace APIs authenticated by downstream API keys.

use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    error::AppError,
    models::{ApiKeyRecord, ApiKeyRole, SessionDetail},
    service::{AppService, ImageEditInput, PublicAuthError, PublicAuthFailure},
};

/// Query parameters for session listings.
#[derive(Debug, Default, Deserialize)]
pub struct SessionListQuery {
    /// Optional maximum number of sessions.
    #[serde(default)]
    limit: Option<u64>,
    /// Optional updated-at cursor.
    #[serde(default)]
    cursor: Option<i64>,
    /// Optional admin key filter.
    #[serde(default)]
    key_id: Option<String>,
    /// Optional admin title search.
    #[serde(default)]
    q: Option<String>,
}

/// Body for creating one session.
#[derive(Debug, Default, Deserialize)]
pub struct CreateSessionRequest {
    /// Optional session title.
    #[serde(default)]
    title: Option<String>,
}

/// Body for updating notification settings.
#[derive(Debug, Default, Deserialize)]
pub struct NotificationRequest {
    /// Optional notification email. Empty string clears it.
    #[serde(default)]
    notification_email: Option<String>,
    /// Optional notification toggle.
    #[serde(default)]
    notification_enabled: Option<bool>,
}

/// Body for patching one session.
#[derive(Debug, Default, Deserialize)]
pub struct PatchSessionRequest {
    /// Optional title replacement.
    #[serde(default)]
    title: Option<String>,
    /// Optional status replacement.
    #[serde(default)]
    status: Option<String>,
}

/// Body for appending one message to a session.
#[derive(Debug, Default, Deserialize)]
pub struct CreateMessageRequest {
    /// Message kind: text or image_generation.
    #[serde(default)]
    kind: String,
    /// Text content for text messages.
    #[serde(default)]
    text: Option<String>,
    /// Image prompt for generation messages.
    #[serde(default)]
    prompt: Option<String>,
    /// Requested model.
    #[serde(default)]
    model: Option<String>,
    /// Requested image count.
    #[serde(default)]
    n: Option<usize>,
}

/// Verifies a product API key.
pub async fn verify_auth(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    Ok(Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "key": serialize_product_key(&key),
    })))
}

/// Returns the current key profile.
pub async fn get_me(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    Ok(Json(json!({ "key": serialize_product_key(&key) })))
}

/// Updates current key notification settings.
pub async fn update_my_notification(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<NotificationRequest>,
) -> Result<Json<Value>, AppError> {
    let mut key = authenticate_product_key(&service, &headers).await?;
    if let Some(email) = body.notification_email {
        key.notification_email = normalize_optional_email(&email);
    }
    if let Some(enabled) = body.notification_enabled {
        key.notification_enabled = enabled;
    }
    service.storage().control.upsert_api_key(&key).await.map_err(AppError::internal)?;
    Ok(Json(json!({ "key": serialize_product_key(&key) })))
}

/// Lists sessions for the current key.
pub async fn list_sessions(
    Query(query): Query<SessionListQuery>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let items = service
        .storage()
        .control
        .list_sessions_for_key(&key.id, query.limit.unwrap_or(50), query.cursor)
        .await
        .map_err(AppError::internal)?;
    Ok(Json(json!({ "items": items })))
}

/// Creates a web session for the current key.
pub async fn create_session(
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<CreateSessionRequest>,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let session = service
        .create_web_session(&key, body.title.as_deref())
        .await
        .map_err(AppError::internal)?;
    Ok(Json(json!({ "session": session })))
}

/// Returns one key-scoped session detail.
pub async fn get_session(
    Path(session_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<SessionDetail>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let detail = service
        .get_session_detail_for_key(&key, &session_id)
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("session not found"))?;
    Ok(Json(detail))
}

/// Patches one key-scoped session.
pub async fn patch_session(
    Path(session_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<PatchSessionRequest>,
) -> Result<Json<SessionDetail>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    if body.title.is_none() && body.status.is_none() {
        let detail = service
            .get_session_detail_for_key(&key, &session_id)
            .await
            .map_err(AppError::internal)?
            .ok_or_else(|| AppError::not_found("session not found"))?;
        return Ok(Json(detail));
    }
    let detail = service
        .patch_session_for_key(&key, &session_id, body.title.as_deref(), body.status.as_deref())
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("session not found"))?;
    Ok(Json(detail))
}

/// Appends one product message to a key-scoped session.
pub async fn create_message(
    Path(session_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    Json(body): Json<CreateMessageRequest>,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    match body.kind.as_str() {
        "text" => {
            let text = body.text.as_deref().unwrap_or_default();
            let model = body.model.as_deref().unwrap_or("gpt-5");
            let detail = service
                .submit_text_message(&key, &session_id, text, model)
                .await
                .map_err(map_product_request_error)?
                .ok_or_else(|| AppError::not_found("session not found"))?;
            Ok(Json(json!(detail)))
        }
        "image_generation" => {
            let prompt = body.prompt.as_deref().unwrap_or_default();
            let model = body.model.as_deref().unwrap_or("gpt-image-1");
            let n = validate_image_count(body.n.unwrap_or(1))?;
            let result = service
                .submit_image_generation_message(&key, &session_id, prompt, model, n)
                .await
                .map_err(map_product_request_error)?
                .ok_or_else(|| AppError::not_found("session not found"))?;
            Ok(Json(json!(result)))
        }
        _ => Err(AppError::bad_request("kind must be text or image_generation")),
    }
}

/// Appends one multipart image-edit message to a key-scoped session.
pub async fn create_edit_message(
    Path(session_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let mut prompt = String::new();
    let mut model = "gpt-image-1".to_string();
    let mut n = 1_usize;
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
    let result = service
        .submit_image_edit_message(
            &key,
            &session_id,
            &prompt,
            &model,
            validate_image_count(n)?,
            ImageEditInput { image_data, file_name, mime_type },
        )
        .await
        .map_err(map_product_request_error)?
        .ok_or_else(|| AppError::not_found("session not found"))?;
    Ok(Json(json!(result)))
}

/// Streams session snapshots and task progress events.
pub async fn session_events(
    Path(session_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let detail = service
        .get_session_detail_for_key(&key, &session_id)
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("session not found"))?;
    let cursor = service
        .storage()
        .control
        .max_task_event_sequence_for_session(&session_id)
        .await
        .map_err(AppError::internal)?;
    let snapshot = serde_json::to_string(&detail).map_err(AppError::internal)?;
    let storage = service.storage();
    let stream_session_id = session_id;
    let stream = async_stream::stream! {
        yield Ok::<Event, Infallible>(Event::default().event("snapshot").data(snapshot));
        let mut cursor = cursor;
        loop {
            tokio::time::sleep(Duration::from_millis(750)).await;
            match storage
                .control
                .list_task_events_for_session_after(&stream_session_id, cursor, 100)
                .await
            {
                Ok(events) => {
                    for event in events {
                        cursor = cursor.max(event.sequence);
                        match serde_json::to_string(&event) {
                            Ok(data) => {
                                yield Ok(Event::default().event("task_event").data(data));
                            }
                            Err(error) => {
                                yield Ok(Event::default().event("error").data(error.to_string()));
                            }
                        }
                    }
                }
                Err(error) => {
                    yield Ok(Event::default().event("error").data(error.to_string()));
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()).into_response())
}

/// Returns one signed image-task share payload.
pub async fn get_share(
    Path(token): Path<String>,
    State(service): State<Arc<AppService>>,
) -> Result<Json<Value>, AppError> {
    let link = service
        .storage()
        .control
        .resolve_signed_link(&token, unix_timestamp_secs())
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("share link not found"))?;
    if link.scope != "image_task" {
        return Err(AppError::not_found("share link not found"));
    }
    let task = service
        .storage()
        .control
        .get_image_task(&link.scope_id)
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("task not found"))?;
    let session = service
        .storage()
        .control
        .get_session_for_admin(&task.session_id)
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("session not found"))?;
    let messages = service
        .storage()
        .control
        .list_messages_for_session(&task.session_id)
        .await
        .map_err(AppError::internal)?;
    let artifacts = service
        .storage()
        .control
        .list_artifacts_for_session(&task.session_id)
        .await
        .map_err(AppError::internal)?
        .into_iter()
        .filter(|artifact| artifact.task_id == task.id)
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "scope": link.scope,
        "session": session,
        "task": task,
        "messages": messages,
        "artifacts": artifacts,
    })))
}

/// Streams one artifact belonging to a signed image-task share.
pub async fn get_shared_artifact(
    Path((token, artifact_id)): Path<(String, String)>,
    State(service): State<Arc<AppService>>,
) -> Result<Response, AppError> {
    let link = service
        .storage()
        .control
        .resolve_signed_link(&token, unix_timestamp_secs())
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("share link not found"))?;
    if link.scope != "image_task" {
        return Err(AppError::not_found("share link not found"));
    }
    let task = service
        .storage()
        .control
        .get_image_task(&link.scope_id)
        .await
        .map_err(AppError::internal)?
        .ok_or_else(|| AppError::not_found("task not found"))?;
    let artifact = service
        .storage()
        .control
        .get_image_artifact(&artifact_id)
        .await
        .map_err(AppError::internal)?
        .filter(|artifact| artifact.task_id == task.id)
        .ok_or_else(|| AppError::not_found("artifact not found"))?;
    let bytes =
        service.storage().artifacts.read_artifact(&artifact).await.map_err(AppError::internal)?;
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, artifact.mime_type)
        .body(Body::from(bytes))
        .map_err(AppError::internal)
}

/// Returns one task visible to the current product key.
pub async fn get_task(
    Path(task_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let task = if service.is_product_admin(&key) {
        service.storage().control.get_image_task(&task_id).await
    } else {
        service.storage().control.get_image_task_for_key(&task_id, &key.id).await
    }
    .map_err(AppError::internal)?
    .ok_or_else(|| AppError::not_found("task not found"))?;
    Ok(Json(json!({ "task": task })))
}

/// Cancels one queued task visible to the current product key.
pub async fn cancel_task(
    Path(task_id): Path<String>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    let key_scope = if service.is_product_admin(&key) { None } else { Some(key.id.as_str()) };
    let cancelled = service
        .storage()
        .control
        .cancel_queued_image_task(&task_id, key_scope)
        .await
        .map_err(AppError::internal)?;
    Ok(Json(json!({ "cancelled": cancelled })))
}

/// Lists sessions across keys for product-admin API keys.
pub async fn list_admin_sessions(
    Query(query): Query<SessionListQuery>,
    State(service): State<Arc<AppService>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let key = authenticate_product_key(&service, &headers).await?;
    if !service.is_product_admin(&key) {
        return Err(AppError::with_status(StatusCode::FORBIDDEN, "admin role required"));
    }
    let items = service
        .storage()
        .control
        .search_sessions_for_admin(
            query.key_id.as_deref(),
            query.q.as_deref(),
            query.limit.unwrap_or(50),
            query.cursor,
        )
        .await
        .map_err(AppError::internal)?;
    Ok(Json(json!({ "items": items })))
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

async fn authenticate_product_key(
    service: &Arc<AppService>,
    headers: &HeaderMap,
) -> Result<ApiKeyRecord, AppError> {
    let bearer = extract_bearer_token(headers)
        .ok_or_else(|| map_product_auth_error(PublicAuthError::InvalidKey))?;
    service.authenticate_product_key(&bearer).await.map_err(map_product_auth_failure)
}

fn map_product_auth_failure(error: PublicAuthFailure) -> AppError {
    match error {
        PublicAuthFailure::Auth(error) => map_product_auth_error(error),
        PublicAuthFailure::Internal(error) => AppError::internal(error),
    }
}

fn map_product_auth_error(error: PublicAuthError) -> AppError {
    match error {
        PublicAuthError::InvalidKey => {
            AppError::with_status(StatusCode::UNAUTHORIZED, error.to_string())
        }
        PublicAuthError::Disabled | PublicAuthError::QuotaExhausted => {
            AppError::with_status(StatusCode::FORBIDDEN, error.to_string())
        }
    }
}

fn map_product_request_error(error: anyhow::Error) -> AppError {
    match error.to_string().as_str() {
        "disabled" => map_product_auth_error(PublicAuthError::Disabled),
        "quota_exhausted" => map_product_auth_error(PublicAuthError::QuotaExhausted),
        message if message.ends_with(" is required") => AppError::bad_request(message),
        _ => AppError::upstream(error),
    }
}

fn serialize_product_key(key: &ApiKeyRecord) -> Value {
    json!({
        "id": key.id,
        "name": key.name,
        "status": key.status,
        "role": match key.role {
            ApiKeyRole::User => "user",
            ApiKeyRole::Admin => "admin",
        },
        "quota_total_calls": key.quota_total_calls,
        "quota_used_calls": key.quota_used_calls,
        "route_strategy": key.route_strategy,
        "account_group_id": key.account_group_id,
        "request_max_concurrency": key.request_max_concurrency,
        "request_min_start_interval_ms": key.request_min_start_interval_ms,
        "notification_email": key.notification_email,
        "notification_enabled": key.notification_enabled,
    })
}

fn normalize_optional_email(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn validate_image_count(n: usize) -> Result<usize, AppError> {
    if (1..=4).contains(&n) {
        Ok(n)
    } else {
        Err(AppError::bad_request("n must be between 1 and 4"))
    }
}

fn unix_timestamp_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock before unix epoch").as_secs()
        as i64
}
