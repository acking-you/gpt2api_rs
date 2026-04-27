//! Router construction and app bootstrap.

use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{to_bytes, Body},
    extract::{Request, State},
    http::{header, HeaderMap, Method},
    middleware::{self, Next},
    response::Response,
    routing::{get, patch, post},
    Router,
};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    http::{admin_api, health::healthz, product_api, public_api},
    models::UsageEventRecord,
    service::AppService,
};

const MAX_LOGGED_REQUEST_BODY_BYTES: usize = 512 * 1024;

/// Builds the application router for a fully initialized runtime service.
pub fn build_router(service: Arc<AppService>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/version", get(public_api::version))
        .route("/auth/login", get(public_api::login).post(public_api::login))
        .route("/auth/verify", post(product_api::verify_auth))
        .route("/me", get(product_api::get_me))
        .route("/me/usage/events", get(product_api::get_my_usage_events))
        .route("/me/notification", patch(product_api::update_my_notification))
        .route("/sessions", get(product_api::list_sessions).post(product_api::create_session))
        .route(
            "/sessions/:session_id",
            get(product_api::get_session)
                .patch(product_api::patch_session)
                .delete(product_api::delete_session),
        )
        .route("/sessions/:session_id/events", get(product_api::session_events))
        .route("/sessions/:session_id/messages", post(product_api::create_message))
        .route("/sessions/:session_id/messages/edit", post(product_api::create_edit_message))
        .route("/tasks/:task_id", get(product_api::get_task))
        .route("/tasks/:task_id/cancel", post(product_api::cancel_task))
        .route("/artifacts/:artifact_id/thumbnail", get(product_api::get_artifact_thumbnail))
        .route("/artifacts/:artifact_id", get(product_api::get_artifact))
        .route("/share/:token", get(product_api::get_share))
        .route(
            "/share/:token/artifacts/:artifact_id/thumbnail",
            get(product_api::get_shared_artifact_thumbnail),
        )
        .route("/share/:token/artifacts/:artifact_id", get(product_api::get_shared_artifact))
        .route("/v1/models", get(public_api::list_models))
        .route("/v1/images/generations", post(public_api::generate_images))
        .route("/v1/images/edits", post(public_api::edit_images))
        .route("/v1/chat/completions", post(public_api::create_chat_completion))
        .route("/v1/responses", post(public_api::create_response))
        .route("/admin/sessions", get(product_api::list_admin_sessions))
        .route("/admin/queue", get(product_api::admin_queue))
        .route("/admin/queue/config", patch(product_api::patch_admin_queue_config))
        .route("/admin/tasks/:task_id/cancel", post(product_api::admin_cancel_task))
        .route("/admin/status", get(admin_api::status))
        .route("/admin/accounts", get(admin_api::list_accounts).delete(admin_api::delete_accounts))
        .route("/admin/accounts/import", post(admin_api::import_accounts))
        .route("/admin/accounts/refresh", post(admin_api::refresh_accounts))
        .route("/admin/accounts/update", post(admin_api::update_account))
        .route(
            "/admin/proxy-configs",
            get(admin_api::list_proxy_configs).post(admin_api::create_proxy_config),
        )
        .route(
            "/admin/proxy-configs/:proxy_id",
            patch(admin_api::update_proxy_config).delete(admin_api::delete_proxy_config),
        )
        .route("/admin/proxy-configs/:proxy_id/check", post(admin_api::check_proxy_config))
        .route(
            "/admin/account-groups",
            get(admin_api::list_account_groups).post(admin_api::create_account_group),
        )
        .route(
            "/admin/account-groups/:group_id",
            patch(admin_api::update_account_group).delete(admin_api::delete_account_group),
        )
        .route("/admin/keys", get(admin_api::list_keys).post(admin_api::create_key))
        .route("/admin/keys/:key_id", patch(admin_api::update_key).delete(admin_api::delete_key))
        .route("/admin/keys/:key_id/rotate", post(admin_api::rotate_key))
        .route("/admin/usage", get(admin_api::list_usage))
        .route("/admin/usage/events", get(admin_api::list_usage_events))
        .route_layer(middleware::from_fn_with_state(Arc::clone(&service), record_api_event))
        .with_state(service)
}

async fn record_api_event(
    State(service): State<Arc<AppService>>,
    request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let bearer = extract_bearer_token(&headers);
    let (key_id, key_name) = resolve_event_key(&service, bearer.as_deref()).await;
    let endpoint = uri.path().to_string();
    let _activity_guard = should_track_request_activity(&method, &endpoint)
        .then(|| service.request_activity_start(&key_id));
    let (request, last_message_content, captured_body) =
        capture_request_body_if_small(request).await;
    let response = next.run(request).await;
    let status = response.status();
    let request_url = uri
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| endpoint.clone());
    let failed = status.as_u16() >= 400;
    let prompt_chars =
        last_message_content.as_deref().map(|value| value.chars().count() as i64).unwrap_or(0);
    let event = UsageEventRecord {
        event_id: Uuid::new_v4().to_string(),
        request_id: Uuid::new_v4().to_string(),
        key_id,
        key_name,
        account_name: String::new(),
        endpoint,
        request_method: method.as_str().to_string(),
        request_url,
        requested_model: String::new(),
        resolved_upstream_model: String::new(),
        session_id: None,
        task_id: None,
        mode: "api_call".to_string(),
        image_size: None,
        requested_n: 0,
        generated_n: 0,
        billable_images: 0,
        billable_credits: 0,
        size_credit_units: 0,
        context_text_count: 0,
        context_image_count: 0,
        context_credit_surcharge: 0,
        client_ip: extract_client_ip(&headers),
        request_headers_json: Some(headers_json(&headers)),
        prompt_preview: last_message_content.as_deref().and_then(prompt_preview),
        last_message_content,
        request_body_json: if failed { captured_body } else { None },
        prompt_chars,
        effective_prompt_chars: 0,
        status_code: i64::from(status.as_u16()),
        latency_ms: started.elapsed().as_millis() as i64,
        error_code: failed.then(|| format!("http_{}", status.as_u16())),
        error_message: failed.then(|| {
            status.canonical_reason().map(ToString::to_string).unwrap_or_else(|| status.to_string())
        }),
        detail_ref: None,
        created_at: unix_timestamp_secs(),
    };
    if let Err(error) = service.storage().control.apply_success_settlement(&event).await {
        tracing::warn!(error = %error, "failed to enqueue api usage event");
    }
    response
}

fn should_track_request_activity(method: &Method, endpoint: &str) -> bool {
    if *method != Method::POST {
        return false;
    }
    matches!(
        endpoint,
        "/v1/images/generations" | "/v1/images/edits" | "/v1/chat/completions" | "/v1/responses"
    ) || is_session_message_endpoint(endpoint)
}

fn is_session_message_endpoint(endpoint: &str) -> bool {
    let mut parts = endpoint.trim_matches('/').split('/');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next(), parts.next()),
        (Some("sessions"), Some(_), Some("messages"), None, None)
            | (Some("sessions"), Some(_), Some("messages"), Some("edit"), None)
    )
}

async fn capture_request_body_if_small(
    request: Request,
) -> (Request, Option<String>, Option<String>) {
    if !should_capture_request_body(request.method(), request.headers()) {
        return (request, None, None);
    }
    let (parts, body) = request.into_parts();
    match to_bytes(body, MAX_LOGGED_REQUEST_BODY_BYTES).await {
        Ok(bytes) => {
            let body_text = String::from_utf8_lossy(&bytes).to_string();
            let last_message_content = extract_last_message_content(&body_text);
            (Request::from_parts(parts, Body::from(bytes)), last_message_content, Some(body_text))
        }
        Err(error) => {
            tracing::warn!(error = %error, "failed to inspect request body for usage log");
            (Request::from_parts(parts, Body::empty()), None, None)
        }
    }
}

fn should_capture_request_body(method: &Method, headers: &HeaderMap) -> bool {
    if *method == Method::GET || *method == Method::HEAD || *method == Method::DELETE {
        return false;
    }
    let is_json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("application/json"));
    if !is_json {
        return false;
    }
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length <= MAX_LOGGED_REQUEST_BODY_BYTES)
}

fn extract_last_message_content(body_text: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(body_text).ok()?;
    if let Some(prompt) = value.get("prompt").and_then(Value::as_str) {
        return compact_text_option(prompt, 2000);
    }
    if let Some(text) = value.get("text").and_then(Value::as_str) {
        return compact_text_option(text, 2000);
    }
    if let Some(input) = value.get("input") {
        if let Some(text) = extract_input_text(input) {
            return compact_text_option(&text, 2000);
        }
    }
    if let Some(messages) = value.get("messages").and_then(Value::as_array) {
        for message in messages.iter().rev() {
            if let Some(content) = message.get("content").and_then(extract_input_text) {
                return compact_text_option(&content, 2000);
            }
        }
    }
    None
}

fn extract_input_text(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_string());
    }
    if let Some(items) = value.as_array() {
        let joined = items
            .iter()
            .filter_map(|item| {
                item.as_str()
                    .map(ToString::to_string)
                    .or_else(|| item.get("text").and_then(Value::as_str).map(ToString::to_string))
                    .or_else(|| {
                        item.get("content").and_then(Value::as_str).map(ToString::to_string)
                    })
            })
            .collect::<Vec<_>>()
            .join("\n");
        return (!joined.trim().is_empty()).then_some(joined);
    }
    None
}

async fn resolve_event_key(service: &Arc<AppService>, bearer: Option<&str>) -> (String, String) {
    let Some(bearer) = bearer.map(str::trim).filter(|value| !value.is_empty()) else {
        return (String::new(), String::new());
    };
    if service.is_admin_token(bearer) {
        return ("service-admin".to_string(), "service-admin".to_string());
    }
    match service.authenticate_public_key(bearer).await {
        Ok(key) => (key.id, key.name),
        Err(_) => (String::new(), String::new()),
    }
}

fn headers_json(headers: &HeaderMap) -> String {
    let mut value = serde_json::Map::new();
    for (name, header_value) in headers {
        let name = name.as_str().to_ascii_lowercase();
        let value_text = header_value.to_str().unwrap_or("<non-utf8>").to_string();
        value.insert(name, json!(value_text));
    }
    Value::Object(value).to_string()
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
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

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn prompt_preview(value: &str) -> Option<String> {
    compact_text_option(value, 240)
}

fn compact_text_option(value: &str, max_chars: usize) -> Option<String> {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return None;
    }
    if normalized.chars().count() <= max_chars {
        return Some(normalized);
    }
    let mut compact = normalized.chars().take(max_chars).collect::<String>();
    compact.push_str("...");
    Some(compact)
}

fn unix_timestamp_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock before unix epoch").as_secs()
        as i64
}

#[cfg(test)]
mod tests {
    use axum::http::Method;

    use super::should_track_request_activity;

    #[test]
    fn request_activity_tracks_only_work_starting_calls() {
        assert!(should_track_request_activity(&Method::POST, "/v1/images/generations"));
        assert!(should_track_request_activity(&Method::POST, "/sessions/sess-1/messages"));
        assert!(should_track_request_activity(&Method::POST, "/sessions/sess-1/messages/edit"));
        assert!(!should_track_request_activity(&Method::GET, "/sessions"));
        assert!(!should_track_request_activity(&Method::GET, "/tasks/task-1"));
        assert!(!should_track_request_activity(&Method::POST, "/auth/verify"));
        assert!(!should_track_request_activity(&Method::GET, "/admin/usage/events"));
    }
}
