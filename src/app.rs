//! Router construction and app bootstrap.

use std::sync::Arc;

use axum::{
    routing::{get, patch, post},
    Router,
};

use crate::{
    http::{admin_api, health::healthz, product_api, public_api},
    service::AppService,
};

/// Builds the application router for a fully initialized runtime service.
pub fn build_router(service: Arc<AppService>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/version", get(public_api::version))
        .route("/auth/login", get(public_api::login).post(public_api::login))
        .route("/auth/verify", post(product_api::verify_auth))
        .route("/me", get(product_api::get_me))
        .route("/me/notification", patch(product_api::update_my_notification))
        .route("/sessions", get(product_api::list_sessions).post(product_api::create_session))
        .route(
            "/sessions/:session_id",
            get(product_api::get_session).patch(product_api::patch_session),
        )
        .route("/sessions/:session_id/events", get(product_api::session_events))
        .route("/sessions/:session_id/messages", post(product_api::create_message))
        .route("/sessions/:session_id/messages/edit", post(product_api::create_edit_message))
        .route("/tasks/:task_id", get(product_api::get_task))
        .route("/tasks/:task_id/cancel", post(product_api::cancel_task))
        .route("/share/:token", get(product_api::get_share))
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
        .route("/admin/keys", get(admin_api::list_keys).post(admin_api::create_key))
        .route("/admin/keys/:key_id", patch(admin_api::update_key).delete(admin_api::delete_key))
        .route("/admin/keys/:key_id/rotate", post(admin_api::rotate_key))
        .route("/admin/usage", get(admin_api::list_usage))
        .with_state(service)
}
