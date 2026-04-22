//! Router construction and app bootstrap.

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};

use crate::{
    http::{admin_api, health::healthz, public_api},
    service::AppService,
};

/// Builds the application router for a fully initialized runtime service.
pub fn build_router(service: Arc<AppService>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/version", get(public_api::version))
        .route("/auth/login", post(public_api::login))
        .route("/v1/models", get(public_api::list_models))
        .route("/v1/images/generations", post(public_api::generate_images))
        .route("/v1/images/edits", post(public_api::edit_images))
        .route("/v1/chat/completions", post(public_api::create_chat_completion))
        .route("/v1/responses", post(public_api::create_response))
        .route("/admin/status", get(admin_api::status))
        .route("/admin/accounts", get(admin_api::list_accounts).delete(admin_api::delete_accounts))
        .route("/admin/accounts/import", post(admin_api::import_accounts))
        .route("/admin/accounts/refresh", post(admin_api::refresh_accounts))
        .route("/admin/accounts/update", post(admin_api::update_account))
        .route("/admin/keys", get(admin_api::list_keys))
        .route("/admin/usage", get(admin_api::list_usage))
        .with_state(service)
}
