//! Router construction and app bootstrap.

use axum::{routing::get, Router};

use crate::http::{
    admin_api::{list_accounts, list_keys, list_usage, status, AdminState},
    health::healthz,
    public_api::list_models,
};

/// Builds the application router with the provided admin bearer token.
pub fn build_router(admin_token: String) -> Router {
    let admin_state = AdminState { admin_token, storage: None };
    build_router_with_state(admin_state)
}

/// Builds the application router for a fully initialized runtime state.
pub fn build_router_with_state(admin_state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/models", get(list_models))
        .route("/admin/status", get(status))
        .route("/admin/accounts", get(list_accounts))
        .route("/admin/keys", get(list_keys))
        .route("/admin/usage", get(list_usage))
        .with_state(admin_state)
}

/// Builds the application router used by integration tests and the initial binary.
pub fn build_router_for_tests() -> Router {
    build_router("secret".to_string())
}
