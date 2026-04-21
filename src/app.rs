//! Router construction and app bootstrap.

use axum::{routing::get, Router};

use crate::http::{
    admin_api::{status, AdminState},
    health::healthz,
    public_api::list_models,
};

/// Builds the application router used by integration tests and the initial binary.
#[must_use]
pub fn build_router_for_tests() -> Router {
    let admin_state = AdminState { admin_token: "secret".to_string() };

    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/models", get(list_models))
        .route("/admin/status", get(status))
        .with_state(admin_state)
}
