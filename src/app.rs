//! Router construction and app bootstrap.

use axum::{routing::get, Router};

use crate::http::{health::healthz, public_api::list_models};

/// Builds the application router used by integration tests and the initial binary.
#[must_use]
pub fn build_router_for_tests() -> Router {
    Router::new().route("/healthz", get(healthz)).route("/v1/models", get(list_models))
}
