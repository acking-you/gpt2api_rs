//! Admin API integration tests.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use gpt2api_rs::app::build_router_for_tests;
use tower::ServiceExt;

/// Requires a bearer token before serving the admin status endpoint.
#[tokio::test]
async fn admin_status_requires_bearer_token() {
    let app = build_router_for_tests();
    let response = app
        .oneshot(Request::builder().uri("/admin/status").body(Body::empty()).expect("request"))
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
