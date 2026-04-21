//! Public API integration tests.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use gpt2api_rs::app::build_router_for_tests;
use tower::ServiceExt;

/// Returns the OpenAI-compatible image model listing.
#[tokio::test]
async fn models_endpoint_returns_image_models() {
    let app = build_router_for_tests();

    let response = app
        .oneshot(Request::builder().uri("/v1/models").body(Body::empty()).expect("request"))
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
}
