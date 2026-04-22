//! Public API integration tests.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use gpt2api_rs::{
    app::build_router, config::ResolvedPaths, service::AppService, storage::Storage,
    upstream::chatgpt::ChatgptUpstreamClient,
};
use tempfile::TempDir;
use tower::ServiceExt;

async fn build_test_app() -> (TempDir, axum::Router) {
    let temp = tempfile::tempdir().expect("temp dir");
    let paths = ResolvedPaths::new(temp.path().to_path_buf());
    let storage = Storage::open(&paths).await.expect("storage opens");
    let service = Arc::new(
        AppService::new(
            storage,
            "secret".to_string(),
            ChatgptUpstreamClient::new("http://127.0.0.1:9", None),
        )
        .await
        .expect("service init"),
    );
    (temp, build_router(service))
}

/// Returns the OpenAI-compatible image model listing.
#[tokio::test]
async fn models_endpoint_returns_image_models() {
    let (_temp, app) = build_test_app().await;

    let response = app
        .oneshot(Request::builder().uri("/v1/models").body(Body::empty()).expect("request"))
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
}
