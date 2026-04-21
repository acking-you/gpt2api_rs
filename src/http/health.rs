//! Health endpoints.

/// Returns a simple liveness payload for process health checks.
pub async fn healthz() -> &'static str {
    "ok"
}
