//! Scheduler and route-selection tests.

use std::{sync::Arc, time::Instant};

use gpt2api_rs::models::{AccountRouteCandidate, RouteStrategy};
use gpt2api_rs::routing::select_best_candidate;
use gpt2api_rs::scheduler::LocalRequestScheduler;

/// Rejects a second request when the local concurrency cap is already consumed.
#[tokio::test(start_paused = true)]
async fn scheduler_blocks_until_start_interval_expires() {
    let scheduler = Arc::new(LocalRequestScheduler::default());
    let queued_at = Instant::now();

    let _first =
        scheduler.try_acquire("alpha", Some(1), Some(500), queued_at).expect("first lease");

    let rejection = scheduler
        .try_acquire("alpha", Some(1), Some(500), queued_at)
        .expect_err("second request rejected");

    assert_eq!(rejection.reason, "local_concurrency_limit");
}

/// Prefers higher remaining quota and then the least recently routed account.
#[test]
fn auto_route_prefers_higher_remaining_quota_then_least_recently_used() {
    let selected = select_best_candidate(
        RouteStrategy::Auto,
        &[
            AccountRouteCandidate::new("a1", 3, 200),
            AccountRouteCandidate::new("a2", 8, 300),
            AccountRouteCandidate::new("a3", 8, 100),
        ],
    )
    .expect("selected candidate");

    assert_eq!(selected.name, "a3");
}
