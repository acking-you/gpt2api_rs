//! Account candidate ordering helpers.

use crate::models::{AccountRouteCandidate, RouteStrategy};

/// Selects the best account candidate for the requested route strategy.
#[must_use]
pub fn select_best_candidate(
    strategy: RouteStrategy,
    candidates: &[AccountRouteCandidate],
) -> Option<AccountRouteCandidate> {
    match strategy {
        RouteStrategy::Auto => {
            candidates.iter().filter(|candidate| candidate.quota_remaining > 0).cloned().max_by(
                |left, right| {
                    left.quota_remaining
                        .cmp(&right.quota_remaining)
                        .then_with(|| right.last_routed_at_ms.cmp(&left.last_routed_at_ms))
                },
            )
        }
        RouteStrategy::Fixed => candidates.first().cloned(),
    }
}
