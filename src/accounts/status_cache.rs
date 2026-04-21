//! Background account status cache refresh helpers.

use std::time::Duration;

use rand::Rng;
use tokio::{sync::watch, task::JoinHandle};

/// Returns the next background refresh delay from the configured window.
#[must_use]
pub fn next_refresh_delay(min_seconds: u64, max_seconds: u64) -> Duration {
    if min_seconds >= max_seconds {
        Duration::from_secs(min_seconds)
    } else {
        Duration::from_secs(rand::thread_rng().gen_range(min_seconds..=max_seconds))
    }
}

/// Spawns a background refresher loop that exits when shutdown becomes `true`.
#[must_use]
pub fn spawn_status_refresher(mut shutdown_rx: watch::Receiver<bool>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(300)) => {}
            }
        }
    })
}
