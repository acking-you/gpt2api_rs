//! Local concurrency and pacing scheduler.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio::sync::Notify;

/// Rejection metadata returned when a local scheduler cannot start a request.
#[derive(Debug, Clone)]
pub struct Rejection {
    /// Stable reason code for admin inspection and HTTP error mapping.
    pub reason: &'static str,
    /// Optional wait duration before retrying the same key.
    pub wait: Option<Duration>,
}

#[derive(Debug, Clone)]
struct Entry {
    in_flight: usize,
    next_start_at: Instant,
}

/// In-memory scheduler enforcing per-key concurrency and pacing windows.
#[derive(Debug, Default)]
pub struct LocalRequestScheduler {
    states: Arc<Mutex<HashMap<String, Entry>>>,
    notify: Arc<Notify>,
}

/// Lease that releases one in-flight slot when dropped.
#[derive(Debug)]
pub struct Lease {
    scheduler: Arc<LocalRequestScheduler>,
    key: String,
}

impl LocalRequestScheduler {
    /// Attempts to acquire a lease immediately for the provided logical key.
    pub fn try_acquire(
        self: &Arc<Self>,
        key: &str,
        max_concurrency: Option<u64>,
        min_start_interval_ms: Option<u64>,
        _queued_at: Instant,
    ) -> Result<Lease, Rejection> {
        let now = Instant::now();
        let mut states = self.states.lock();
        let entry =
            states.entry(key.to_string()).or_insert(Entry { in_flight: 0, next_start_at: now });

        if let Some(limit) = max_concurrency.filter(|value| *value > 0) {
            if entry.in_flight >= limit as usize {
                return Err(Rejection { reason: "local_concurrency_limit", wait: None });
            }
        }

        if let Some(interval_ms) = min_start_interval_ms {
            if now < entry.next_start_at {
                return Err(Rejection {
                    reason: "local_start_interval",
                    wait: Some(entry.next_start_at.saturating_duration_since(now)),
                });
            }
            entry.next_start_at = now + Duration::from_millis(interval_ms);
        }

        entry.in_flight += 1;

        Ok(Lease { scheduler: Arc::clone(self), key: key.to_string() })
    }

    /// Waits until either the supplied duration elapses or another lease is released.
    pub async fn wait_for_available(&self, wait: Option<Duration>) {
        match wait {
            Some(wait) => {
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = self.notify.notified() => {}
                }
            }
            None => {
                self.notify.notified().await;
            }
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut states = self.scheduler.states.lock();
        if let Some(entry) = states.get_mut(&self.key) {
            if entry.in_flight > 0 {
                entry.in_flight -= 1;
            }
        }
        self.scheduler.notify.notify_waiters();
    }
}
