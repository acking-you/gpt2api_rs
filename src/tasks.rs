//! Background image task runner and queue drain loop.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use tokio::{sync::watch, task::JoinHandle};

use crate::service::AppService;

/// Background worker that drains queued image tasks.
pub struct ImageTaskRunner {
    service: Arc<AppService>,
}

impl ImageTaskRunner {
    /// Creates a runner bound to the application service.
    #[must_use]
    pub fn new(service: Arc<AppService>) -> Self {
        Self { service }
    }

    /// Spawns the queue-drain loop.
    pub fn spawn(self, mut shutdown_rx: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {
                        let _ = self.drain_once().await;
                    }
                }
            }
        })
    }

    /// Claims and executes tasks until no global queue slot is available.
    pub async fn drain_once(&self) -> Result<()> {
        loop {
            let config = self.service.storage().control.get_runtime_config().await?;
            self.service
                .recover_timed_out_image_tasks(
                    config.image_task_timeout_seconds,
                    unix_timestamp_secs(),
                )
                .await?;
            let Some(task) = self
                .service
                .storage()
                .control
                .claim_next_image_task(config.global_image_concurrency, unix_timestamp_secs())
                .await?
            else {
                return Ok(());
            };

            let service = Arc::clone(&self.service);
            tokio::spawn(async move {
                let _ = service
                    .execute_claimed_image_task_with_timeout(
                        task,
                        config.image_task_timeout_seconds,
                    )
                    .await;
            });
        }
    }
}

fn unix_timestamp_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock before unix epoch").as_secs()
        as i64
}
