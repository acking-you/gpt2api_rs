//! Usage-event ledger enqueue.

use anyhow::Result;

use crate::{models::UsageEventRecord, storage::control::ControlDb};

/// Records a successful generation as an immutable usage event.
pub async fn record_successful_generation(
    control: &ControlDb,
    event: &UsageEventRecord,
) -> Result<()> {
    control.apply_success_settlement(event).await
}
