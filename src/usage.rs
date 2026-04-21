//! Quota settlement and usage-event enqueue.

use anyhow::Result;

use crate::{models::UsageEventRecord, storage::control::ControlDb};

/// Records a successful generation against the control-plane quota state.
pub async fn record_successful_generation(
    control: &ControlDb,
    event: &UsageEventRecord,
) -> Result<()> {
    control.apply_success_settlement(event).await
}
