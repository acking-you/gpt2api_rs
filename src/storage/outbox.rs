//! Outbox payload wrappers for SQLite-to-DuckDB event flushing.

use serde::{Deserialize, Serialize};

use crate::models::UsageEventRecord;

/// One pending SQLite outbox row containing a serialized usage event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxRow {
    /// Stable outbox row id.
    pub id: String,
    /// Usage event payload that should be flushed to DuckDB.
    pub payload: UsageEventRecord,
}
