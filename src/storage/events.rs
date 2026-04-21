//! DuckDB storage for usage-event summaries.

use std::path::PathBuf;

use anyhow::Result;
use duckdb::Connection;

use super::migrations::bootstrap_event_schema;
use crate::models::UsageEventRecord;

/// DuckDB wrapper for usage-event writes and queries.
#[derive(Debug, Clone)]
pub struct EventStore {
    path: PathBuf,
}

impl EventStore {
    /// Opens the DuckDB file and bootstraps the schema.
    pub async fn open(path: PathBuf) -> Result<Self> {
        let path_clone = path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(path_clone)?;
            bootstrap_event_schema(&conn)?;
            Ok(())
        })
        .await??;

        Ok(Self { path })
    }

    /// Returns the underlying DuckDB file path.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Lists the most recent usage-event summaries up to the requested limit.
    pub async fn list_recent_usage(&self, limit: u64) -> Result<Vec<UsageEventRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<UsageEventRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    event_id, request_id, key_id, key_name, account_name, endpoint,
                    requested_model, resolved_upstream_model, requested_n, generated_n,
                    billable_images, status_code, latency_ms, error_code, error_message,
                    detail_ref, created_at
                FROM usage_events
                ORDER BY created_at DESC, event_id DESC
                LIMIT ?
                "#,
            )?;
            let rows = stmt.query_map([limit as i64], |row| {
                Ok(UsageEventRecord {
                    event_id: row.get(0)?,
                    request_id: row.get(1)?,
                    key_id: row.get(2)?,
                    key_name: row.get(3)?,
                    account_name: row.get(4)?,
                    endpoint: row.get(5)?,
                    requested_model: row.get(6)?,
                    resolved_upstream_model: row.get(7)?,
                    requested_n: row.get(8)?,
                    generated_n: row.get(9)?,
                    billable_images: row.get(10)?,
                    status_code: row.get(11)?,
                    latency_ms: row.get(12)?,
                    error_code: row.get(13)?,
                    error_message: row.get(14)?,
                    detail_ref: row.get(15)?,
                    created_at: row.get(16)?,
                })
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }
}
