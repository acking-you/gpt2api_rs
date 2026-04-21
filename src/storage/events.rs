//! DuckDB storage for usage-event summaries.

use std::path::PathBuf;

use anyhow::Result;
use duckdb::Connection;

use super::migrations::bootstrap_event_schema;

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
}
