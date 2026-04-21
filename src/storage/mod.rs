//! Storage facade for the control database and event store.

pub mod control;
pub mod events;
pub mod migrations;
pub mod outbox;

use anyhow::Result;

use crate::config::ResolvedPaths;

/// Open handles to both persistent storage backends.
#[derive(Debug, Clone)]
pub struct Storage {
    /// SQLite control-plane database.
    pub control: control::ControlDb,
    /// DuckDB usage-event store.
    pub events: events::EventStore,
}

impl Storage {
    /// Opens all storage backends and creates required directories.
    pub async fn open(paths: &ResolvedPaths) -> Result<Self> {
        tokio::fs::create_dir_all(&paths.root).await?;
        tokio::fs::create_dir_all(&paths.event_blobs_dir).await?;

        let control = control::ControlDb::open(paths.control_db.clone()).await?;
        let events = events::EventStore::open(paths.events_duckdb.clone()).await?;

        Ok(Self { control, events })
    }
}
