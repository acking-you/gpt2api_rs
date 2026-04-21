//! Runtime configuration and storage path resolution.

use std::path::PathBuf;

/// Fully resolved on-disk paths used by the service.
#[derive(Debug, Clone)]
pub struct ResolvedPaths {
    /// Root directory that owns all service files.
    pub root: PathBuf,
    /// SQLite database for control-plane state.
    pub control_db: PathBuf,
    /// DuckDB file for usage-event summaries.
    pub events_duckdb: PathBuf,
    /// Directory for large event diagnostic sidecars.
    pub event_blobs_dir: PathBuf,
}

impl ResolvedPaths {
    /// Builds the canonical path layout under one root directory.
    pub fn new(root: PathBuf) -> Self {
        Self {
            control_db: root.join("control.db"),
            events_duckdb: root.join("events.duckdb"),
            event_blobs_dir: root.join("event-blobs"),
            root,
        }
    }
}
