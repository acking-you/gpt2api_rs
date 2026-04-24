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
    /// Directory containing generated image artifacts.
    pub image_artifacts_dir: PathBuf,
}

impl ResolvedPaths {
    /// Builds the canonical path layout under one root directory.
    pub fn new(root: PathBuf) -> Self {
        Self {
            control_db: root.join("control.db"),
            events_duckdb: root.join("events.duckdb"),
            event_blobs_dir: root.join("event-blobs"),
            image_artifacts_dir: root.join("artifacts").join("images"),
            root,
        }
    }
}

/// SMTP configuration for optional image completion emails.
#[derive(Debug, Clone, Default)]
pub struct SmtpConfig {
    /// Public base URL used in email links.
    pub public_base_url: Option<String>,
    /// SMTP host.
    pub host: Option<String>,
    /// SMTP port.
    pub port: u16,
    /// SMTP username.
    pub username: Option<String>,
    /// SMTP password.
    pub password: Option<String>,
    /// Sender email address.
    pub from: Option<String>,
}

impl SmtpConfig {
    /// Reads SMTP config from environment variables.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            public_base_url: std::env::var("GPT2API_PUBLIC_BASE_URL").ok(),
            host: std::env::var("GPT2API_SMTP_HOST").ok(),
            port: std::env::var("GPT2API_SMTP_PORT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(587),
            username: std::env::var("GPT2API_SMTP_USERNAME").ok(),
            password: std::env::var("GPT2API_SMTP_PASSWORD").ok(),
            from: std::env::var("GPT2API_SMTP_FROM").ok(),
        }
    }

    /// Returns whether enough configuration exists to attempt SMTP delivery.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.public_base_url.as_deref().is_some_and(has_text)
            && self.host.as_deref().is_some_and(has_text)
            && self.username.as_deref().is_some_and(has_text)
            && self.password.as_deref().is_some_and(has_text)
            && self.from.as_deref().is_some_and(has_text)
    }
}

fn has_text(value: &str) -> bool {
    !value.trim().is_empty()
}
