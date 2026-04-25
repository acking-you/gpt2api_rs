//! Runtime configuration and storage path resolution.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::Deserialize;

const DEFAULT_EMAIL_ACCOUNTS_FILE: &str = "backend/.local/email_accounts.json";
const FALLBACK_EMAIL_ACCOUNTS_FILE: &str = ".local/email_accounts.json";
const DEFAULT_SMTP_HOST: &str = "smtp.gmail.com";
const DEFAULT_SMTP_PORT: u16 = 587;

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
    /// Reads SMTP config from StaticFlow's email account file plus env overrides.
    #[must_use]
    pub fn from_env() -> Self {
        let mut config = match Self::from_email_accounts_file() {
            Ok(Some(config)) => config,
            Ok(None) => Self::default(),
            Err(error) => {
                tracing::warn!("gpt2api email notifier disabled: {error:#}");
                Self::default()
            }
        };

        if let Some(public_base_url) =
            env_text("GPT2API_PUBLIC_BASE_URL").or_else(|| env_text("SITE_BASE_URL"))
        {
            config.public_base_url = Some(public_base_url);
        }
        if let Some(host) = env_text("GPT2API_SMTP_HOST") {
            config.host = Some(host);
        }
        if let Some(port) = env_text("GPT2API_SMTP_PORT").and_then(|value| value.parse().ok()) {
            config.port = port;
        } else if config.port == 0 {
            config.port = DEFAULT_SMTP_PORT;
        }
        if let Some(username) = env_text("GPT2API_SMTP_USERNAME") {
            config.username = Some(username);
        }
        if let Some(password) = env_text("GPT2API_SMTP_PASSWORD") {
            config.password = Some(compact_password(&password));
        }
        if let Some(from) = env_text("GPT2API_SMTP_FROM") {
            config.from = Some(from);
        }

        config
    }

    /// Reads SMTP credentials from StaticFlow's `email_accounts.json` format.
    pub fn from_email_accounts_file() -> Result<Option<Self>> {
        let path = resolve_email_accounts_file_path();
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read email accounts file {}", path.display()))?;
        let accounts: EmailAccountsConfig = serde_json::from_str(&raw)
            .with_context(|| format!("invalid email accounts JSON: {}", path.display()))?;
        build_smtp_config_from_email_accounts(accounts).map(Some)
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

#[derive(Debug, Clone, Deserialize)]
struct EmailAccountsConfig {
    public_mailbox: PublicMailboxConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct PublicMailboxConfig {
    #[serde(default)]
    smtp_host: Option<String>,
    #[serde(default)]
    smtp_port: Option<u16>,
    username: String,
    app_password: String,
    #[serde(default)]
    display_name: Option<String>,
}

fn build_smtp_config_from_email_accounts(config: EmailAccountsConfig) -> Result<SmtpConfig> {
    let username = required_text(config.public_mailbox.username, "public_mailbox.username")?;
    let password =
        required_password(config.public_mailbox.app_password, "public_mailbox.app_password")?;
    let host = optional_text(config.public_mailbox.smtp_host)
        .unwrap_or_else(|| DEFAULT_SMTP_HOST.to_string());
    let port = config.public_mailbox.smtp_port.unwrap_or(DEFAULT_SMTP_PORT);
    let from = match optional_text(config.public_mailbox.display_name) {
        Some(display_name) => format!("{display_name} <{username}>"),
        None => username.clone(),
    };
    Ok(SmtpConfig {
        public_base_url: None,
        host: Some(host),
        port,
        username: Some(username),
        password: Some(password),
        from: Some(from),
    })
}

fn resolve_email_accounts_file_path() -> PathBuf {
    for env_name in ["GPT2API_EMAIL_ACCOUNTS_FILE", "EMAIL_ACCOUNTS_FILE"] {
        if let Some(path) = env_text(env_name) {
            return PathBuf::from(path);
        }
    }

    if let Some(path) = find_email_accounts_file_from_current_dir() {
        return path;
    }

    PathBuf::from(DEFAULT_EMAIL_ACCOUNTS_FILE)
}

fn find_email_accounts_file_from_current_dir() -> Option<PathBuf> {
    let current_dir = env::current_dir().ok()?;
    for dir in current_dir.ancestors() {
        for relative in [DEFAULT_EMAIL_ACCOUNTS_FILE, FALLBACK_EMAIL_ACCOUNTS_FILE] {
            let candidate = dir.join(relative);
            if file_exists(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

fn file_exists(path: &Path) -> bool {
    path.is_file()
}

fn env_text(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|value| optional_text(Some(value)))
}

fn optional_text(value: Option<String>) -> Option<String> {
    value.map(|item| item.trim().to_string()).filter(|item| !item.is_empty())
}

fn required_text(value: String, field_name: &str) -> Result<String> {
    let Some(value) = optional_text(Some(value)) else {
        anyhow::bail!("{field_name} is required");
    };
    Ok(value)
}

fn required_password(value: String, field_name: &str) -> Result<String> {
    let password = compact_password(&required_text(value, field_name)?);
    if password.is_empty() {
        anyhow::bail!("{field_name} is required");
    }
    Ok(password)
}

fn compact_password(value: &str) -> String {
    value.chars().filter(|ch| !ch.is_whitespace()).collect()
}

fn has_text(value: &str) -> bool {
    !value.trim().is_empty()
}

#[cfg(test)]
mod tests {
    use super::{build_smtp_config_from_email_accounts, EmailAccountsConfig};

    #[test]
    fn staticflow_email_accounts_config_maps_public_mailbox_to_smtp_config() {
        let config: EmailAccountsConfig = serde_json::from_str(
            r#"{
                "public_mailbox": {
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 465,
                    "username": "sender@example.com",
                    "app_password": "ab cd ef",
                    "display_name": "StaticFlow"
                },
                "admin_mailbox": {
                    "username": "admin@example.com",
                    "app_password": "unused"
                }
            }"#,
        )
        .expect("config should parse");

        let smtp = build_smtp_config_from_email_accounts(config).expect("smtp should build");

        assert_eq!(smtp.host.as_deref(), Some("smtp.example.com"));
        assert_eq!(smtp.port, 465);
        assert_eq!(smtp.username.as_deref(), Some("sender@example.com"));
        assert_eq!(smtp.password.as_deref(), Some("abcdef"));
        assert_eq!(smtp.from.as_deref(), Some("StaticFlow <sender@example.com>"));
        assert_eq!(smtp.public_base_url, None);
    }
}
