//! SQLite control-plane storage.

use std::path::PathBuf;

use anyhow::Result;
use rusqlite::{params, Connection};

use super::migrations::bootstrap_control_schema;
use crate::models::AccountRecord;

/// SQLite wrapper for control-plane reads and writes.
#[derive(Debug, Clone)]
pub struct ControlDb {
    path: PathBuf,
}

impl ControlDb {
    /// Opens the SQLite database and bootstraps the schema.
    pub async fn open(path: PathBuf) -> Result<Self> {
        let path_clone = path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(path_clone)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            bootstrap_control_schema(&conn)?;
            Ok(())
        })
        .await??;

        Ok(Self { path })
    }

    /// Lists all SQLite table names in ascending order.
    pub async fn list_table_names(&self) -> Result<Vec<String>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn
                .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name ASC")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Inserts or updates an imported account in the control-plane database.
    pub async fn upsert_account(&self, account: &AccountRecord) -> Result<()> {
        let path = self.path.clone();
        let account = account.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(path)?;
            conn.execute(
                r#"
                INSERT INTO accounts (
                    name, access_token, source_kind, email, user_id, plan_type,
                    default_model_slug, status, quota_remaining, restore_at,
                    last_refresh_at, last_used_at, last_error, success_count, fail_count,
                    request_max_concurrency, request_min_start_interval_ms, browser_profile_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
                ON CONFLICT(name) DO UPDATE SET
                    access_token = excluded.access_token,
                    source_kind = excluded.source_kind,
                    email = excluded.email,
                    user_id = excluded.user_id,
                    plan_type = excluded.plan_type,
                    default_model_slug = excluded.default_model_slug,
                    status = excluded.status,
                    quota_remaining = excluded.quota_remaining,
                    restore_at = excluded.restore_at,
                    last_refresh_at = excluded.last_refresh_at,
                    last_used_at = excluded.last_used_at,
                    last_error = excluded.last_error,
                    success_count = excluded.success_count,
                    fail_count = excluded.fail_count,
                    request_max_concurrency = excluded.request_max_concurrency,
                    request_min_start_interval_ms = excluded.request_min_start_interval_ms,
                    browser_profile_json = excluded.browser_profile_json
                "#,
                params![
                    &account.name,
                    &account.access_token,
                    account.source_kind.as_str(),
                    &account.email,
                    &account.user_id,
                    &account.plan_type,
                    &account.default_model_slug,
                    &account.status,
                    account.quota_remaining,
                    &account.restore_at,
                    &account.last_refresh_at,
                    &account.last_used_at,
                    &account.last_error,
                    account.success_count,
                    account.fail_count,
                    &account.request_max_concurrency,
                    &account.request_min_start_interval_ms,
                    &account.browser_profile_json,
                ],
            )?;
            Ok(())
        })
        .await??;

        Ok(())
    }
}
