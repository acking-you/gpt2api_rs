//! SQLite control-plane storage.

use std::path::PathBuf;

use anyhow::Result;
use rusqlite::{params, Connection};

use super::migrations::bootstrap_control_schema;
use crate::models::{AccountRecord, ApiKeyRecord, UsageEventRecord};

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

    /// Inserts or replaces a downstream API-key configuration row.
    pub async fn upsert_api_key(&self, key: &ApiKeyRecord) -> Result<()> {
        let path = self.path.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(path)?;
            conn.execute(
                "INSERT OR REPLACE INTO api_keys (id, name, secret_hash, status, quota_total_images, quota_used_images, route_strategy, account_group_id, request_max_concurrency, request_min_start_interval_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    &key.id,
                    &key.name,
                    &key.secret_hash,
                    &key.status,
                    key.quota_total_images,
                    key.quota_used_images,
                    &key.route_strategy,
                    &key.account_group_id,
                    &key.request_max_concurrency,
                    &key.request_min_start_interval_ms,
                ],
            )?;
            Ok(())
        })
        .await??;

        Ok(())
    }

    /// Fetches one downstream API key record by id.
    pub async fn get_api_key(&self, key_id: &str) -> Result<Option<ApiKeyRecord>> {
        let path = self.path.clone();
        let key_id = key_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<ApiKeyRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                "SELECT id, name, secret_hash, status, quota_total_images, quota_used_images, route_strategy, account_group_id, request_max_concurrency, request_min_start_interval_ms FROM api_keys WHERE id = ?1 LIMIT 1",
            )?;
            let row = stmt.query_row([key_id], |row| {
                Ok(ApiKeyRecord {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    secret_hash: row.get(2)?,
                    status: row.get(3)?,
                    quota_total_images: row.get(4)?,
                    quota_used_images: row.get(5)?,
                    route_strategy: row.get(6)?,
                    account_group_id: row.get(7)?,
                    request_max_concurrency: row.get(8)?,
                    request_min_start_interval_ms: row.get(9)?,
                })
            });

            match row {
                Ok(record) => Ok(Some(record)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(err) => Err(err.into()),
            }
        })
        .await?
    }

    /// Applies a successful usage settlement transaction and enqueues an outbox row.
    pub async fn apply_success_settlement(&self, event: &UsageEventRecord) -> Result<()> {
        let path = self.path.clone();
        let event = event.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            tx.execute(
                "UPDATE api_keys SET quota_used_images = quota_used_images + ?2 WHERE id = ?1",
                params![&event.key_id, event.billable_images],
            )?;
            tx.execute(
                "INSERT INTO event_outbox (id, event_kind, payload_json, created_at, flushed_at) VALUES (?1, 'usage_event', ?2, ?3, NULL)",
                params![&event.event_id, serde_json::to_string(&event)?, event.created_at],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await??;

        Ok(())
    }

    /// Lists pending outbox ids that have not been flushed yet.
    pub async fn list_pending_outbox(&self) -> Result<Vec<String>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                "SELECT id FROM event_outbox WHERE flushed_at IS NULL ORDER BY created_at ASC",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }
}
