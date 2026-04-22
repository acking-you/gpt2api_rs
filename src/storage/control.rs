//! SQLite control-plane storage.

use std::path::PathBuf;

use anyhow::Result;
use rusqlite::{params, Connection};

use super::migrations::bootstrap_control_schema;
use crate::{
    models::{AccountRecord, ApiKeyRecord, UsageEventRecord},
    storage::outbox::OutboxRow,
};

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

    /// Lists all imported accounts ordered by account name.
    pub async fn list_accounts(&self) -> Result<Vec<AccountRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<AccountRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    name, access_token, source_kind, email, user_id, plan_type,
                    default_model_slug, status, quota_remaining, quota_known, restore_at,
                    last_refresh_at, last_used_at, last_error, success_count, fail_count,
                    request_max_concurrency, request_min_start_interval_ms, browser_profile_json
                FROM accounts
                ORDER BY name ASC
                "#,
            )?;
            let rows = stmt.query_map([], |row| {
                let source_kind = match row.get::<_, String>(2)?.as_str() {
                    "token" => crate::models::AccountSourceKind::Token,
                    "session_json" => crate::models::AccountSourceKind::SessionJson,
                    "cpa_json" => crate::models::AccountSourceKind::CpaJson,
                    other => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            format!("unknown source kind: {other}").into(),
                        ))
                    }
                };
                Ok(AccountRecord {
                    name: row.get(0)?,
                    access_token: row.get(1)?,
                    source_kind,
                    email: row.get(3)?,
                    user_id: row.get(4)?,
                    plan_type: row.get(5)?,
                    default_model_slug: row.get(6)?,
                    status: row.get(7)?,
                    quota_remaining: row.get(8)?,
                    quota_known: row.get(9)?,
                    restore_at: row.get(10)?,
                    last_refresh_at: row.get(11)?,
                    last_used_at: row.get(12)?,
                    last_error: row.get(13)?,
                    success_count: row.get(14)?,
                    fail_count: row.get(15)?,
                    request_max_concurrency: row.get(16)?,
                    request_min_start_interval_ms: row.get(17)?,
                    browser_profile_json: row.get(18)?,
                })
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Fetches one imported account by stable account name.
    pub async fn get_account(&self, account_name: &str) -> Result<Option<AccountRecord>> {
        let path = self.path.clone();
        let account_name = account_name.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<AccountRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    name, access_token, source_kind, email, user_id, plan_type,
                    default_model_slug, status, quota_remaining, quota_known, restore_at,
                    last_refresh_at, last_used_at, last_error, success_count, fail_count,
                    request_max_concurrency, request_min_start_interval_ms, browser_profile_json
                FROM accounts
                WHERE name = ?1
                LIMIT 1
                "#,
            )?;
            let row = stmt.query_row([account_name], |row| {
                let source_kind = match row.get::<_, String>(2)?.as_str() {
                    "token" => crate::models::AccountSourceKind::Token,
                    "session_json" => crate::models::AccountSourceKind::SessionJson,
                    "cpa_json" => crate::models::AccountSourceKind::CpaJson,
                    other => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            format!("unknown source kind: {other}").into(),
                        ))
                    }
                };
                Ok(AccountRecord {
                    name: row.get(0)?,
                    access_token: row.get(1)?,
                    source_kind,
                    email: row.get(3)?,
                    user_id: row.get(4)?,
                    plan_type: row.get(5)?,
                    default_model_slug: row.get(6)?,
                    status: row.get(7)?,
                    quota_remaining: row.get(8)?,
                    quota_known: row.get(9)?,
                    restore_at: row.get(10)?,
                    last_refresh_at: row.get(11)?,
                    last_used_at: row.get(12)?,
                    last_error: row.get(13)?,
                    success_count: row.get(14)?,
                    fail_count: row.get(15)?,
                    request_max_concurrency: row.get(16)?,
                    request_min_start_interval_ms: row.get(17)?,
                    browser_profile_json: row.get(18)?,
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

    /// Finds one imported account by raw access token.
    pub async fn find_account_by_access_token(
        &self,
        access_token: &str,
    ) -> Result<Option<AccountRecord>> {
        let path = self.path.clone();
        let access_token = access_token.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<AccountRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    name, access_token, source_kind, email, user_id, plan_type,
                    default_model_slug, status, quota_remaining, quota_known, restore_at,
                    last_refresh_at, last_used_at, last_error, success_count, fail_count,
                    request_max_concurrency, request_min_start_interval_ms, browser_profile_json
                FROM accounts
                WHERE access_token = ?1
                LIMIT 1
                "#,
            )?;
            let row = stmt.query_row([access_token], |row| {
                let source_kind = match row.get::<_, String>(2)?.as_str() {
                    "token" => crate::models::AccountSourceKind::Token,
                    "session_json" => crate::models::AccountSourceKind::SessionJson,
                    "cpa_json" => crate::models::AccountSourceKind::CpaJson,
                    other => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            format!("unknown source kind: {other}").into(),
                        ))
                    }
                };
                Ok(AccountRecord {
                    name: row.get(0)?,
                    access_token: row.get(1)?,
                    source_kind,
                    email: row.get(3)?,
                    user_id: row.get(4)?,
                    plan_type: row.get(5)?,
                    default_model_slug: row.get(6)?,
                    status: row.get(7)?,
                    quota_remaining: row.get(8)?,
                    quota_known: row.get(9)?,
                    restore_at: row.get(10)?,
                    last_refresh_at: row.get(11)?,
                    last_used_at: row.get(12)?,
                    last_error: row.get(13)?,
                    success_count: row.get(14)?,
                    fail_count: row.get(15)?,
                    request_max_concurrency: row.get(16)?,
                    request_min_start_interval_ms: row.get(17)?,
                    browser_profile_json: row.get(18)?,
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
                    default_model_slug, status, quota_remaining, quota_known, restore_at,
                    last_refresh_at, last_used_at, last_error, success_count, fail_count,
                    request_max_concurrency, request_min_start_interval_ms, browser_profile_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
                ON CONFLICT(name) DO UPDATE SET
                    access_token = excluded.access_token,
                    source_kind = excluded.source_kind,
                    email = excluded.email,
                    user_id = excluded.user_id,
                    plan_type = excluded.plan_type,
                    default_model_slug = excluded.default_model_slug,
                    status = excluded.status,
                    quota_remaining = excluded.quota_remaining,
                    quota_known = excluded.quota_known,
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
                    account.quota_known,
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

    /// Fetches one downstream API key by stored secret hash.
    pub async fn find_api_key_by_secret_hash(
        &self,
        secret_hash: &str,
    ) -> Result<Option<ApiKeyRecord>> {
        let path = self.path.clone();
        let secret_hash = secret_hash.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<ApiKeyRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                "SELECT id, name, secret_hash, status, quota_total_images, quota_used_images, route_strategy, account_group_id, request_max_concurrency, request_min_start_interval_ms FROM api_keys WHERE secret_hash = ?1 LIMIT 1",
            )?;
            let row = stmt.query_row([secret_hash], |row| {
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

    /// Lists all configured downstream API keys ordered by id.
    pub async fn list_api_keys(&self) -> Result<Vec<ApiKeyRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<ApiKeyRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                "SELECT id, name, secret_hash, status, quota_total_images, quota_used_images, route_strategy, account_group_id, request_max_concurrency, request_min_start_interval_ms FROM api_keys ORDER BY id ASC",
            )?;
            let rows = stmt.query_map([], |row| {
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
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
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

    /// Lists pending outbox rows with deserialized payloads up to the requested limit.
    pub async fn list_pending_outbox_rows(&self, limit: u64) -> Result<Vec<OutboxRow>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<OutboxRow>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                "SELECT id, payload_json FROM event_outbox WHERE flushed_at IS NULL ORDER BY created_at ASC LIMIT ?1",
            )?;
            let rows = stmt.query_map([limit as i64], |row| {
                let payload_json: String = row.get(1)?;
                let payload = serde_json::from_str::<UsageEventRecord>(&payload_json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Text,
                        error.into(),
                    )
                })?;
                Ok(OutboxRow { id: row.get(0)?, payload })
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Marks the provided outbox ids as flushed at the supplied unix timestamp.
    pub async fn mark_outbox_flushed(&self, ids: &[String], flushed_at: i64) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let path = self.path.clone();
        let ids = ids.to_vec();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            {
                let mut stmt =
                    tx.prepare("UPDATE event_outbox SET flushed_at = ?1 WHERE id = ?2")?;
                for id in &ids {
                    stmt.execute(params![flushed_at, id])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// Deletes imported accounts by raw access token and returns the number of removed rows.
    pub async fn delete_accounts_by_access_tokens(&self, access_tokens: &[String]) -> Result<u64> {
        if access_tokens.is_empty() {
            return Ok(0);
        }
        let path = self.path.clone();
        let access_tokens = access_tokens.to_vec();
        tokio::task::spawn_blocking(move || -> Result<u64> {
            let conn = Connection::open(path)?;
            let mut removed = 0_u64;
            let mut stmt = conn.prepare("DELETE FROM accounts WHERE access_token = ?1")?;
            for access_token in &access_tokens {
                removed += stmt.execute([access_token])? as u64;
            }
            Ok(removed)
        })
        .await?
    }
}
