//! SQLite control-plane storage.

use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::migrations::bootstrap_control_schema;
use crate::{
    models::{
        AccountProxyMode, AccountRecord, ApiKeyRecord, ApiKeyRole, CreatedSignedLink,
        MessageRecord, MessageStatus, ProxyConfigRecord, RuntimeConfigRecord, SessionRecord,
        SessionSource, SignedLinkRecord, UsageEventRecord,
    },
    storage::outbox::OutboxRow,
};

const API_KEY_COLUMNS: &str = "id, name, secret_hash, secret_plaintext, status, quota_total_calls, quota_used_calls, route_strategy, account_group_id, request_max_concurrency, request_min_start_interval_ms, role, notification_email, notification_enabled";

/// SQLite wrapper for control-plane reads and writes.
#[derive(Debug, Clone)]
pub struct ControlDb {
    path: PathBuf,
}

fn api_key_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ApiKeyRecord> {
    let role_raw: String = row.get(11)?;
    let role = ApiKeyRole::parse(&role_raw)
        .ok_or_else(|| invalid_text_value(11, &role_raw, "api key role"))?;
    let notification_enabled: i64 = row.get(13)?;
    Ok(ApiKeyRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        secret_hash: row.get(2)?,
        secret_plaintext: row.get(3)?,
        status: row.get(4)?,
        quota_total_calls: row.get(5)?,
        quota_used_calls: row.get(6)?,
        route_strategy: row.get(7)?,
        account_group_id: row.get(8)?,
        request_max_concurrency: row.get(9)?,
        request_min_start_interval_ms: row.get(10)?,
        role,
        notification_email: row.get(12)?,
        notification_enabled: notification_enabled != 0,
    })
}

fn account_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AccountRecord> {
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
    let proxy_mode_raw: String = row.get(18)?;
    let proxy_mode = AccountProxyMode::parse(&proxy_mode_raw).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            18,
            rusqlite::types::Type::Text,
            format!("unknown proxy mode: {proxy_mode_raw}").into(),
        )
    })?;
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
        proxy_mode,
        proxy_config_id: row.get(19)?,
        browser_profile_json: row.get(20)?,
    })
}

fn proxy_config_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProxyConfigRecord> {
    Ok(ProxyConfigRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        proxy_url: row.get(2)?,
        proxy_username: row.get(3)?,
        proxy_password: row.get(4)?,
        status: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn runtime_config_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RuntimeConfigRecord> {
    Ok(RuntimeConfigRecord {
        refresh_min_seconds: row.get(0)?,
        refresh_max_seconds: row.get(1)?,
        refresh_jitter_seconds: row.get(2)?,
        default_request_max_concurrency: row.get(3)?,
        default_request_min_start_interval_ms: row.get(4)?,
        event_flush_batch_size: row.get(5)?,
        event_flush_interval_seconds: row.get(6)?,
        global_image_concurrency: row.get(7)?,
        signed_link_ttl_seconds: row.get(8)?,
        queue_eta_window_size: row.get(9)?,
    })
}

fn session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
    let source_raw: String = row.get(3)?;
    let source = SessionSource::parse(&source_raw)
        .ok_or_else(|| invalid_text_value(3, &source_raw, "session source"))?;
    Ok(SessionRecord {
        id: row.get(0)?,
        key_id: row.get(1)?,
        title: row.get(2)?,
        source,
        status: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
        last_message_at: row.get(7)?,
    })
}

fn message_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRecord> {
    let status_raw: String = row.get(5)?;
    let status = MessageStatus::parse(&status_raw)
        .ok_or_else(|| invalid_text_value(5, &status_raw, "message status"))?;
    Ok(MessageRecord {
        id: row.get(0)?,
        session_id: row.get(1)?,
        key_id: row.get(2)?,
        role: row.get(3)?,
        content_json: row.get(4)?,
        status,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn signed_link_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SignedLinkRecord> {
    Ok(SignedLinkRecord {
        id: row.get(0)?,
        token_hash: row.get(1)?,
        scope: row.get(2)?,
        scope_id: row.get(3)?,
        expires_at: row.get(4)?,
        revoked_at: row.get(5)?,
        created_at: row.get(6)?,
        used_at: row.get(7)?,
    })
}

fn invalid_text_value(column: usize, value: &str, label: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        format!("unknown {label}: {value}").into(),
    )
}

fn optional_row<T>(result: rusqlite::Result<T>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn normalize_session_title(title: &str) -> String {
    let title = title.trim();
    if title.is_empty() {
        "New chat".to_string()
    } else {
        title.chars().take(120).collect()
    }
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn unix_timestamp_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock before unix epoch").as_secs()
        as i64
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
                    request_max_concurrency, request_min_start_interval_ms, proxy_mode,
                    proxy_config_id, browser_profile_json
                FROM accounts
                ORDER BY name ASC
                "#,
            )?;
            let rows = stmt.query_map([], account_from_row)?;
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
                    request_max_concurrency, request_min_start_interval_ms, proxy_mode,
                    proxy_config_id, browser_profile_json
                FROM accounts
                WHERE name = ?1
                LIMIT 1
                "#,
            )?;
            let row = stmt.query_row([account_name], account_from_row);
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
                    request_max_concurrency, request_min_start_interval_ms, proxy_mode,
                    proxy_config_id, browser_profile_json
                FROM accounts
                WHERE access_token = ?1
                LIMIT 1
                "#,
            )?;
            let row = stmt.query_row([access_token], account_from_row);
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
                    request_max_concurrency, request_min_start_interval_ms, proxy_mode,
                    proxy_config_id, browser_profile_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
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
                    proxy_mode = excluded.proxy_mode,
                    proxy_config_id = excluded.proxy_config_id,
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
                    account.proxy_mode.as_str(),
                    &account.proxy_config_id,
                    &account.browser_profile_json,
                ],
            )?;
            Ok(())
        })
        .await??;

        Ok(())
    }

    /// Fetches one stored proxy config by stable id.
    pub async fn get_proxy_config(&self, proxy_id: &str) -> Result<Option<ProxyConfigRecord>> {
        let path = self.path.clone();
        let proxy_id = proxy_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<ProxyConfigRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                "SELECT id, name, proxy_url, proxy_username, proxy_password, status, created_at, updated_at FROM proxy_configs WHERE id = ?1 LIMIT 1",
            )?;
            let row = stmt.query_row([proxy_id], proxy_config_from_row);

            match row {
                Ok(record) => Ok(Some(record)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(err) => Err(err.into()),
            }
        })
        .await?
    }

    /// Lists all stored proxy configs ordered by name.
    pub async fn list_proxy_configs(&self) -> Result<Vec<ProxyConfigRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<ProxyConfigRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                "SELECT id, name, proxy_url, proxy_username, proxy_password, status, created_at, updated_at FROM proxy_configs ORDER BY name ASC",
            )?;
            let rows = stmt.query_map([], proxy_config_from_row)?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Inserts or updates one stored proxy config row.
    pub async fn upsert_proxy_config(&self, proxy: &ProxyConfigRecord) -> Result<()> {
        let path = self.path.clone();
        let proxy = proxy.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(path)?;
            conn.execute(
                r#"
                INSERT INTO proxy_configs (
                    id, name, proxy_url, proxy_username, proxy_password, status, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    proxy_url = excluded.proxy_url,
                    proxy_username = excluded.proxy_username,
                    proxy_password = excluded.proxy_password,
                    status = excluded.status,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at
                "#,
                params![
                    &proxy.id,
                    &proxy.name,
                    &proxy.proxy_url,
                    &proxy.proxy_username,
                    &proxy.proxy_password,
                    &proxy.status,
                    proxy.created_at,
                    proxy.updated_at,
                ],
            )?;
            Ok(())
        })
        .await??;

        Ok(())
    }

    /// Deletes one stored proxy config by stable id.
    pub async fn delete_proxy_config(&self, proxy_id: &str) -> Result<bool> {
        let path = self.path.clone();
        let proxy_id = proxy_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let conn = Connection::open(path)?;
            let changed =
                conn.execute("DELETE FROM proxy_configs WHERE id = ?1", params![proxy_id])?;
            Ok(changed > 0)
        })
        .await?
    }

    /// Counts how many accounts currently bind one proxy config.
    pub async fn count_accounts_bound_to_proxy_config(&self, proxy_id: &str) -> Result<u64> {
        let path = self.path.clone();
        let proxy_id = proxy_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<u64> {
            let conn = Connection::open(path)?;
            let count: u64 = conn.query_row(
                "SELECT COUNT(*) FROM accounts WHERE proxy_config_id = ?1",
                params![proxy_id],
                |row| row.get(0),
            )?;
            Ok(count)
        })
        .await?
    }

    /// Inserts or replaces a downstream API-key configuration row.
    pub async fn upsert_api_key(&self, key: &ApiKeyRecord) -> Result<()> {
        let path = self.path.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
	            let conn = Connection::open(path)?;
	            conn.execute(
	                "INSERT OR REPLACE INTO api_keys (id, name, secret_hash, secret_plaintext, status, quota_total_calls, quota_used_calls, route_strategy, account_group_id, request_max_concurrency, request_min_start_interval_ms, role, notification_email, notification_enabled) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
	                params![
	                    &key.id,
	                    &key.name,
                    &key.secret_hash,
                    &key.secret_plaintext,
                    &key.status,
                    key.quota_total_calls,
                    key.quota_used_calls,
                    &key.route_strategy,
	                    &key.account_group_id,
	                    &key.request_max_concurrency,
	                    &key.request_min_start_interval_ms,
	                    key.role.as_str(),
	                    &key.notification_email,
	                    if key.notification_enabled { 1_i64 } else { 0_i64 },
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
            let sql = format!("SELECT {API_KEY_COLUMNS} FROM api_keys WHERE id = ?1 LIMIT 1");
            let mut stmt = conn.prepare(&sql)?;
            let row = stmt.query_row([key_id], api_key_from_row);

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
            let sql =
                format!("SELECT {API_KEY_COLUMNS} FROM api_keys WHERE secret_hash = ?1 LIMIT 1");
            let mut stmt = conn.prepare(&sql)?;
            let row = stmt.query_row([secret_hash], api_key_from_row);
            match row {
                Ok(record) => Ok(Some(record)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(err) => Err(err.into()),
            }
        })
        .await?
    }

    /// Fetches one downstream API key by stored plaintext secret.
    pub async fn find_api_key_by_secret_plaintext(
        &self,
        secret_plaintext: &str,
    ) -> Result<Option<ApiKeyRecord>> {
        let path = self.path.clone();
        let secret_plaintext = secret_plaintext.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<ApiKeyRecord>> {
            let conn = Connection::open(path)?;
            let sql = format!(
                "SELECT {API_KEY_COLUMNS} FROM api_keys WHERE secret_plaintext = ?1 LIMIT 1"
            );
            let mut stmt = conn.prepare(&sql)?;
            let row = stmt.query_row([secret_plaintext], api_key_from_row);
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
            let sql = format!("SELECT {API_KEY_COLUMNS} FROM api_keys ORDER BY id ASC");
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], api_key_from_row)?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Fetches the singleton runtime configuration row.
    pub async fn get_runtime_config(&self) -> Result<RuntimeConfigRecord> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<RuntimeConfigRecord> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    refresh_min_seconds,
                    refresh_max_seconds,
                    refresh_jitter_seconds,
                    default_request_max_concurrency,
                    default_request_min_start_interval_ms,
                    event_flush_batch_size,
                    event_flush_interval_seconds,
                    global_image_concurrency,
                    signed_link_ttl_seconds,
                    queue_eta_window_size
                FROM runtime_config
                WHERE id = 1
                LIMIT 1
                "#,
            )?;
            Ok(stmt.query_row([], runtime_config_from_row)?)
        })
        .await?
    }

    /// Updates product-specific runtime configuration fields.
    pub async fn update_runtime_config_product_fields(
        &self,
        global_image_concurrency: i64,
        signed_link_ttl_seconds: i64,
        queue_eta_window_size: i64,
    ) -> Result<RuntimeConfigRecord> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<RuntimeConfigRecord> {
            let conn = Connection::open(path)?;
            conn.execute(
                r#"
                UPDATE runtime_config
                SET
                    global_image_concurrency = ?1,
                    signed_link_ttl_seconds = ?2,
                    queue_eta_window_size = ?3
                WHERE id = 1
                "#,
                params![global_image_concurrency, signed_link_ttl_seconds, queue_eta_window_size,],
            )?;
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    refresh_min_seconds,
                    refresh_max_seconds,
                    refresh_jitter_seconds,
                    default_request_max_concurrency,
                    default_request_min_start_interval_ms,
                    event_flush_batch_size,
                    event_flush_interval_seconds,
                    global_image_concurrency,
                    signed_link_ttl_seconds,
                    queue_eta_window_size
                FROM runtime_config
                WHERE id = 1
                LIMIT 1
                "#,
            )?;
            Ok(stmt.query_row([], runtime_config_from_row)?)
        })
        .await?
    }

    /// Creates one durable conversation session.
    pub async fn create_session(
        &self,
        key_id: &str,
        title: &str,
        source: SessionSource,
    ) -> Result<SessionRecord> {
        let path = self.path.clone();
        let key_id = key_id.to_string();
        let title = normalize_session_title(title);
        tokio::task::spawn_blocking(move || -> Result<SessionRecord> {
            let conn = Connection::open(path)?;
            let now = unix_timestamp_secs();
            let session = SessionRecord {
                id: format!("sess_{}", Uuid::new_v4().simple()),
                key_id,
                title,
                source,
                status: "active".to_string(),
                created_at: now,
                updated_at: now,
                last_message_at: None,
            };
            conn.execute(
                r#"
                INSERT INTO sessions (
                    id, key_id, title, source, status, created_at, updated_at, last_message_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    &session.id,
                    &session.key_id,
                    &session.title,
                    session.source.as_str(),
                    &session.status,
                    session.created_at,
                    session.updated_at,
                    &session.last_message_at,
                ],
            )?;
            Ok(session)
        })
        .await?
    }

    /// Fetches one session owned by a key.
    pub async fn get_session_for_key(
        &self,
        session_id: &str,
        key_id: &str,
    ) -> Result<Option<SessionRecord>> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        let key_id = key_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<SessionRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                r#"
                SELECT id, key_id, title, source, status, created_at, updated_at, last_message_at
                FROM sessions
                WHERE id = ?1 AND key_id = ?2
                LIMIT 1
                "#,
            )?;
            optional_row(stmt.query_row(params![session_id, key_id], session_from_row))
        })
        .await?
    }

    /// Fetches one session without key scoping for product-admin views.
    pub async fn get_session_for_admin(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<SessionRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                r#"
                SELECT id, key_id, title, source, status, created_at, updated_at, last_message_at
                FROM sessions
                WHERE id = ?1
                LIMIT 1
                "#,
            )?;
            optional_row(stmt.query_row([session_id], session_from_row))
        })
        .await?
    }

    /// Updates title and/or status for a key-scoped session.
    pub async fn update_session_for_key(
        &self,
        session_id: &str,
        key_id: &str,
        title: Option<&str>,
        status: Option<&str>,
    ) -> Result<Option<SessionRecord>> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        let key_id = key_id.to_string();
        let title = title.map(normalize_session_title);
        let status = status.map(ToString::to_string);
        tokio::task::spawn_blocking(move || -> Result<Option<SessionRecord>> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            let exists = tx
                .query_row(
                    "SELECT 1 FROM sessions WHERE id = ?1 AND key_id = ?2 LIMIT 1",
                    params![&session_id, &key_id],
                    |_| Ok(()),
                )
                .map(|_| true)
                .or_else(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => Ok(false),
                    other => Err(other),
                })?;
            if !exists {
                return Ok(None);
            }
            if let Some(title) = title {
                tx.execute(
                    "UPDATE sessions SET title = ?1, updated_at = ?2 WHERE id = ?3 AND key_id = ?4",
                    params![title, unix_timestamp_secs(), &session_id, &key_id],
                )?;
            }
            if let Some(status) = status {
                tx.execute(
                    "UPDATE sessions SET status = ?1, updated_at = ?2 WHERE id = ?3 AND key_id = ?4",
                    params![status, unix_timestamp_secs(), &session_id, &key_id],
                )?;
            }
            let session = {
                let mut stmt = tx.prepare(
                    r#"
                    SELECT id, key_id, title, source, status, created_at, updated_at, last_message_at
                    FROM sessions
                    WHERE id = ?1 AND key_id = ?2
                    LIMIT 1
                    "#,
                )?;
                stmt.query_row(params![session_id, key_id], session_from_row)?
            };
            tx.commit()?;
            Ok(Some(session))
        })
        .await?
    }

    /// Lists sessions owned by one key in reverse update order.
    pub async fn list_sessions_for_key(
        &self,
        key_id: &str,
        limit: u64,
        cursor_updated_before: Option<i64>,
    ) -> Result<Vec<SessionRecord>> {
        let path = self.path.clone();
        let key_id = key_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<SessionRecord>> {
            let conn = Connection::open(path)?;
            let mut values = vec![SqlValue::Text(key_id)];
            let mut sql = String::from(
                r#"
                SELECT id, key_id, title, source, status, created_at, updated_at, last_message_at
                FROM sessions
                WHERE key_id = ?1
                "#,
            );
            if let Some(cursor) = cursor_updated_before {
                sql.push_str(" AND updated_at < ?2");
                values.push(SqlValue::Integer(cursor));
            }
            sql.push_str(" ORDER BY updated_at DESC LIMIT ?");
            values.push(SqlValue::Integer(limit.clamp(1, 200) as i64));
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(values), session_from_row)?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Searches sessions for product-admin views.
    pub async fn search_sessions_for_admin(
        &self,
        key_id: Option<&str>,
        query: Option<&str>,
        limit: u64,
        cursor_updated_before: Option<i64>,
    ) -> Result<Vec<SessionRecord>> {
        let path = self.path.clone();
        let key_id = key_id.map(ToString::to_string);
        let query = query.map(ToString::to_string);
        tokio::task::spawn_blocking(move || -> Result<Vec<SessionRecord>> {
            let conn = Connection::open(path)?;
            let mut values = Vec::new();
            let mut clauses = Vec::new();
            if let Some(key_id) = key_id {
                clauses.push("key_id = ?");
                values.push(SqlValue::Text(key_id));
            }
            if let Some(query) = query.filter(|value| !value.trim().is_empty()) {
                clauses.push("title LIKE ?");
                values.push(SqlValue::Text(format!("%{}%", query.trim())));
            }
            if let Some(cursor) = cursor_updated_before {
                clauses.push("updated_at < ?");
                values.push(SqlValue::Integer(cursor));
            }

            let mut sql = String::from(
                "SELECT id, key_id, title, source, status, created_at, updated_at, last_message_at FROM sessions",
            );
            if !clauses.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&clauses.join(" AND "));
            }
            sql.push_str(" ORDER BY updated_at DESC LIMIT ?");
            values.push(SqlValue::Integer(limit.clamp(1, 200) as i64));
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(values), session_from_row)?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Appends one message and updates the owning session timestamp.
    pub async fn append_message(
        &self,
        session_id: &str,
        key_id: &str,
        role: &str,
        content: serde_json::Value,
        status: MessageStatus,
    ) -> Result<MessageRecord> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        let key_id = key_id.to_string();
        let role = role.to_string();
        let content_json = serde_json::to_string(&content)?;
        tokio::task::spawn_blocking(move || -> Result<MessageRecord> {
            let mut conn = Connection::open(path)?;
            let now = unix_timestamp_secs();
            let message = MessageRecord {
                id: format!("msg_{}", Uuid::new_v4().simple()),
                session_id,
                key_id,
                role,
                content_json,
                status,
                created_at: now,
                updated_at: now,
            };
            let tx = conn.transaction()?;
            tx.execute(
                r#"
                INSERT INTO messages (
                    id, session_id, key_id, role, content_json, status, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    &message.id,
                    &message.session_id,
                    &message.key_id,
                    &message.role,
                    &message.content_json,
                    message.status.as_str(),
                    message.created_at,
                    message.updated_at,
                ],
            )?;
            tx.execute(
                "UPDATE sessions SET updated_at = ?1, last_message_at = ?1 WHERE id = ?2",
                params![now, &message.session_id],
            )?;
            tx.commit()?;
            Ok(message)
        })
        .await?
    }

    /// Updates one message content and status.
    pub async fn update_message_content_status(
        &self,
        message_id: &str,
        content: serde_json::Value,
        status: MessageStatus,
    ) -> Result<()> {
        let path = self.path.clone();
        let message_id = message_id.to_string();
        let content_json = serde_json::to_string(&content)?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(path)?;
            conn.execute(
                r#"
                UPDATE messages
                SET content_json = ?1, status = ?2, updated_at = ?3
                WHERE id = ?4
                "#,
                params![content_json, status.as_str(), unix_timestamp_secs(), message_id],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// Lists messages for a session in creation order.
    pub async fn list_messages_for_session(&self, session_id: &str) -> Result<Vec<MessageRecord>> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<MessageRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                r#"
                SELECT id, session_id, key_id, role, content_json, status, created_at, updated_at
                FROM messages
                WHERE session_id = ?1
                ORDER BY created_at ASC
                "#,
            )?;
            let rows = stmt.query_map([session_id], message_from_row)?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Creates a signed link and stores only the token hash.
    pub async fn create_signed_link(
        &self,
        scope: &str,
        scope_id: &str,
        created_at: i64,
        ttl_seconds: i64,
    ) -> Result<CreatedSignedLink> {
        let path = self.path.clone();
        let scope = scope.to_string();
        let scope_id = scope_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<CreatedSignedLink> {
            let conn = Connection::open(path)?;
            let plaintext_token =
                format!("g2s_{}_{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
            let record = SignedLinkRecord {
                id: format!("link_{}", Uuid::new_v4().simple()),
                token_hash: sha256_hex(&plaintext_token),
                scope,
                scope_id,
                expires_at: created_at.saturating_add(ttl_seconds.max(1)),
                revoked_at: None,
                created_at,
                used_at: None,
            };
            conn.execute(
                r#"
                INSERT INTO signed_links (
                    id, token_hash, scope, scope_id, expires_at, revoked_at, created_at, used_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    &record.id,
                    &record.token_hash,
                    &record.scope,
                    &record.scope_id,
                    record.expires_at,
                    &record.revoked_at,
                    record.created_at,
                    &record.used_at,
                ],
            )?;
            Ok(CreatedSignedLink { record, plaintext_token })
        })
        .await?
    }

    /// Resolves a plaintext signed-link token when it is valid at `now`.
    pub async fn resolve_signed_link(
        &self,
        plaintext_token: &str,
        now: i64,
    ) -> Result<Option<SignedLinkRecord>> {
        let path = self.path.clone();
        let token_hash = sha256_hex(plaintext_token);
        tokio::task::spawn_blocking(move || -> Result<Option<SignedLinkRecord>> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            let mut link = {
                let mut stmt = tx.prepare(
                    r#"
                    SELECT id, token_hash, scope, scope_id, expires_at, revoked_at, created_at, used_at
                    FROM signed_links
                    WHERE token_hash = ?1 AND expires_at > ?2 AND revoked_at IS NULL
                    LIMIT 1
                    "#,
                )?;
                optional_row(stmt.query_row(params![token_hash, now], signed_link_from_row))?
            };
            if let Some(record) = &mut link {
                tx.execute(
                    "UPDATE signed_links SET used_at = ?1 WHERE id = ?2",
                    params![now, &record.id],
                )?;
                record.used_at = Some(now);
            }
            tx.commit()?;
            Ok(link)
        })
        .await?
    }

    /// Revokes a signed link.
    pub async fn revoke_signed_link(&self, link_id: &str, revoked_at: i64) -> Result<()> {
        let path = self.path.clone();
        let link_id = link_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(path)?;
            conn.execute(
                "UPDATE signed_links SET revoked_at = ?1 WHERE id = ?2",
                params![revoked_at, link_id],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// Applies a successful usage settlement transaction and enqueues an outbox row.
    pub async fn apply_success_settlement(&self, event: &UsageEventRecord) -> Result<()> {
        let path = self.path.clone();
        let event = event.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            tx.execute(
                "UPDATE api_keys SET quota_used_calls = quota_used_calls + ?2 WHERE id = ?1",
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

    /// Deletes one downstream API key by stable id and returns whether a row was removed.
    pub async fn delete_api_key(&self, key_id: &str) -> Result<bool> {
        let path = self.path.clone();
        let key_id = key_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let conn = Connection::open(path)?;
            Ok(conn.execute("DELETE FROM api_keys WHERE id = ?1", params![key_id])? > 0)
        })
        .await?
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

    /// Resolves one account group into its member account names.
    pub async fn get_account_group_account_names(
        &self,
        group_id: &str,
    ) -> Result<Option<Vec<String>>> {
        let path = self.path.clone();
        let group_id = group_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<Vec<String>>> {
            let conn = Connection::open(path)?;
            let exists = conn
                .query_row(
                    "SELECT 1 FROM account_groups WHERE id = ?1 LIMIT 1",
                    [&group_id],
                    |_| Ok(()),
                )
                .map(|_| true)
                .or_else(|err| match err {
                    rusqlite::Error::QueryReturnedNoRows => Ok(false),
                    other => Err(other),
                })?;
            if !exists {
                return Ok(None);
            }

            let mut stmt = conn.prepare(
                "SELECT account_name FROM account_group_members WHERE group_id = ?1 ORDER BY account_name ASC",
            )?;
            let rows = stmt.query_map([group_id], |row| row.get::<_, String>(0))?;
            Ok(Some(rows.collect::<std::result::Result<Vec<_>, _>>()?))
        })
        .await?
    }
}
