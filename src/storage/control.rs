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
        AccountGroupRecord, AccountProxyMode, AccountRecord, AdminQueueSnapshot, ApiKeyRecord,
        ApiKeyRole, CreatedSignedLink, ImageArtifactRecord, ImageTaskRecord, ImageTaskStatus,
        MessageRecord, MessageStatus, ProxyConfigRecord, QueueSnapshot, RuntimeConfigRecord,
        SessionRecord, SessionSource, SignedLinkRecord, TaskEventRecord, UsageEventRecord,
    },
    storage::outbox::OutboxRow,
};

const API_KEY_COLUMNS: &str = "id, name, secret_hash, secret_plaintext, status, quota_total_calls, quota_used_calls, route_strategy, account_group_id, fixed_account_name, request_max_concurrency, request_min_start_interval_ms, role, notification_email, notification_enabled";
const IMAGE_TASK_COLUMNS: &str = "id, session_id, message_id, key_id, status, mode, prompt, model, n, request_json, phase, queue_entered_at, started_at, finished_at, position_snapshot, estimated_start_after_ms, error_code, error_message";
const IMAGE_ARTIFACT_COLUMNS: &str = "id, task_id, session_id, message_id, key_id, relative_path, mime_type, sha256, size_bytes, width, height, revised_prompt, created_at";
const TASK_EVENT_COLUMNS: &str =
    "rowid, id, task_id, session_id, key_id, event_kind, payload_json, created_at";

/// SQLite wrapper for control-plane reads and writes.
#[derive(Debug, Clone)]
pub struct ControlDb {
    path: PathBuf,
}

/// Input required to create one queued image task.
#[derive(Debug, Clone)]
pub struct CreateImageTaskInput<'a> {
    /// Parent session id.
    pub session_id: &'a str,
    /// Assistant message id updated by this task.
    pub message_id: &'a str,
    /// Owning API-key id.
    pub key_id: &'a str,
    /// Generation or edit mode.
    pub mode: &'a str,
    /// Original user prompt.
    pub prompt: &'a str,
    /// Requested model.
    pub model: &'a str,
    /// Requested image count.
    pub n: i64,
    /// Original request JSON.
    pub request_json: serde_json::Value,
}

fn api_key_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ApiKeyRecord> {
    let role_raw: String = row.get(12)?;
    let role = ApiKeyRole::parse(&role_raw)
        .ok_or_else(|| invalid_text_value(12, &role_raw, "api key role"))?;
    let notification_enabled: i64 = row.get(14)?;
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
        fixed_account_name: row.get(9)?,
        request_max_concurrency: row.get(10)?,
        request_min_start_interval_ms: row.get(11)?,
        role,
        notification_email: row.get(13)?,
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
        image_task_timeout_seconds: row.get(10)?,
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

fn image_task_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ImageTaskRecord> {
    let status_raw: String = row.get(4)?;
    let status = ImageTaskStatus::parse(&status_raw)
        .ok_or_else(|| invalid_text_value(4, &status_raw, "image task status"))?;
    Ok(ImageTaskRecord {
        id: row.get(0)?,
        session_id: row.get(1)?,
        message_id: row.get(2)?,
        key_id: row.get(3)?,
        status,
        mode: row.get(5)?,
        prompt: row.get(6)?,
        model: row.get(7)?,
        n: row.get(8)?,
        request_json: row.get(9)?,
        phase: row.get(10)?,
        queue_entered_at: row.get(11)?,
        started_at: row.get(12)?,
        finished_at: row.get(13)?,
        position_snapshot: row.get(14)?,
        estimated_start_after_ms: row.get(15)?,
        error_code: row.get(16)?,
        error_message: row.get(17)?,
    })
}

fn image_task_from_row_with_offset(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> rusqlite::Result<ImageTaskRecord> {
    let status_raw: String = row.get(offset + 4)?;
    let status = ImageTaskStatus::parse(&status_raw)
        .ok_or_else(|| invalid_text_value(offset + 4, &status_raw, "image task status"))?;
    Ok(ImageTaskRecord {
        id: row.get(offset)?,
        session_id: row.get(offset + 1)?,
        message_id: row.get(offset + 2)?,
        key_id: row.get(offset + 3)?,
        status,
        mode: row.get(offset + 5)?,
        prompt: row.get(offset + 6)?,
        model: row.get(offset + 7)?,
        n: row.get(offset + 8)?,
        request_json: row.get(offset + 9)?,
        phase: row.get(offset + 10)?,
        queue_entered_at: row.get(offset + 11)?,
        started_at: row.get(offset + 12)?,
        finished_at: row.get(offset + 13)?,
        position_snapshot: row.get(offset + 14)?,
        estimated_start_after_ms: row.get(offset + 15)?,
        error_code: row.get(offset + 16)?,
        error_message: row.get(offset + 17)?,
    })
}

fn image_artifact_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ImageArtifactRecord> {
    Ok(ImageArtifactRecord {
        id: row.get(0)?,
        task_id: row.get(1)?,
        session_id: row.get(2)?,
        message_id: row.get(3)?,
        key_id: row.get(4)?,
        relative_path: row.get(5)?,
        mime_type: row.get(6)?,
        sha256: row.get(7)?,
        size_bytes: row.get(8)?,
        width: row.get(9)?,
        height: row.get(10)?,
        revised_prompt: row.get(11)?,
        created_at: row.get(12)?,
    })
}

fn task_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskEventRecord> {
    Ok(TaskEventRecord {
        sequence: row.get(0)?,
        id: row.get(1)?,
        task_id: row.get(2)?,
        session_id: row.get(3)?,
        key_id: row.get(4)?,
        event_kind: row.get(5)?,
        payload_json: row.get(6)?,
        created_at: row.get(7)?,
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

fn account_group_members(conn: &Connection, group_id: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT account_name FROM account_group_members WHERE group_id = ?1 ORDER BY account_name ASC",
    )?;
    let rows = stmt.query_map([group_id], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
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

fn list_tasks_by_status(conn: &Connection, status: &str) -> Result<Vec<ImageTaskRecord>> {
    let sql = format!(
        "SELECT {IMAGE_TASK_COLUMNS} FROM image_tasks WHERE status = ?1 ORDER BY queue_entered_at ASC, rowid ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([status], image_task_from_row)?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
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

    /// Lists all reusable account groups ordered by name.
    pub async fn list_account_groups(&self) -> Result<Vec<AccountGroupRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<AccountGroupRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt =
                conn.prepare("SELECT id, name FROM account_groups ORDER BY name ASC, id ASC")?;
            let rows =
                stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
            let mut groups = Vec::new();
            for row in rows {
                let (id, name) = row?;
                groups.push(AccountGroupRecord {
                    account_names: account_group_members(&conn, &id)?,
                    id,
                    name,
                });
            }
            Ok(groups)
        })
        .await?
    }

    /// Fetches one reusable account group by id.
    pub async fn get_account_group(&self, group_id: &str) -> Result<Option<AccountGroupRecord>> {
        let path = self.path.clone();
        let group_id = group_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<AccountGroupRecord>> {
            let conn = Connection::open(path)?;
            let row = conn.query_row(
                "SELECT id, name FROM account_groups WHERE id = ?1 LIMIT 1",
                [&group_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            );
            match row {
                Ok((id, name)) => Ok(Some(AccountGroupRecord {
                    account_names: account_group_members(&conn, &id)?,
                    id,
                    name,
                })),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(err) => Err(err.into()),
            }
        })
        .await?
    }

    /// Inserts or replaces one reusable account group and all its members.
    pub async fn upsert_account_group(&self, group: &AccountGroupRecord) -> Result<()> {
        let path = self.path.clone();
        let group = group.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT OR REPLACE INTO account_groups (id, name) VALUES (?1, ?2)",
                params![&group.id, &group.name],
            )?;
            tx.execute(
                "DELETE FROM account_group_members WHERE group_id = ?1",
                params![&group.id],
            )?;
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO account_group_members (group_id, account_name) VALUES (?1, ?2)",
                )?;
                for account_name in &group.account_names {
                    stmt.execute(params![&group.id, account_name])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// Deletes one reusable account group.
    pub async fn delete_account_group(&self, group_id: &str) -> Result<bool> {
        let path = self.path.clone();
        let group_id = group_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            tx.execute(
                "DELETE FROM account_group_members WHERE group_id = ?1",
                params![&group_id],
            )?;
            let changed =
                tx.execute("DELETE FROM account_groups WHERE id = ?1", params![&group_id])?;
            tx.commit()?;
            Ok(changed > 0)
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
	                "INSERT OR REPLACE INTO api_keys (id, name, secret_hash, secret_plaintext, status, quota_total_calls, quota_used_calls, route_strategy, account_group_id, fixed_account_name, request_max_concurrency, request_min_start_interval_ms, role, notification_email, notification_enabled) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
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
                    &key.fixed_account_name,
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
                    queue_eta_window_size,
                    image_task_timeout_seconds
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
        image_task_timeout_seconds: i64,
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
                    queue_eta_window_size = ?3,
                    image_task_timeout_seconds = ?4
                WHERE id = 1
                "#,
                params![
                    global_image_concurrency,
                    signed_link_ttl_seconds,
                    queue_eta_window_size,
                    image_task_timeout_seconds,
                ],
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
                    queue_eta_window_size,
                    image_task_timeout_seconds
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
                WHERE id = ?1 AND key_id = ?2 AND status != 'deleted'
                LIMIT 1
                "#,
            )?;
            optional_row(stmt.query_row(params![session_id, key_id], session_from_row))
        })
        .await?
    }

    /// Permanently deletes one key-scoped session and all session-owned rows.
    pub async fn hard_delete_session_for_key(
        &self,
        session_id: &str,
        key_id: &str,
    ) -> Result<Option<Vec<ImageArtifactRecord>>> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        let key_id = key_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<Vec<ImageArtifactRecord>>> {
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
            let artifacts = {
                let sql = format!(
                    "SELECT {IMAGE_ARTIFACT_COLUMNS} FROM image_artifacts WHERE session_id = ?1 AND key_id = ?2 ORDER BY created_at ASC, rowid ASC"
                );
                let mut stmt = tx.prepare(&sql)?;
                let rows = stmt.query_map(params![&session_id, &key_id], image_artifact_from_row)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()?
            };
            tx.execute(
                r#"
                DELETE FROM signed_links
                WHERE (scope = 'session' AND scope_id = ?1)
                   OR (
                        scope = 'image_task'
                        AND scope_id IN (
                            SELECT id FROM image_tasks WHERE session_id = ?1 AND key_id = ?2
                        )
                   )
                "#,
                params![&session_id, &key_id],
            )?;
            tx.execute(
                "DELETE FROM image_artifacts WHERE session_id = ?1 AND key_id = ?2",
                params![&session_id, &key_id],
            )?;
            tx.execute(
                "DELETE FROM task_events WHERE session_id = ?1 AND key_id = ?2",
                params![&session_id, &key_id],
            )?;
            tx.execute(
                "DELETE FROM image_tasks WHERE session_id = ?1 AND key_id = ?2",
                params![&session_id, &key_id],
            )?;
            tx.execute(
                "DELETE FROM messages WHERE session_id = ?1 AND key_id = ?2",
                params![&session_id, &key_id],
            )?;
            tx.execute(
                "DELETE FROM sessions WHERE id = ?1 AND key_id = ?2",
                params![&session_id, &key_id],
            )?;
            tx.commit()?;
            Ok(Some(artifacts))
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
                WHERE key_id = ?1 AND status != 'deleted'
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
            clauses.push("status != 'deleted'");
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

    /// Creates one queued image task.
    pub async fn create_image_task(
        &self,
        input: CreateImageTaskInput<'_>,
    ) -> Result<ImageTaskRecord> {
        let path = self.path.clone();
        let session_id = input.session_id.to_string();
        let message_id = input.message_id.to_string();
        let key_id = input.key_id.to_string();
        let mode = input.mode.to_string();
        let prompt = input.prompt.to_string();
        let model = input.model.to_string();
        let n = input.n;
        let request_json = serde_json::to_string(&input.request_json)?;
        tokio::task::spawn_blocking(move || -> Result<ImageTaskRecord> {
            let conn = Connection::open(path)?;
            let now = unix_timestamp_secs();
            let task = ImageTaskRecord {
                id: format!("task_{}", Uuid::new_v4().simple()),
                session_id,
                message_id,
                key_id,
                status: ImageTaskStatus::Queued,
                mode,
                prompt,
                model,
                n: n.max(1),
                request_json,
                phase: "queued".to_string(),
                queue_entered_at: now,
                started_at: None,
                finished_at: None,
                position_snapshot: None,
                estimated_start_after_ms: None,
                error_code: None,
                error_message: None,
            };
            conn.execute(
                r#"
                INSERT INTO image_tasks (
                    id, session_id, message_id, key_id, status, mode, prompt, model, n,
                    request_json, phase, queue_entered_at, started_at, finished_at,
                    position_snapshot, estimated_start_after_ms, error_code, error_message
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
                "#,
                params![
                    &task.id,
                    &task.session_id,
                    &task.message_id,
                    &task.key_id,
                    task.status.as_str(),
                    &task.mode,
                    &task.prompt,
                    &task.model,
                    task.n,
                    &task.request_json,
                    &task.phase,
                    task.queue_entered_at,
                    &task.started_at,
                    &task.finished_at,
                    &task.position_snapshot,
                    &task.estimated_start_after_ms,
                    &task.error_code,
                    &task.error_message,
                ],
            )?;
            Ok(task)
        })
        .await?
    }

    /// Fetches one image task by id.
    pub async fn get_image_task(&self, task_id: &str) -> Result<Option<ImageTaskRecord>> {
        let path = self.path.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<ImageTaskRecord>> {
            let conn = Connection::open(path)?;
            let sql = format!("SELECT {IMAGE_TASK_COLUMNS} FROM image_tasks WHERE id = ?1 LIMIT 1");
            let mut stmt = conn.prepare(&sql)?;
            optional_row(stmt.query_row([task_id], image_task_from_row))
        })
        .await?
    }

    /// Fetches one image task owned by a key.
    pub async fn get_image_task_for_key(
        &self,
        task_id: &str,
        key_id: &str,
    ) -> Result<Option<ImageTaskRecord>> {
        let path = self.path.clone();
        let task_id = task_id.to_string();
        let key_id = key_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<ImageTaskRecord>> {
            let conn = Connection::open(path)?;
            let sql = format!(
                "SELECT {IMAGE_TASK_COLUMNS} FROM image_tasks WHERE id = ?1 AND key_id = ?2 LIMIT 1"
            );
            let mut stmt = conn.prepare(&sql)?;
            optional_row(stmt.query_row(params![task_id, key_id], image_task_from_row))
        })
        .await?
    }

    /// Lists image tasks for one session in queue order.
    pub async fn list_tasks_for_session(&self, session_id: &str) -> Result<Vec<ImageTaskRecord>> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<ImageTaskRecord>> {
            let conn = Connection::open(path)?;
            let sql = format!(
                "SELECT {IMAGE_TASK_COLUMNS} FROM image_tasks WHERE session_id = ?1 ORDER BY queue_entered_at ASC, rowid ASC"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([session_id], image_task_from_row)?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Lists image tasks currently claimed by workers.
    pub async fn list_running_image_tasks(&self) -> Result<Vec<ImageTaskRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<ImageTaskRecord>> {
            let conn = Connection::open(path)?;
            list_tasks_by_status(&conn, "running")
        })
        .await?
    }

    /// Lists running image tasks whose worker runtime already exceeded the cutoff.
    pub async fn list_running_image_tasks_started_at_or_before(
        &self,
        cutoff_started_at: i64,
    ) -> Result<Vec<ImageTaskRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<ImageTaskRecord>> {
            let conn = Connection::open(path)?;
            let sql = format!(
                "SELECT {IMAGE_TASK_COLUMNS} FROM image_tasks WHERE status = 'running' AND started_at IS NOT NULL AND started_at <= ?1 ORDER BY started_at ASC, rowid ASC"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([cutoff_started_at], image_task_from_row)?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Lists image artifacts for one session in creation order.
    pub async fn list_artifacts_for_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<ImageArtifactRecord>> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<ImageArtifactRecord>> {
            let conn = Connection::open(path)?;
            let sql = format!(
                "SELECT {IMAGE_ARTIFACT_COLUMNS} FROM image_artifacts WHERE session_id = ?1 ORDER BY created_at ASC, rowid ASC"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([session_id], image_artifact_from_row)?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Inserts image artifact metadata.
    pub async fn insert_image_artifact(&self, artifact: &ImageArtifactRecord) -> Result<()> {
        let path = self.path.clone();
        let artifact = artifact.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(path)?;
            conn.execute(
                r#"
                INSERT INTO image_artifacts (
                    id, task_id, session_id, message_id, key_id, relative_path, mime_type,
                    sha256, size_bytes, width, height, revised_prompt, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                "#,
                params![
                    &artifact.id,
                    &artifact.task_id,
                    &artifact.session_id,
                    &artifact.message_id,
                    &artifact.key_id,
                    &artifact.relative_path,
                    &artifact.mime_type,
                    &artifact.sha256,
                    artifact.size_bytes,
                    &artifact.width,
                    &artifact.height,
                    &artifact.revised_prompt,
                    artifact.created_at,
                ],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// Fetches one image artifact.
    pub async fn get_image_artifact(
        &self,
        artifact_id: &str,
    ) -> Result<Option<ImageArtifactRecord>> {
        let path = self.path.clone();
        let artifact_id = artifact_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<ImageArtifactRecord>> {
            let conn = Connection::open(path)?;
            let sql = format!(
                "SELECT {IMAGE_ARTIFACT_COLUMNS} FROM image_artifacts WHERE id = ?1 LIMIT 1"
            );
            let mut stmt = conn.prepare(&sql)?;
            optional_row(stmt.query_row([artifact_id], image_artifact_from_row))
        })
        .await?
    }

    /// Claims the next queued image task when global concurrency allows.
    pub async fn claim_next_image_task(
        &self,
        global_limit: i64,
        started_at: i64,
    ) -> Result<Option<ImageTaskRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<ImageTaskRecord>> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            let running_count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM image_tasks WHERE status = 'running'",
                [],
                |row| row.get(0),
            )?;
            if running_count >= global_limit.max(1) {
                return Ok(None);
            }
            let task_id = {
                let mut stmt = tx.prepare(
                    "SELECT id FROM image_tasks WHERE status = 'queued' ORDER BY queue_entered_at ASC, rowid ASC LIMIT 1",
                )?;
                optional_row(stmt.query_row([], |row| row.get::<_, String>(0)))?
            };
            let Some(task_id) = task_id else {
                return Ok(None);
            };
            tx.execute(
                r#"
                UPDATE image_tasks
                SET status = 'running', phase = 'allocating', started_at = ?1
                WHERE id = ?2 AND status = 'queued'
                "#,
                params![started_at, &task_id],
            )?;
            let task = {
                let sql = format!("SELECT {IMAGE_TASK_COLUMNS} FROM image_tasks WHERE id = ?1 LIMIT 1");
                let mut stmt = tx.prepare(&sql)?;
                stmt.query_row([task_id], image_task_from_row)?
            };
            tx.commit()?;
            Ok(Some(task))
        })
        .await?
    }

    /// Updates the visible phase for one image task and appends an event.
    pub async fn mark_image_task_phase(
        &self,
        task_id: &str,
        phase: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        let path = self.path.clone();
        let task_id = task_id.to_string();
        let phase = phase.to_string();
        let payload_json = serde_json::to_string(&payload)?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            tx.execute(
                "UPDATE image_tasks SET phase = ?1 WHERE id = ?2",
                params![&phase, &task_id],
            )?;
            let (session_id, key_id): (String, String) = tx.query_row(
                "SELECT session_id, key_id FROM image_tasks WHERE id = ?1 LIMIT 1",
                [&task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            tx.execute(
                r#"
                INSERT INTO task_events (id, task_id, session_id, key_id, event_kind, payload_json, created_at)
                VALUES (?1, ?2, ?3, ?4, 'phase', ?5, ?6)
                "#,
                params![
                    format!("evt_{}", Uuid::new_v4().simple()),
                    &task_id,
                    session_id,
                    key_id,
                    payload_json,
                    unix_timestamp_secs(),
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// Marks one task as succeeded.
    pub async fn mark_image_task_succeeded(
        &self,
        task_id: &str,
        finished_at: i64,
        artifact_ids: &[String],
    ) -> Result<()> {
        let path = self.path.clone();
        let task_id = task_id.to_string();
        let artifact_ids = artifact_ids.to_vec();
        let payload_json =
            serde_json::to_string(&serde_json::json!({ "artifact_ids": artifact_ids }))?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            tx.execute(
                r#"
                UPDATE image_tasks
                SET status = 'succeeded', phase = 'done', finished_at = ?1, error_code = NULL, error_message = NULL
                WHERE id = ?2
                "#,
                params![finished_at, &task_id],
            )?;
            let (session_id, key_id): (String, String) = tx.query_row(
                "SELECT session_id, key_id FROM image_tasks WHERE id = ?1 LIMIT 1",
                [&task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            tx.execute(
                r#"
                INSERT INTO task_events (id, task_id, session_id, key_id, event_kind, payload_json, created_at)
                VALUES (?1, ?2, ?3, ?4, 'succeeded', ?5, ?6)
                "#,
                params![
                    format!("evt_{}", Uuid::new_v4().simple()),
                    &task_id,
                    session_id,
                    key_id,
                    payload_json,
                    finished_at,
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// Marks one task as failed.
    pub async fn mark_image_task_failed(
        &self,
        task_id: &str,
        finished_at: i64,
        error_code: &str,
        error_message: &str,
    ) -> Result<()> {
        let path = self.path.clone();
        let task_id = task_id.to_string();
        let error_code = error_code.to_string();
        let error_message = error_message.to_string();
        let payload_json = serde_json::to_string(
            &serde_json::json!({ "error_code": &error_code, "error_message": &error_message }),
        )?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            tx.execute(
                r#"
                UPDATE image_tasks
                SET status = 'failed', phase = 'failed', finished_at = ?1, error_code = ?2, error_message = ?3
                WHERE id = ?4
                "#,
                params![finished_at, &error_code, &error_message, &task_id],
            )?;
            let (session_id, key_id): (String, String) = tx.query_row(
                "SELECT session_id, key_id FROM image_tasks WHERE id = ?1 LIMIT 1",
                [&task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            tx.execute(
                r#"
                INSERT INTO task_events (id, task_id, session_id, key_id, event_kind, payload_json, created_at)
                VALUES (?1, ?2, ?3, ?4, 'failed', ?5, ?6)
                "#,
                params![
                    format!("evt_{}", Uuid::new_v4().simple()),
                    &task_id,
                    session_id,
                    key_id,
                    payload_json,
                    finished_at,
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// Cancels one queued image task.
    pub async fn cancel_queued_image_task(
        &self,
        task_id: &str,
        key_id: Option<&str>,
    ) -> Result<bool> {
        let path = self.path.clone();
        let task_id = task_id.to_string();
        let key_id = key_id.map(ToString::to_string);
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            let finished_at = unix_timestamp_secs();
            let changed = match key_id {
                Some(key_id) => tx.execute(
                    "UPDATE image_tasks SET status = 'cancelled', phase = 'cancelled', finished_at = ?1 WHERE id = ?2 AND key_id = ?3 AND status = 'queued'",
                    params![finished_at, &task_id, key_id],
                )?,
                None => tx.execute(
                    "UPDATE image_tasks SET status = 'cancelled', phase = 'cancelled', finished_at = ?1 WHERE id = ?2 AND status = 'queued'",
                    params![finished_at, &task_id],
                )?,
            };
            if changed > 0 {
                let (session_id, key_id): (String, String) = tx.query_row(
                    "SELECT session_id, key_id FROM image_tasks WHERE id = ?1 LIMIT 1",
                    [&task_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                tx.execute(
                    r#"
                    INSERT INTO task_events (id, task_id, session_id, key_id, event_kind, payload_json, created_at)
                    VALUES (?1, ?2, ?3, ?4, 'cancelled', ?5, ?6)
                    "#,
                    params![
                        format!("evt_{}", Uuid::new_v4().simple()),
                        &task_id,
                        session_id,
                        key_id,
                        serde_json::json!({ "phase": "cancelled" }).to_string(),
                        finished_at,
                    ],
                )?;
            }
            tx.commit()?;
            Ok(changed > 0)
        })
        .await?
    }

    /// Returns a queue snapshot for one task.
    pub async fn queue_snapshot_for_task(&self, task_id: &str) -> Result<QueueSnapshot> {
        let path = self.path.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<QueueSnapshot> {
            let conn = Connection::open(path)?;
            let sql = format!(
                "SELECT rowid, {IMAGE_TASK_COLUMNS} FROM image_tasks WHERE id = ?1 LIMIT 1"
            );
            let mut stmt = conn.prepare(&sql)?;
            let (target_rowid, task): (i64, ImageTaskRecord) = stmt
                .query_row([&task_id], |row| {
                    Ok((row.get(0)?, image_task_from_row_with_offset(row, 1)?))
                })?;
            let position_ahead = if task.status == ImageTaskStatus::Queued {
                conn.query_row(
                    r#"
                    SELECT COUNT(*)
                    FROM image_tasks
                    WHERE status = 'queued'
                      AND (
                        queue_entered_at < ?1
                        OR (queue_entered_at = ?1 AND rowid < ?2)
                      )
                    "#,
                    params![task.queue_entered_at, target_rowid],
                    |row| row.get(0),
                )?
            } else {
                0
            };
            Ok(QueueSnapshot { task, position_ahead, estimated_start_after_ms: None })
        })
        .await?
    }

    /// Returns an admin-visible queue snapshot.
    pub async fn queue_snapshot_admin(&self) -> Result<AdminQueueSnapshot> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<AdminQueueSnapshot> {
            let conn = Connection::open(path)?;
            let config = {
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
                        queue_eta_window_size,
                        image_task_timeout_seconds
                    FROM runtime_config
                    WHERE id = 1
                    LIMIT 1
                    "#,
                )?;
                stmt.query_row([], runtime_config_from_row)?
            };
            let running = list_tasks_by_status(&conn, "running")?;
            let queued = list_tasks_by_status(&conn, "queued")?;
            Ok(AdminQueueSnapshot {
                running,
                queued,
                global_image_concurrency: config.global_image_concurrency,
            })
        })
        .await?
    }

    /// Returns the current maximum task-event sequence for a session.
    pub async fn max_task_event_sequence_for_session(&self, session_id: &str) -> Result<i64> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<i64> {
            let conn = Connection::open(path)?;
            Ok(conn.query_row(
                "SELECT COALESCE(MAX(rowid), 0) FROM task_events WHERE session_id = ?1",
                [session_id],
                |row| row.get(0),
            )?)
        })
        .await?
    }

    /// Lists task events after a session-local sequence cursor.
    pub async fn list_task_events_for_session_after(
        &self,
        session_id: &str,
        after_sequence: i64,
        limit: u64,
    ) -> Result<Vec<TaskEventRecord>> {
        let path = self.path.clone();
        let session_id = session_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<TaskEventRecord>> {
            let conn = Connection::open(path)?;
            let sql = format!(
                "SELECT {TASK_EVENT_COLUMNS} FROM task_events WHERE session_id = ?1 AND rowid > ?2 ORDER BY rowid ASC LIMIT ?3"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(
                params![session_id, after_sequence, i64::try_from(limit).unwrap_or(i64::MAX)],
                task_event_from_row,
            )?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Appends one task event using the task's stored session and key.
    pub async fn append_task_event(
        &self,
        task_id: &str,
        event_kind: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        let path = self.path.clone();
        let task_id = task_id.to_string();
        let event_kind = event_kind.to_string();
        let payload_json = serde_json::to_string(&payload)?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(path)?;
            let (session_id, key_id): (String, String) = conn.query_row(
                "SELECT session_id, key_id FROM image_tasks WHERE id = ?1 LIMIT 1",
                [&task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            conn.execute(
                r#"
                INSERT INTO task_events (id, task_id, session_id, key_id, event_kind, payload_json, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    format!("evt_{}", Uuid::new_v4().simple()),
                    task_id,
                    session_id,
                    key_id,
                    event_kind,
                    payload_json,
                    unix_timestamp_secs(),
                ],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
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

    /// Enqueues a successful usage event. DuckDB usage events are the billing ledger.
    pub async fn apply_success_settlement(&self, event: &UsageEventRecord) -> Result<()> {
        let path = self.path.clone();
        let event = event.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
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

    /// Sums billable credits in usage events that have not reached DuckDB yet.
    pub async fn sum_pending_billable_credits_for_key(&self, key_id: &str) -> Result<i64> {
        let path = self.path.clone();
        let key_id = key_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<i64> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                "SELECT payload_json FROM event_outbox WHERE flushed_at IS NULL AND event_kind = 'usage_event'",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let mut total = 0_i64;
            for row in rows {
                let payload_json = row?;
                let event = serde_json::from_str::<UsageEventRecord>(&payload_json).map_err(
                    |error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            error.into(),
                        )
                    },
                )?;
                if event.key_id == key_id {
                    total += if event.billable_credits > 0 {
                        event.billable_credits
                    } else {
                        event.billable_images
                    };
                }
            }
            Ok(total)
        })
        .await?
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
