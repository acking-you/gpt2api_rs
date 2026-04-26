//! DuckDB storage for usage-event summaries.

use std::path::PathBuf;

use anyhow::Result;
use duckdb::Connection;

use super::migrations::bootstrap_event_schema;
use crate::models::UsageEventRecord;

/// Admin usage-event query filter.
#[derive(Debug, Clone, Default)]
pub struct UsageEventQuery {
    /// Optional exact key id filter.
    pub key_id: Option<String>,
    /// Optional case-insensitive search term.
    pub q: Option<String>,
    /// Whether management-plane requests should be included.
    pub include_admin: bool,
    /// Page limit.
    pub limit: u64,
    /// Page offset.
    pub offset: u64,
}

/// Paginated usage-event query result.
#[derive(Debug, Clone)]
pub struct UsageEventPage {
    /// Total matching rows.
    pub total: u64,
    /// Current page offset.
    pub offset: u64,
    /// Current page limit.
    pub limit: u64,
    /// Whether more rows exist after this page.
    pub has_more: bool,
    /// Sum of billable credits over all matching rows.
    pub billable_credit_total: i64,
    /// Current page rows.
    pub events: Vec<UsageEventRecord>,
}

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

    /// Lists the most recent usage-event summaries up to the requested limit.
    pub async fn list_recent_usage(&self, limit: u64) -> Result<Vec<UsageEventRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<UsageEventRecord>> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    event_id, request_id, key_id, key_name, account_name, endpoint,
                    COALESCE(request_method, 'POST'), COALESCE(request_url, endpoint),
                    requested_model, resolved_upstream_model, session_id, task_id,
                    COALESCE(mode, ''), image_size, requested_n, generated_n, billable_images,
                    COALESCE(billable_credits, billable_images), COALESCE(size_credit_units, 1),
                    COALESCE(context_text_count, 0),
                    COALESCE(context_image_count, 0), COALESCE(context_credit_surcharge, 0),
                    COALESCE(client_ip, ''), request_headers_json, prompt_preview,
                    last_message_content, request_body_json, COALESCE(prompt_chars, 0),
                    COALESCE(effective_prompt_chars, 0),
                    status_code, latency_ms, error_code, error_message, detail_ref, created_at
                FROM usage_events
                ORDER BY created_at DESC, event_id DESC
                LIMIT ?
                "#,
            )?;
            let rows = stmt.query_map([limit as i64], usage_event_from_row)?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .await?
    }

    /// Queries usage events with paging and simple key/search filters.
    pub async fn query_usage_events(&self, query: UsageEventQuery) -> Result<UsageEventPage> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<UsageEventPage> {
            let conn = Connection::open(path)?;
            let key_id = normalize_query_param(query.key_id);
            let q = normalize_query_param(query.q).map(|value| format!("%{}%", value.to_lowercase()));
            let limit = query.limit.clamp(1, 500);
            let offset = query.offset;
            let admin_filter_sql = if query.include_admin {
                ""
            } else {
                "AND endpoint NOT LIKE '/admin/%'"
            };
            let where_sql = format!(
                r#"
                (?1 IS NULL OR key_id = ?1)
                AND (
                    ?2 IS NULL
                    OR LOWER(
                        COALESCE(key_name, '') || ' ' ||
                        COALESCE(key_id, '') || ' ' ||
                        COALESCE(request_id, '') || ' ' ||
                        COALESCE(endpoint, '') || ' ' ||
                        COALESCE(image_size, '') || ' ' ||
                    COALESCE(client_ip, '') || ' ' ||
                    COALESCE(prompt_preview, '') || ' ' ||
                    COALESCE(last_message_content, '')
                    ) LIKE ?2
                )
                {admin_filter_sql}
            "#
            );
            let total: i64 = conn.query_row(
                &format!("SELECT COUNT(*) FROM usage_events WHERE {where_sql}"),
                duckdb::params![&key_id, &q],
                |row| row.get(0),
            )?;
            let billable_credit_total: i64 = conn.query_row(
                &format!(
                    "SELECT COALESCE(SUM(COALESCE(billable_credits, billable_images)), 0) FROM usage_events WHERE {where_sql}"
                ),
                duckdb::params![&key_id, &q],
                |row| row.get(0),
            )?;
            let mut stmt = conn.prepare(&format!(
                r#"
                SELECT
                    event_id, request_id, key_id, key_name, account_name, endpoint,
                    COALESCE(request_method, 'POST'), COALESCE(request_url, endpoint),
                    requested_model, resolved_upstream_model, session_id, task_id,
                    COALESCE(mode, ''), image_size, requested_n, generated_n, billable_images,
                    COALESCE(billable_credits, billable_images), COALESCE(size_credit_units, 1),
                    COALESCE(context_text_count, 0),
                    COALESCE(context_image_count, 0), COALESCE(context_credit_surcharge, 0),
                    COALESCE(client_ip, ''), request_headers_json, prompt_preview,
                    last_message_content, request_body_json, COALESCE(prompt_chars, 0),
                    COALESCE(effective_prompt_chars, 0),
                    status_code, latency_ms, error_code, error_message, detail_ref, created_at
                FROM usage_events
                WHERE {where_sql}
                ORDER BY created_at DESC, event_id DESC
                LIMIT ?3 OFFSET ?4
                "#
            ))?;
            let rows = stmt.query_map(
                duckdb::params![&key_id, &q, limit as i64, offset as i64],
                usage_event_from_row,
            )?;
            let events = rows.collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(UsageEventPage {
                total: total.max(0) as u64,
                offset,
                limit,
                has_more: offset.saturating_add(events.len() as u64) < total.max(0) as u64,
                billable_credit_total,
                events,
            })
        })
        .await?
    }

    /// Sums billable credits for one key from persisted DuckDB events.
    pub async fn sum_billable_credits_for_key(&self, key_id: &str) -> Result<i64> {
        let path = self.path.clone();
        let key_id = key_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<i64> {
            let conn = Connection::open(path)?;
            let total = conn.query_row(
                "SELECT COALESCE(SUM(COALESCE(billable_credits, billable_images)), 0) FROM usage_events WHERE key_id = ?1",
                duckdb::params![&key_id],
                |row| row.get(0),
            )?;
            Ok(total)
        })
        .await?
    }

    /// Inserts one batch of usage-event summaries into DuckDB.
    pub async fn insert_usage_events(&self, events: &[UsageEventRecord]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let path = self.path.clone();
        let events = events.to_vec();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(path)?;
            let tx = conn.transaction()?;
            {
                let mut stmt = tx.prepare(
                    r#"
                    INSERT INTO usage_events (
                        event_id, request_id, key_id, key_name, account_name, endpoint,
                        request_method, request_url, requested_model, resolved_upstream_model,
                        session_id, task_id, mode, image_size, requested_n, generated_n, billable_images,
                        billable_credits, size_credit_units, context_text_count, context_image_count,
                        context_credit_surcharge, client_ip, request_headers_json, prompt_preview,
                        last_message_content, request_body_json, prompt_chars,
                        effective_prompt_chars, status_code, latency_ms, error_code,
                        error_message, detail_ref, created_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                              ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26,
                              ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35)
                    "#,
                )?;
                for event in &events {
                    stmt.execute(duckdb::params![
                        &event.event_id,
                        &event.request_id,
                        &event.key_id,
                        &event.key_name,
                        &event.account_name,
                        &event.endpoint,
                        &event.request_method,
                        &event.request_url,
                        &event.requested_model,
                        &event.resolved_upstream_model,
                        &event.session_id,
                        &event.task_id,
                        &event.mode,
                        &event.image_size,
                        event.requested_n,
                        event.generated_n,
                        event.billable_images,
                        normalized_billable_credits(event),
                        event.size_credit_units,
                        event.context_text_count,
                        event.context_image_count,
                        event.context_credit_surcharge,
                        &event.client_ip,
                        &event.request_headers_json,
                        &event.prompt_preview,
                        &event.last_message_content,
                        &event.request_body_json,
                        event.prompt_chars,
                        event.effective_prompt_chars,
                        event.status_code,
                        event.latency_ms,
                        &event.error_code,
                        &event.error_message,
                        &event.detail_ref,
                        event.created_at,
                    ])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }
}

fn usage_event_from_row(row: &duckdb::Row<'_>) -> duckdb::Result<UsageEventRecord> {
    Ok(UsageEventRecord {
        event_id: row.get(0)?,
        request_id: row.get(1)?,
        key_id: row.get(2)?,
        key_name: row.get(3)?,
        account_name: row.get(4)?,
        endpoint: row.get(5)?,
        request_method: row.get(6)?,
        request_url: row.get(7)?,
        requested_model: row.get(8)?,
        resolved_upstream_model: row.get(9)?,
        session_id: row.get(10)?,
        task_id: row.get(11)?,
        mode: row.get(12)?,
        image_size: row.get(13)?,
        requested_n: row.get(14)?,
        generated_n: row.get(15)?,
        billable_images: row.get(16)?,
        billable_credits: row.get(17)?,
        size_credit_units: row.get(18)?,
        context_text_count: row.get(19)?,
        context_image_count: row.get(20)?,
        context_credit_surcharge: row.get(21)?,
        client_ip: row.get(22)?,
        request_headers_json: row.get(23)?,
        prompt_preview: row.get(24)?,
        last_message_content: row.get(25)?,
        request_body_json: row.get(26)?,
        prompt_chars: row.get(27)?,
        effective_prompt_chars: row.get(28)?,
        status_code: row.get(29)?,
        latency_ms: row.get(30)?,
        error_code: row.get(31)?,
        error_message: row.get(32)?,
        detail_ref: row.get(33)?,
        created_at: row.get(34)?,
    })
}

fn normalize_query_param(value: Option<String>) -> Option<String> {
    value.map(|item| item.trim().to_string()).filter(|item| !item.is_empty())
}

fn normalized_billable_credits(event: &UsageEventRecord) -> i64 {
    if event.billable_credits > 0 {
        event.billable_credits
    } else {
        event.billable_images
    }
}
