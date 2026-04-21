//! Schema bootstrap helpers for SQLite and DuckDB.

use anyhow::Result;
use duckdb::Connection as DuckConnection;
use rusqlite::Connection as SqliteConnection;

/// Creates all control-plane tables if they do not exist yet.
pub fn bootstrap_control_schema(conn: &SqliteConnection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS accounts (
            name TEXT PRIMARY KEY NOT NULL,
            access_token TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            email TEXT,
            user_id TEXT,
            plan_type TEXT,
            default_model_slug TEXT,
            status TEXT NOT NULL,
            quota_remaining INTEGER NOT NULL DEFAULT 0,
            restore_at TEXT,
            last_refresh_at INTEGER,
            last_used_at INTEGER,
            last_error TEXT,
            success_count INTEGER NOT NULL DEFAULT 0,
            fail_count INTEGER NOT NULL DEFAULT 0,
            request_max_concurrency INTEGER,
            request_min_start_interval_ms INTEGER,
            browser_profile_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS account_groups (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS account_group_members (
            group_id TEXT NOT NULL,
            account_name TEXT NOT NULL,
            PRIMARY KEY (group_id, account_name)
        );

        CREATE TABLE IF NOT EXISTS api_keys (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            secret_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            quota_total_images INTEGER NOT NULL,
            quota_used_images INTEGER NOT NULL DEFAULT 0,
            route_strategy TEXT NOT NULL,
            account_group_id TEXT,
            request_max_concurrency INTEGER,
            request_min_start_interval_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS runtime_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            refresh_min_seconds INTEGER NOT NULL,
            refresh_max_seconds INTEGER NOT NULL,
            refresh_jitter_seconds INTEGER NOT NULL,
            default_request_max_concurrency INTEGER NOT NULL,
            default_request_min_start_interval_ms INTEGER NOT NULL,
            event_flush_batch_size INTEGER NOT NULL,
            event_flush_interval_seconds INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS event_outbox (
            id TEXT PRIMARY KEY NOT NULL,
            event_kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            flushed_at INTEGER
        );
        "#,
    )?;
    Ok(())
}

/// Creates all event-store tables if they do not exist yet.
pub fn bootstrap_event_schema(conn: &DuckConnection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS usage_events (
            event_id TEXT,
            request_id TEXT,
            key_id TEXT,
            key_name TEXT,
            account_name TEXT,
            endpoint TEXT,
            requested_model TEXT,
            resolved_upstream_model TEXT,
            requested_n BIGINT,
            generated_n BIGINT,
            billable_images BIGINT,
            status_code BIGINT,
            latency_ms BIGINT,
            error_code TEXT,
            error_message TEXT,
            detail_ref TEXT,
            created_at BIGINT
        );
        "#,
    )?;
    Ok(())
}
