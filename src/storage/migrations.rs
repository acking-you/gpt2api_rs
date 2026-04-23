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
            quota_known INTEGER NOT NULL DEFAULT 0,
            restore_at TEXT,
            last_refresh_at INTEGER,
            last_used_at INTEGER,
            last_error TEXT,
            success_count INTEGER NOT NULL DEFAULT 0,
            fail_count INTEGER NOT NULL DEFAULT 0,
            request_max_concurrency INTEGER,
            request_min_start_interval_ms INTEGER,
            proxy_mode TEXT NOT NULL DEFAULT 'inherit',
            proxy_config_id TEXT,
            browser_profile_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS proxy_configs (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            proxy_url TEXT NOT NULL,
            proxy_username TEXT,
            proxy_password TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
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
            secret_plaintext TEXT,
            status TEXT NOT NULL,
            quota_total_calls INTEGER NOT NULL,
            quota_used_calls INTEGER NOT NULL DEFAULT 0,
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
    ensure_account_column(
        conn,
        "quota_known",
        "ALTER TABLE accounts ADD COLUMN quota_known INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_account_column(
        conn,
        "proxy_mode",
        "ALTER TABLE accounts ADD COLUMN proxy_mode TEXT NOT NULL DEFAULT 'inherit'",
    )?;
    ensure_account_column(
        conn,
        "proxy_config_id",
        "ALTER TABLE accounts ADD COLUMN proxy_config_id TEXT",
    )?;
    ensure_api_key_call_quota_columns(conn)?;
    ensure_api_key_secret_plaintext_column(conn)?;
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

fn ensure_account_column(conn: &SqliteConnection, column_name: &str, ddl: &str) -> Result<()> {
    let columns = table_columns(conn, "accounts")?;
    if columns.iter().any(|column| column == column_name) {
        return Ok(());
    }
    conn.execute_batch(ddl)?;
    Ok(())
}

fn ensure_api_key_call_quota_columns(conn: &SqliteConnection) -> Result<()> {
    let columns = table_columns(conn, "api_keys")?;
    let has_total_calls = columns.iter().any(|column| column == "quota_total_calls");
    let has_used_calls = columns.iter().any(|column| column == "quota_used_calls");
    let has_total_images = columns.iter().any(|column| column == "quota_total_images");
    let has_used_images = columns.iter().any(|column| column == "quota_used_images");

    if !has_total_calls {
        conn.execute_batch(
            "ALTER TABLE api_keys ADD COLUMN quota_total_calls INTEGER NOT NULL DEFAULT 0",
        )?;
        if has_total_images {
            conn.execute_batch(
                "UPDATE api_keys SET quota_total_calls = quota_total_images WHERE quota_total_calls = 0",
            )?;
        }
    }
    if !has_used_calls {
        conn.execute_batch(
            "ALTER TABLE api_keys ADD COLUMN quota_used_calls INTEGER NOT NULL DEFAULT 0",
        )?;
        if has_used_images {
            conn.execute_batch(
                "UPDATE api_keys SET quota_used_calls = quota_used_images WHERE quota_used_calls = 0",
            )?;
        }
    }
    if has_total_images || has_used_images {
        rebuild_api_keys_table(conn)?;
    }
    Ok(())
}

fn ensure_api_key_secret_plaintext_column(conn: &SqliteConnection) -> Result<()> {
    let columns = table_columns(conn, "api_keys")?;
    if columns.iter().any(|column| column == "secret_plaintext") {
        return Ok(());
    }
    conn.execute_batch("ALTER TABLE api_keys ADD COLUMN secret_plaintext TEXT")?;
    Ok(())
}

fn table_columns(conn: &SqliteConnection, table_name: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table_name})"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn rebuild_api_keys_table(conn: &SqliteConnection) -> Result<()> {
    let columns = table_columns(conn, "api_keys")?;
    let select_secret_plaintext = if columns.iter().any(|column| column == "secret_plaintext") {
        "secret_plaintext"
    } else {
        "NULL"
    };
    let sql = format!(
        r#"
        BEGIN IMMEDIATE;
        DROP TABLE IF EXISTS api_keys__new;
        CREATE TABLE api_keys__new (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            secret_hash TEXT NOT NULL,
            secret_plaintext TEXT,
            status TEXT NOT NULL,
            quota_total_calls INTEGER NOT NULL,
            quota_used_calls INTEGER NOT NULL DEFAULT 0,
            route_strategy TEXT NOT NULL,
            account_group_id TEXT,
            request_max_concurrency INTEGER,
            request_min_start_interval_ms INTEGER
        );
        INSERT INTO api_keys__new (
            id,
            name,
            secret_hash,
            secret_plaintext,
            status,
            quota_total_calls,
            quota_used_calls,
            route_strategy,
            account_group_id,
            request_max_concurrency,
            request_min_start_interval_ms
        )
        SELECT
            id,
            name,
            secret_hash,
            {select_secret_plaintext},
            status,
            quota_total_calls,
            quota_used_calls,
            route_strategy,
            account_group_id,
            request_max_concurrency,
            request_min_start_interval_ms
        FROM api_keys;
        DROP TABLE api_keys;
        ALTER TABLE api_keys__new RENAME TO api_keys;
        COMMIT;
        "#,
    );
    conn.execute_batch(&sql)?;
    Ok(())
}
