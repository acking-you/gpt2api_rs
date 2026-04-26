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
            fixed_account_name TEXT,
            request_max_concurrency INTEGER,
            request_min_start_interval_ms INTEGER,
            role TEXT NOT NULL DEFAULT 'user',
            notification_email TEXT,
            notification_enabled INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS runtime_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            refresh_min_seconds INTEGER NOT NULL,
            refresh_max_seconds INTEGER NOT NULL,
            refresh_jitter_seconds INTEGER NOT NULL,
            default_request_max_concurrency INTEGER NOT NULL,
            default_request_min_start_interval_ms INTEGER NOT NULL,
            event_flush_batch_size INTEGER NOT NULL,
            event_flush_interval_seconds INTEGER NOT NULL,
            global_image_concurrency INTEGER NOT NULL DEFAULT 1,
            signed_link_ttl_seconds INTEGER NOT NULL DEFAULT 604800,
            queue_eta_window_size INTEGER NOT NULL DEFAULT 20,
            image_task_timeout_seconds INTEGER NOT NULL DEFAULT 900
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
    ensure_runtime_config_product_columns(conn)?;
    ensure_runtime_config_row(conn)?;
    bootstrap_product_tables(conn)?;
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
    ensure_api_key_product_columns(conn)?;
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
            request_method TEXT,
            request_url TEXT,
            requested_model TEXT,
            resolved_upstream_model TEXT,
            session_id TEXT,
            task_id TEXT,
            mode TEXT,
            image_size TEXT,
            requested_n BIGINT,
            generated_n BIGINT,
            billable_images BIGINT,
            billable_credits BIGINT,
            size_credit_units BIGINT,
            context_text_count BIGINT,
            context_image_count BIGINT,
            context_credit_surcharge BIGINT,
            client_ip TEXT,
            request_headers_json TEXT,
            prompt_preview TEXT,
            last_message_content TEXT,
            request_body_json TEXT,
            prompt_chars BIGINT,
            effective_prompt_chars BIGINT,
            status_code BIGINT,
            latency_ms BIGINT,
            error_code TEXT,
            error_message TEXT,
            detail_ref TEXT,
            created_at BIGINT
        );
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS request_method TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS request_url TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS session_id TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS task_id TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS mode TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS image_size TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS billable_credits BIGINT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS size_credit_units BIGINT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS context_text_count BIGINT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS context_image_count BIGINT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS context_credit_surcharge BIGINT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS client_ip TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS request_headers_json TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS prompt_preview TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS last_message_content TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS request_body_json TEXT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS prompt_chars BIGINT;
        ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS effective_prompt_chars BIGINT;
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

fn ensure_api_key_product_columns(conn: &SqliteConnection) -> Result<()> {
    let columns = table_columns(conn, "api_keys")?;
    if !columns.iter().any(|column| column == "fixed_account_name") {
        conn.execute_batch("ALTER TABLE api_keys ADD COLUMN fixed_account_name TEXT")?;
    }
    if !columns.iter().any(|column| column == "role") {
        conn.execute_batch("ALTER TABLE api_keys ADD COLUMN role TEXT NOT NULL DEFAULT 'user'")?;
    }
    if !columns.iter().any(|column| column == "notification_email") {
        conn.execute_batch("ALTER TABLE api_keys ADD COLUMN notification_email TEXT")?;
    }
    if !columns.iter().any(|column| column == "notification_enabled") {
        conn.execute_batch(
            "ALTER TABLE api_keys ADD COLUMN notification_enabled INTEGER NOT NULL DEFAULT 0",
        )?;
    }
    Ok(())
}

fn ensure_runtime_config_product_columns(conn: &SqliteConnection) -> Result<()> {
    let columns = table_columns(conn, "runtime_config")?;
    if !columns.iter().any(|column| column == "global_image_concurrency") {
        conn.execute_batch(
            "ALTER TABLE runtime_config ADD COLUMN global_image_concurrency INTEGER NOT NULL DEFAULT 1",
        )?;
    }
    if !columns.iter().any(|column| column == "signed_link_ttl_seconds") {
        conn.execute_batch(
            "ALTER TABLE runtime_config ADD COLUMN signed_link_ttl_seconds INTEGER NOT NULL DEFAULT 604800",
        )?;
    }
    if !columns.iter().any(|column| column == "queue_eta_window_size") {
        conn.execute_batch(
            "ALTER TABLE runtime_config ADD COLUMN queue_eta_window_size INTEGER NOT NULL DEFAULT 20",
        )?;
    }
    if !columns.iter().any(|column| column == "image_task_timeout_seconds") {
        conn.execute_batch(
            "ALTER TABLE runtime_config ADD COLUMN image_task_timeout_seconds INTEGER NOT NULL DEFAULT 900",
        )?;
    }
    Ok(())
}

fn ensure_runtime_config_row(conn: &SqliteConnection) -> Result<()> {
    conn.execute_batch(
        r#"
        INSERT OR IGNORE INTO runtime_config (
            id,
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
        ) VALUES (
            1,
            300,
            900,
            60,
            1,
            0,
            100,
            5,
            1,
            604800,
            20,
            900
        );
        "#,
    )?;
    Ok(())
}

fn bootstrap_product_tables(conn: &SqliteConnection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY NOT NULL,
            key_id TEXT NOT NULL,
            title TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            last_message_at INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_key_updated ON sessions(key_id, updated_at);
        CREATE INDEX IF NOT EXISTS idx_sessions_source_updated ON sessions(source, updated_at);
        CREATE INDEX IF NOT EXISTS idx_sessions_status_updated ON sessions(status, updated_at);

        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY NOT NULL,
            session_id TEXT NOT NULL,
            key_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content_json TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_messages_session_created ON messages(session_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_messages_key_created ON messages(key_id, created_at);

        CREATE TABLE IF NOT EXISTS image_tasks (
            id TEXT PRIMARY KEY NOT NULL,
            session_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            key_id TEXT NOT NULL,
            status TEXT NOT NULL,
            mode TEXT NOT NULL,
            prompt TEXT NOT NULL,
            model TEXT NOT NULL,
            n INTEGER NOT NULL,
            request_json TEXT NOT NULL,
            phase TEXT NOT NULL,
            queue_entered_at INTEGER NOT NULL,
            started_at INTEGER,
            finished_at INTEGER,
            position_snapshot INTEGER,
            estimated_start_after_ms INTEGER,
            error_code TEXT,
            error_message TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_image_tasks_status_queue ON image_tasks(status, queue_entered_at);
        CREATE INDEX IF NOT EXISTS idx_image_tasks_key_queue ON image_tasks(key_id, queue_entered_at);
        CREATE INDEX IF NOT EXISTS idx_image_tasks_session_queue ON image_tasks(session_id, queue_entered_at);

        CREATE TABLE IF NOT EXISTS task_events (
            id TEXT PRIMARY KEY NOT NULL,
            task_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            key_id TEXT NOT NULL,
            event_kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_task_events_task_created ON task_events(task_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_task_events_session_created ON task_events(session_id, created_at);

        CREATE TABLE IF NOT EXISTS image_artifacts (
            id TEXT PRIMARY KEY NOT NULL,
            task_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            key_id TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            width INTEGER,
            height INTEGER,
            revised_prompt TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_image_artifacts_task_created ON image_artifacts(task_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_image_artifacts_session_created ON image_artifacts(session_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_image_artifacts_key_created ON image_artifacts(key_id, created_at);

        CREATE TABLE IF NOT EXISTS signed_links (
            id TEXT PRIMARY KEY NOT NULL,
            token_hash TEXT NOT NULL UNIQUE,
            scope TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            expires_at INTEGER NOT NULL,
            revoked_at INTEGER,
            created_at INTEGER NOT NULL,
            used_at INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_signed_links_scope ON signed_links(scope, scope_id);
        "#,
    )?;
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
    let select_role = if columns.iter().any(|column| column == "role") { "role" } else { "'user'" };
    let select_notification_email = if columns.iter().any(|column| column == "notification_email") {
        "notification_email"
    } else {
        "NULL"
    };
    let select_notification_enabled =
        if columns.iter().any(|column| column == "notification_enabled") {
            "notification_enabled"
        } else {
            "0"
        };
    let select_fixed_account_name = if columns.iter().any(|column| column == "fixed_account_name") {
        "fixed_account_name"
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
            fixed_account_name TEXT,
            request_max_concurrency INTEGER,
            request_min_start_interval_ms INTEGER,
            role TEXT NOT NULL DEFAULT 'user',
            notification_email TEXT,
            notification_enabled INTEGER NOT NULL DEFAULT 0
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
            fixed_account_name,
            request_max_concurrency,
            request_min_start_interval_ms,
            role,
            notification_email,
            notification_enabled
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
            {select_fixed_account_name},
            request_max_concurrency,
            request_min_start_interval_ms,
            {select_role},
            {select_notification_email},
            {select_notification_enabled}
        FROM api_keys;
        DROP TABLE api_keys;
        ALTER TABLE api_keys__new RENAME TO api_keys;
        COMMIT;
        "#,
    );
    conn.execute_batch(&sql)?;
    Ok(())
}
