# gpt2api-rs Architecture

## Scope

`gpt2api-rs` is a standalone Rust service for managing imported ChatGPT web
access tokens and exposing a small image-gateway-compatible control plane. The
current codebase intentionally stops at bootstrap capabilities:

- local storage bootstrap
- account import parsing helpers
- account refresh helpers against ChatGPT web endpoints
- routing and scheduler primitives
- public bootstrap endpoints
- admin list/status endpoints
- local admin CLI REST client

The actual image-generation POST flow is the next implementation slice.

## Repository layout

- `src/main.rs`: binary entrypoint for `serve` and `admin`
- `src/cli.rs`: clap command definitions
- `src/app.rs`: axum router assembly
- `src/http/`: public and admin handlers
- `src/admin_client.rs`: REST client used by the local admin CLI
- `src/storage/`: SQLite control DB and DuckDB event store
- `src/accounts/`: credential import and metadata refresh helpers
- `src/upstream/`: ChatGPT SSE parsing helpers
- `tests/`: integration coverage for bootstrap behavior

## Storage model

The service stores state under one `--storage-dir` root:

- `control.db`: SQLite control-plane state
- `events.duckdb`: usage-event summaries
- `event-blobs/`: reserved directory for larger sidecar payloads

SQLite tables:

- `accounts`
- `account_groups`
- `account_group_members`
- `api_keys`
- `runtime_config`
- `event_outbox`

DuckDB tables:

- `usage_events`

## Runtime flow

### `serve`

1. Resolve the storage layout from `--storage-dir`.
2. Create required directories if missing.
3. Open and bootstrap SQLite and DuckDB schemas.
4. Build the axum router with the configured admin bearer token.
5. Bind `--listen` and start serving.

### `admin`

The CLI does not read local files directly. It calls the running service through
REST using:

- `GET /admin/accounts`
- `GET /admin/keys`
- `GET /admin/usage?limit=<n>`

The CLI currently prints pretty JSON with `--json`, or Rust debug output without
it.

## HTTP surfaces

### Public

- `GET /healthz`
- `GET /v1/models`

### Admin

- `GET /admin/status`
- `GET /admin/accounts`
- `GET /admin/keys`
- `GET /admin/usage`

All admin endpoints require `Authorization: Bearer <admin-token>`.

## Testing and CI

The repository is validated with stable Rust:

```bash
cargo +stable fmt --check
cargo +stable clippy --all-targets --all-features
cargo +stable test
```

Integration tests currently cover:

- storage bootstrap
- account import parsing
- account refresh helpers
- scheduling and candidate selection
- outbox settlement
- upstream SSE parsing
- public bootstrap endpoints
- admin REST endpoints
- admin REST client

## Known gaps

- image-generation POST handlers are not implemented yet
- admin write operations such as account import/delete are not exposed yet
- periodic background refresh is scaffolded but not wired into runtime startup
- usage flushing from SQLite outbox into DuckDB is not wired into a live worker
