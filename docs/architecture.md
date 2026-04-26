# gpt2api-rs Architecture

## Scope

`gpt2api-rs` is a standalone Rust service for managing imported ChatGPT web
accounts and exposing public GPT2API image-generation endpoints plus an admin
control plane. Current runtime capabilities include:

- local storage bootstrap
- account import parsing helpers
- account refresh helpers against ChatGPT web endpoints
- routing, account groups, and proxy configuration
- public key login, session, task, artifact, and OpenAI-compatible image
  endpoints
- admin account, key, usage, queue, proxy, and group endpoints
- local admin CLI REST client

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
- `GET /version`
- `GET|POST /auth/login`
- `POST /auth/verify`
- `GET /me`
- `GET /me/usage/events`
- `PATCH /me/notification`
- `GET|POST /sessions`
- `GET|PATCH|DELETE /sessions/:session_id`
- `GET /sessions/:session_id/events`
- `POST /sessions/:session_id/messages`
- `POST /sessions/:session_id/messages/edit`
- `GET /tasks/:task_id`
- `POST /tasks/:task_id/cancel`
- `GET /artifacts/:artifact_id`
- `GET /share/:token`
- `GET /share/:token/artifacts/:artifact_id`
- `GET /v1/models`
- `POST /v1/images/generations`
- `POST /v1/images/edits`
- `POST /v1/chat/completions`
- `POST /v1/responses`

### Admin

- `GET /admin/sessions`
- `GET /admin/queue`
- `PATCH /admin/queue/config`
- `POST /admin/tasks/:task_id/cancel`
- `GET /admin/status`
- `GET /admin/accounts`
- `DELETE /admin/accounts`
- `POST /admin/accounts/import`
- `POST /admin/accounts/refresh`
- `POST /admin/accounts/update`
- `GET|POST /admin/proxy-configs`
- `GET|PATCH|DELETE /admin/proxy-configs/:proxy_id`
- `POST /admin/proxy-configs/:proxy_id/check`
- `GET|POST /admin/account-groups`
- `PATCH|DELETE /admin/account-groups/:group_id`
- `GET /admin/keys`
- `POST /admin/keys`
- `PATCH|DELETE /admin/keys/:key_id`
- `POST /admin/keys/:key_id/rotate`
- `GET /admin/usage`
- `GET /admin/usage/events`

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

- This document is an implementation overview. User-facing request examples live
  in `skills/gpt2api-image-request/SKILL.md`.
