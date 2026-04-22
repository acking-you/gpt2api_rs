# ChatGPT Web Image Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `gpt2api-rs` into a working ChatGPT Web image gateway with a complete public image API, writable admin control plane, local scheduler enforcement, and real runtime workers.

**Architecture:** Keep SQLite/DuckDB/admin/scheduler as the system spine, add a real ChatGPT Web upstream transport and a service orchestration layer, then expose chatgpt2api-compatible public endpoints on top of that runtime.

**Tech Stack:** Rust, Axum, Tokio, Reqwest with proxy support, SQLite via Rusqlite, DuckDB, Serde, Tower tests.

---

## File Map

- Modify: `src/models.rs`
- Modify: `src/cli.rs`
- Modify: `src/main.rs`
- Modify: `src/app.rs`
- Modify: `src/error.rs`
- Modify: `src/http/admin_api.rs`
- Modify: `src/http/public_api.rs`
- Modify: `src/upstream/chatgpt.rs`
- Modify: `src/upstream/mod.rs`
- Modify: `src/service.rs`
- Modify: `src/storage/control.rs`
- Modify: `src/storage/events.rs`
- Modify: `src/storage/mod.rs`
- Modify: `src/storage/outbox.rs`
- Modify: `src/accounts/import.rs`
- Modify: `src/accounts/refresh.rs`
- Modify: `src/accounts/status_cache.rs`
- Modify: `Cargo.toml`
- Test: `tests/public_api.rs`
- Test: `tests/admin_api.rs`
- Test: `tests/account_refresh.rs`
- Test: `tests/outbox_events.rs`
- Create: `tests/image_generation_flow.rs`
- Create: `tests/session_login.rs`

### Task 1: Runtime and model expansion

**Files:**
- Modify: `src/models.rs`
- Modify: `src/error.rs`
- Modify: `src/storage/control.rs`
- Modify: `src/storage/outbox.rs`

- [ ] Add request/response DTOs for public image endpoints and admin write APIs.
- [ ] Add stable application error variants for auth failure, bad request, rate limiting, upstream failure, and not found.
- [ ] Extend control DB helpers with account lookup, delete, update, runtime config access, outbox payload reads, and flushed-row marking.
- [ ] Add tests for any new storage helpers that change transactional behavior.

### Task 2: Admin write surface

**Files:**
- Modify: `src/http/admin_api.rs`
- Modify: `src/accounts/import.rs`
- Modify: `src/accounts/refresh.rs`
- Modify: `src/admin_client.rs`
- Modify: `src/cli.rs`
- Test: `tests/admin_api.rs`

- [ ] Implement import, delete, refresh, and update handlers for accounts.
- [ ] Support importing session JSON by extracting `accessToken` and optional related fields.
- [ ] Keep existing read endpoints backward compatible.
- [ ] Add integration tests covering successful writes and validation failures.

### Task 3: ChatGPT Web transport

**Files:**
- Modify: `src/upstream/chatgpt.rs`
- Modify: `src/upstream/mod.rs`
- Modify: `Cargo.toml`
- Test: `tests/image_generation_flow.rs`
- Test: `tests/session_login.rs`

- [ ] Implement a reqwest-based upstream client with cookie store and fixed proxy `http://127.0.0.1:11118`.
- [ ] Implement account refresh requests against `/backend-api/me` and `/backend-api/conversation/init`.
- [ ] Implement image upload, conversation request, SSE parsing, fallback polling, and file download helpers.
- [ ] Add mocked tests for generation and edit flows.

### Task 4: Service orchestration and routing

**Files:**
- Modify: `src/service.rs`
- Modify: `src/scheduler.rs`
- Modify: `src/routing.rs`
- Modify: `src/models.rs`
- Test: `tests/image_generation_flow.rs`

- [ ] Build a real runtime service that authenticates API keys, picks accounts, acquires scheduler leases, executes upstream requests, and records results.
- [ ] Preserve account-level scheduler behavior as the first throttle layer and key-level scheduler behavior as the second.
- [ ] Generate usage events on success and consistent account state mutations on failure.
- [ ] Add tests that prove route selection and local rate limiting still work after integration.

### Task 5: Public API completion

**Files:**
- Modify: `src/http/public_api.rs`
- Modify: `src/app.rs`
- Test: `tests/public_api.rs`
- Test: `tests/image_generation_flow.rs`

- [ ] Add `/auth/login`, `/version`, `/v1/images/generations`, `/v1/images/edits`, `/v1/chat/completions`, and `/v1/responses`.
- [ ] Keep `/v1/models` and `/healthz`.
- [ ] Map chatgpt2api-compatible request shapes into the internal service layer.
- [ ] Add integration tests for happy path and request validation failures.

### Task 6: Worker wiring

**Files:**
- Modify: `src/main.rs`
- Modify: `src/accounts/status_cache.rs`
- Modify: `src/storage/events.rs`
- Modify: `src/storage/outbox.rs`
- Modify: `src/storage/mod.rs`
- Test: `tests/outbox_events.rs`
- Test: `tests/account_refresh.rs`

- [ ] Wire the limited-account refresher into runtime startup and shutdown.
- [ ] Implement and wire an outbox flusher from SQLite to DuckDB.
- [ ] Add tests that cover worker-side effects instead of only helper functions.

### Task 7: End-to-end validation

**Files:**
- Modify as needed based on fixes from validation

- [ ] Run `cargo test`.
- [ ] Run `cargo clippy --all-targets --all-features -- -D warnings`.
- [ ] Boot the service locally with the required proxy.
- [ ] Import or bootstrap the provided session material.
- [ ] Exercise:
  - `/auth/login`
  - `/v1/images/generations`
  - `/v1/images/edits`
  - `/v1/chat/completions`
  - `/v1/responses`
- [ ] Fix any mismatches found in live validation and rerun the full quality gate.
