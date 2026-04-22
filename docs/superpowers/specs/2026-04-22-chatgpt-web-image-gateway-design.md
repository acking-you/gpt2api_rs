# ChatGPT Web Image Gateway Design

## Goal

Turn `gpt2api-rs` from a bootstrap control-plane prototype into a usable local
ChatGPT Web image gateway that:

- exposes the same core public API shape as `~/llm_pro/chatgpt2api`
- keeps admin/storage/scheduling centered on the existing Rust design
- routes outbound upstream traffic through a local HTTP proxy at
  `http://127.0.0.1:11118`
- supports a direct operator login/import flow using ChatGPT session material
- uses StaticFlow-style account-level concurrency and minimum-start-interval
  controls

This scope explicitly excludes CPA pool integration and the Python project's web
frontend.

## Non-Goals

- support for remote CPA account import pools
- migration of the Python admin frontend
- support for non-image ChatGPT request types
- speculative multi-provider abstraction

## External Contract

### Public HTTP endpoints

The service must expose these public endpoints:

- `POST /auth/login`
- `GET /version`
- `GET /healthz`
- `GET /v1/models`
- `POST /v1/images/generations`
- `POST /v1/images/edits`
- `POST /v1/chat/completions`
- `POST /v1/responses`

Behavior should remain intentionally narrow:

- only image-generation compatible requests are accepted on
  `/v1/chat/completions` and `/v1/responses`
- request/response JSON shape should stay compatible with the Python reference
  project where this gateway claims compatibility
- stream mode is rejected on image proxy endpoints unless and until there is a
  faithful upstream mapping

### Admin HTTP endpoints

The service must expose these admin endpoints:

- existing read endpoints:
  - `GET /admin/status`
  - `GET /admin/accounts`
  - `GET /admin/keys`
  - `GET /admin/usage`
- new write endpoints:
  - `POST /admin/accounts/import`
  - `DELETE /admin/accounts`
  - `POST /admin/accounts/refresh`
  - `POST /admin/accounts/update`
- login/bootstrap endpoint:
  - `POST /auth/login`

All admin endpoints continue using a configured bearer token.

## Runtime Architecture

### 1. Control plane

The existing SQLite + DuckDB layout remains the source of truth.

- SQLite stores accounts, API keys, runtime config, and the usage outbox
- DuckDB stores the flushed usage-event summaries
- account rows continue to hold account-level local scheduler settings
- API keys continue to hold key-level local scheduler settings

The current schema is sufficient for the first complete slice. New columns are
only added where needed for direct session bootstrap and admin ergonomics.

### 2. Upstream transport

A new ChatGPT Web transport layer is added under `src/upstream/` and owns:

- reqwest client construction with:
  - proxy `http://127.0.0.1:11118`
  - rustls TLS
  - cookie store
- account fingerprint resolution from persisted browser profile JSON
- bootstrap requests to the ChatGPT Web surface
- `chat-requirements` token generation path
- optional proof token attachment when the upstream demands it
- file upload init + PUT + upload finalization
- conversation request execution
- SSE parsing for generated file ids and conversation id
- conversation polling when SSE does not contain the final file ids
- file download to base64 response payloads

This transport is account-scoped: one request uses exactly one selected account.

### 3. Request orchestration

A new service layer coordinates:

- downstream API-key authentication and quota checks
- account selection using the existing routing helpers
- account-level scheduler lease acquisition
- key-level scheduler lease acquisition
- upstream execution
- local success/failure bookkeeping
- usage-event creation and outbox enqueue

The correct algorithm is:

1. authenticate the downstream API key
2. list candidate accounts
3. refresh stale or unknown account state when needed
4. apply account-level scheduler gating first
5. apply key-level scheduler gating second
6. execute upstream image request
7. on success:
   - decrement local account quota estimate
   - increment account success counters
   - increment key usage
   - enqueue one usage event
8. on failure:
   - increment account fail counters
   - classify invalid token vs temporary upstream failure
   - mark account unusable when the upstream token is invalid

### 4. Background workers

Two runtime workers are wired into startup:

- limited-account refresher:
  - periodically refreshes accounts currently marked limited
  - stops cleanly on shutdown
- outbox flusher:
  - moves pending SQLite outbox events into DuckDB in bounded batches
  - marks flushed rows in SQLite
  - stops cleanly on shutdown

These workers are part of normal runtime startup, not just helper functions.

## Data Model Decisions

### Accounts

`AccountRecord` remains the durable unit for upstream credentials.

The first complete version must support importing:

- raw `access_token`
- session JSON copied from `chatgpt.com/api/auth/session`

Session imports must extract:

- `accessToken`
- optional `sessionToken`
- optional account metadata when present

The normalized account name should stay stable and deterministic. Account status
values should remain Rust-native and consistent across admin and routing logic.

### Runtime config

`runtime_config` must become live runtime input rather than dead schema. The
service needs defaults for:

- account refresh windows
- default local scheduler caps
- usage flush batch size
- usage flush interval

## Error Handling

Errors should stay explicit and protocol-faithful.

- bad downstream request shape: `400`
- bad/missing downstream auth key: `401`
- no routeable account or exhausted local quota: `429`
- upstream account invalid/revoked: `502` to the caller, plus local account
  state mutation
- unexpected upstream protocol failure: `502`
- internal storage/runtime fault: `500`

The service should emit structured JSON error bodies with one stable `error`
string and optional `detail`.

## Testing Strategy

The completed slice is only acceptable if it includes:

- unit coverage for import normalization and request-shape conversion
- integration coverage for the new admin write endpoints
- mocked upstream coverage for:
  - account refresh
  - image generation
  - image edit
  - response/chat-completions compatibility paths
  - outbox flush worker
- local scheduler tests that confirm account-level gating still works
- one real operator validation path using the provided token and the mandated
  local proxy

## Validation Standard

Implementation is only considered complete when all of the following pass:

- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`
- a local service can boot with proxy `127.0.0.1:11118`
- the provided session material can be imported/login-tested
- all public image endpoints listed above can be exercised successfully

