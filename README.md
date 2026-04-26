# gpt2api-rs

Standalone Rust gateway for ChatGPT web image generation accounts. The service
keeps control-plane state in local SQLite, usage summaries in DuckDB, and
exposes a small admin REST surface plus a matching CLI.

## Documentation

- [`docs/architecture.md`](docs/architecture.md): current module layout, storage
  model, HTTP surfaces, and known gaps
- [`skills/gpt2api-image-request/SKILL.md`](skills/gpt2api-image-request/SKILL.md):
  downstream user workflow for requesting and retrieving images through
  `https://ackingliu.top/api/gpt2api`
- [`.github/workflows/ci.yml`](.github/workflows/ci.yml): repository CI that runs
  formatting, clippy, and tests on stable Rust

## Commands

Run the service:

```bash
cargo run -- serve \
  --listen 127.0.0.1:8787 \
  --storage-dir /tmp/gpt2api \
  --admin-token secret
```

Optional image completion emails use the same SMTP account file shape as
StaticFlow:

```text
backend/.local/email_accounts.json
```

or an explicit path through `GPT2API_EMAIL_ACCOUNTS_FILE` / `EMAIL_ACCOUNTS_FILE`.
The service reads `public_mailbox.smtp_host`, `public_mailbox.smtp_port`,
`public_mailbox.username`, `public_mailbox.app_password`, and
`public_mailbox.display_name`. Set `GPT2API_PUBLIC_BASE_URL` or `SITE_BASE_URL`
so generated email links point at the public `/gpt2api/share/<token>` page.
`GPT2API_SMTP_*` variables still override the file values when present.

List imported accounts through the admin CLI:

```bash
cargo run -- admin \
  --base-url http://127.0.0.1:8787 \
  --admin-token secret \
  accounts list --json
```

List keys:

```bash
cargo run -- admin \
  --base-url http://127.0.0.1:8787 \
  --admin-token secret \
  keys list --json
```

List recent usage:

```bash
cargo run -- admin \
  --base-url http://127.0.0.1:8787 \
  --admin-token secret \
  usage list --limit 50 --json
```

## Currently implemented HTTP endpoints

Public:

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

Admin:

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

Downstream image requests should use `Authorization: Bearer <GPT2API_KEY>` and
the public base URL `https://ackingliu.top/api/gpt2api`; see the image request
skill above for end-to-end examples.

## Local quality gate

The repository CI matches these local commands:

```bash
cargo +stable fmt --check
cargo +stable clippy --all-targets --all-features
cargo +stable test
```
