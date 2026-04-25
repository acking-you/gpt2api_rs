# gpt2api-rs

Standalone Rust gateway for ChatGPT web image generation accounts. The service
keeps control-plane state in local SQLite, usage summaries in DuckDB, and
exposes a small admin REST surface plus a matching CLI.

## Documentation

- [`docs/architecture.md`](docs/architecture.md): current module layout, storage
  model, HTTP surfaces, and known gaps
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
- `GET /v1/models`

Admin:

- `GET /admin/status`
- `GET /admin/accounts`
- `GET /admin/keys`
- `GET /admin/usage`

The image-generation POST handlers are still the next implementation slice.

## Local quality gate

The repository CI matches these local commands:

```bash
cargo +stable fmt --check
cargo +stable clippy --all-targets --all-features
cargo +stable test
```
