# gpt2api-rs

Standalone Rust gateway for ChatGPT web image generation accounts. The service
keeps control-plane state in local SQLite, usage summaries in DuckDB, and
exposes a small admin REST surface plus a matching CLI.

## Commands

Run the service:

```bash
cargo run -- serve \
  --listen 127.0.0.1:8787 \
  --storage-dir /tmp/gpt2api \
  --admin-token secret
```

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
