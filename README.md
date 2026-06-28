# Vivado Server

`vivado-server` exposes a server-side Vivado CLI through an authenticated HTTP
API. It is intended for machines that keep Vivado installed locally while remote
clients control interactive `vivado -mode tcl` sessions over REST.

## Quick start

Create a config file, or start from `config.example.toml`:

```toml
listen_addr = "0.0.0.0:8080"
vivado_path = "vivado"
workspace_root = "C:/vivado-workspaces"
auth_tokens = ["change-me"]

max_active_sessions = 1
heartbeat_timeout_secs = 120
output_buffer_bytes = 1048576

sync_max_file_bytes = 1073741824
sync_max_manifest_entries = 200000
sync_session_ttl_secs = 3600

# [tls]
# cert_path = "server.crt"
# key_path = "server.key"
```

Start the service:

```sh
cargo run -- --config config.toml
```

Enable detailed debug logging:

```sh
RUST_LOG=vivado_server=debug,tower_http=debug cargo run -- --config config.toml
```

PowerShell:

```powershell
$env:RUST_LOG = "vivado_server=debug,tower_http=debug"
cargo run -- --config .\config.toml
```

Use `vivado_server=trace` when you also need per-manifest-entry and upload chunk
diagnostics. Logs include request/session/sync identifiers, paths, sizes, hashes,
counts, and timings; authorization tokens and file contents are not logged.

Create a session:

```sh
curl -H "Authorization: Bearer change-me" \
  -H "Content-Type: application/json" \
  -d '{"project":"demo","args":["-mode","tcl"]}' \
  http://127.0.0.1:8080/v1/sessions
```

## API shape

All `/v1/*` routes require `Authorization: Bearer <token>`.

- `POST /v1/sessions`: start Vivado in `workspace_root/<project>`.
- `POST /v1/sessions/{session_id}/stdin`: send text to the interactive process.
- `GET /v1/sessions/{session_id}/output?cursor=0&timeout_ms=30000`: long-poll
  output chunks.
- `POST /v1/sessions/{session_id}/heartbeat`: keep the session alive.
- `DELETE /v1/sessions/{session_id}`: terminate the session.

## File sync API

Sync is file-level and scoped to `workspace_root/<project>`. Paths are UTF-8
relative paths using `/`; absolute paths, `..`, Windows drive prefixes, and the
internal `.vivado-server-sync/` directory are rejected.

For a full client implementation guide, see
[`docs/client-development.zh-CN.md`](docs/client-development.zh-CN.md). An
English reference is also available at
[`docs/client-development.md`](docs/client-development.md).

Get the server manifest:

```sh
curl -H "Authorization: Bearer change-me" \
  -H "Content-Type: application/json" \
  -d '{"exclude_globs":[".git/**","target/**"]}' \
  http://127.0.0.1:8080/v1/projects/demo/sync/manifest
```

Plan a push from a client-computed manifest:

```sh
curl -H "Authorization: Bearer change-me" \
  -H "Content-Type: application/json" \
  -d '{"delete_extra":false,"entries":[{"path":"src/top.tcl","kind":"file","size_bytes":11,"mtime_unix_ms":1700000000000,"sha256":"..."}]}' \
  http://127.0.0.1:8080/v1/projects/demo/sync/push/plan
```

Upload each planned file as raw bytes, then commit:

```sh
curl -X PUT -H "Authorization: Bearer change-me" \
  --data-binary @src/top.tcl \
  http://127.0.0.1:8080/v1/projects/demo/sync/<sync_id>/files/src/top.tcl

curl -H "Authorization: Bearer change-me" \
  -H "Content-Type: application/json" \
  -d '{"force":false}' \
  http://127.0.0.1:8080/v1/projects/demo/sync/<sync_id>/commit
```

Plan a pull and download server files:

```sh
curl -H "Authorization: Bearer change-me" \
  -H "Content-Type: application/json" \
  -d '{"delete_extra":true,"entries":[]}' \
  http://127.0.0.1:8080/v1/projects/demo/sync/pull/plan

curl -H "Authorization: Bearer change-me" \
  -o result.txt \
  http://127.0.0.1:8080/v1/projects/demo/sync/files/out/result.txt
```

`delete_extra=true` is explicit. Without it, sync plans never delete target-side
extra files.
