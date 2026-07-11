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
api_json_body_limit_bytes = 67108864
session_retention_secs = 3600
stdin_max_bytes = 1048576

sync_max_file_bytes = 1073741824
sync_max_manifest_entries = 200000
sync_session_ttl_secs = 3600

# [tls]
# cert_path = "server.crt"
# key_path = "server.key"
```

`auth_tokens` must contain at least one non-empty token. When `listen_addr` is
not loopback, configure TLS (or terminate TLS in a trusted reverse proxy); the
server logs a warning because bearer tokens and project data otherwise travel
in cleartext.

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
Errors, including extractor and unknown-route errors, use a JSON
`{"error":"..."}` body. Internal diagnostics stay in server logs.

- `POST /v1/sessions`: start Vivado in `workspace_root/<project>`.
- `POST /v1/sessions/{session_id}/stdin`: send text to the interactive process.
- `GET /v1/sessions/{session_id}/output?cursor=0&timeout_ms=30000`: long-poll
  output chunks.
- `POST /v1/sessions/{session_id}/heartbeat`: keep the session alive.
- `DELETE /v1/sessions/{session_id}`: terminate the session.

Terminal sessions remain queryable for `session_retention_secs`, then the
heartbeat reaper removes them. Heartbeats are accepted only while a session is
running, and a single stdin request is limited by `stdin_max_bytes`.

On Windows, an extensionless absolute `vivado_path` automatically resolves to
the adjacent `.bat` wrapper when present. Batch wrappers are launched through
`cmd.exe`; request arguments containing cmd metacharacters are rejected instead
of being interpolated into a shell command.

## File sync API

Sync is file-level and scoped to `workspace_root/<project>`. Paths are UTF-8
relative paths using `/`; absolute paths, `..`, Windows drive prefixes, and the
internal `.vivado-server-sync/` directory are rejected.

For deterministic Windows/Linux behavior, path segments also reject control
characters, trailing dots/spaces, Windows device names such as `CON` and
`COM1`, segments over 255 UTF-8 bytes, and links/reparse points. A complete sync
path is limited to 4096 UTF-8 bytes.

For a full client implementation guide, see
[`docs/client-development.zh-CN.md`](docs/client-development.zh-CN.md). An
English reference is also available at
[`docs/client-development.md`](docs/client-development.md).
Maintainers should also read [`docs/architecture.md`](docs/architecture.md) for
the concurrency, rollback, path-boundary, and failure-model invariants.

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
extra files. The only exception is a same-path file/directory type replacement:
its conflicting target is listed for deletion because the requested entry
cannot otherwise be created. Replacing a non-empty directory with a file
requires `delete_extra=true`.

Push commits are serialized per project. Baseline checks and all mutations run
under that project lock; overwritten/deleted paths are first moved to a rollback
area on the same filesystem. If a later operation fails, prior mutations are
restored and uploaded files remain staged for retry. Missing parent directory
entries are synthesized, and uppercase SHA-256 input is normalized to lowercase.
