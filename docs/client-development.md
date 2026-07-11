# Vivado Server Client Development Guide

This document is for client developers integrating with `vivado-server`.
It covers the HTTP API used to synchronize project files and control remote
Vivado CLI sessions.

## 1. Base Rules

### Base URL

All API examples use:

```text
http://127.0.0.1:8080
```

Production deployments may use HTTPS when server TLS is configured or when a
reverse proxy terminates TLS.

### Authentication

All `/v1/*` routes require Bearer token authentication:

```http
Authorization: Bearer <token>
```

Unauthenticated or invalid-token requests return:

```http
401 Unauthorized
Content-Type: application/json

{"error":"unauthorized"}
```

`GET /healthz` does not require authentication.

### JSON And Binary Bodies

- JSON endpoints use `Content-Type: application/json`.
- File upload endpoints send raw bytes in the request body.
- File download endpoints return raw bytes with checksum metadata in response
  headers.

### Error Shape

Errors are JSON:

```json
{
  "error": "human-readable message"
}
```

Common status codes:

| Status | Meaning |
|---:|---|
| 400 | Invalid request, invalid path, invalid manifest, checksum mismatch |
| 401 | Missing or invalid Bearer token |
| 404 | Session, sync session, or file not found |
| 409 | Conflict, session limit reached, missing upload, commit conflict |
| 413 | JSON request exceeds `api_json_body_limit_bytes` |
| 500 | Server-side failure |

Unknown `/v1/*` routes and extractor failures use the same JSON error shape.
Unknown versioned routes still require authentication. Internal OS/path details
are logged server-side and are not returned in a `500` body.

## 2. Naming And Path Rules

### Project Name

`project` appears in URLs:

```text
/v1/projects/{project}/...
/v1/sessions
```

Valid project names:

- non-empty
- not `.` or `..`
- ASCII letters, digits, `_`, `-`, `.`
- at most 64 bytes, with no trailing dot
- not a Windows device name such as `CON`, `NUL`, `COM1`, or `LPT1`

Invalid examples:

```text
../demo
demo/sub
demo:1
项目
```

The server maps a valid project to:

```text
workspace_root/<project>
```

### Sync File Path

Sync paths are UTF-8 relative paths inside a project directory.

Valid examples:

```text
src/top.tcl
rtl/core.v
constraints/top.xdc
out/result.txt
```

Invalid paths:

```text
                         # empty
.
..
../x
/absolute/path
a\b
C:/x
a:b
a//b
.vivado-server-sync/state
```

Rules:

- Use `/` as the separator on every platform.
- Do not send absolute paths.
- Do not send `.` or `..` segments.
- Do not send backslashes.
- Do not send Windows drive prefixes.
- The server-reserved `.vivado-server-sync/` directory is always rejected.
- Segments may not end in a dot/space, contain control characters, use Windows
  device names, or exceed 255 UTF-8 bytes. The full path limit is 4096 bytes.
- Symlinks, junctions, and other reparse points are rejected, including in
  existing ancestor directories.
- URL paths should be percent-encoded per segment when needed. In normal
  Vivado project paths using letters, digits, `_`, `-`, `.`, and `/`, no extra
  encoding is usually needed.

## 3. Manifest Model

The client computes a local manifest and sends it to plan push or pull syncs.
The server normalizes SHA-256 to lowercase and synthesizes missing directory
entries for accepted nested paths. A manifest is rejected if a file is an
ancestor of another entry.

### Manifest Entry

File entry:

```json
{
  "path": "src/top.tcl",
  "kind": "file",
  "size_bytes": 1234,
  "mtime_unix_ms": 1700000000000,
  "sha256": "64 lowercase or uppercase hex chars"
}
```

Directory entry:

```json
{
  "path": "src",
  "kind": "dir",
  "mtime_unix_ms": 1700000000000
}
```

Field rules:

| Field | File | Dir | Notes |
|---|---|---|---|
| `path` | required | required | normalized sync path |
| `kind` | required | required | `file` or `dir` |
| `size_bytes` | required | ignored | unsigned integer |
| `mtime_unix_ms` | required | optional | Unix epoch milliseconds |
| `sha256` | required | ignored | SHA-256 of file bytes, hex encoded |

The server supports only regular files and directories. Symlinks are not
supported in v1.

### Equality Rule

Two files are treated as equal only when all of these match:

- `size_bytes`
- `mtime_unix_ms`
- `sha256`

This is intentionally strict. If the client cannot preserve mtime locally, it
should still send accurate local mtime and expect changed files to be planned.

### Manifest Limits

Configured server defaults:

```toml
sync_max_file_bytes = 1073741824
sync_max_manifest_entries = 200000
sync_session_ttl_secs = 3600
```

The server rejects manifests and uploads that exceed these limits.

## 4. Glob Filtering

Sync requests may include filters:

```json
{
  "include_globs": ["src/**", "constraints/**"],
  "exclude_globs": ["**/*.tmp", ".git/**"]
}
```

Rules:

- Filters are supplied per request.
- There are no server-configured default include/exclude rules.
- If `include_globs` is empty, all non-excluded entries are included.
- If `include_globs` is non-empty, an entry must match at least one include glob.
- Any matching exclude glob removes the entry.
- `.vivado-server-sync` and `.vivado-server-sync/**` are always excluded.

Recommended client excludes for Vivado projects:

```json
[
  ".git/**",
  ".vivado-server-sync/**",
  "*.jou",
  "*.log",
  "*.str",
  ".Xil/**",
  "*.cache/**",
  "*.runs/**",
  "*.sim/**"
]
```

Use project-specific rules carefully. Excluding generated output is reasonable
for push, but pull may intentionally include selected output directories.

## 5. Sync Workflow Overview

### Push: Client To Server

Use push when the client wants to upload local project files to the Vivado
server.

Flow:

1. Client scans local project and computes manifest.
2. Client calls `POST /v1/projects/{project}/sync/push/plan`.
3. Server returns a `sync_id`, upload list, create-dir list, and optional delete
   list.
4. Client uploads every file in `upload_files`.
5. Client calls commit.
6. Server moves staged files into the project directory and optionally deletes
   extra server files.

Important:

- Nothing is overwritten until `commit`.
- Uploading all requested files is required.
- `sync_id` expires after `sync_session_ttl_secs`.
- If files changed on the server after planning, commit returns `409` unless
  `force=true`.

### Pull: Server To Client

Use pull when the client wants to download server-side generated files or make
the local directory match the server.

Flow:

1. Client scans local project and computes manifest.
2. Client calls `POST /v1/projects/{project}/sync/pull/plan`.
3. Server returns files to download, directories to create, and optional local
   delete instructions.
4. Client downloads each file with `GET /v1/projects/{project}/sync/files/{path}`.
5. Client verifies `x-sync-sha256`, size, and optionally applies mtime locally.
6. If `delete_extra=true`, client applies returned local deletions.

Important:

- Pull does not create server-side sync sessions.
- Delete lists in pull responses are instructions for the client, not actions
  performed by the server.

## 6. Sync API Reference

### 6.1 Get Server Manifest

```http
POST /v1/projects/{project}/sync/manifest
Authorization: Bearer <token>
Content-Type: application/json
```

Request:

```json
{
  "include_globs": [],
  "exclude_globs": [".git/**", ".Xil/**"]
}
```

Response:

```json
{
  "entries": [
    {
      "path": "src",
      "kind": "dir",
      "mtime_unix_ms": 1700000000000
    },
    {
      "path": "src/top.tcl",
      "kind": "file",
      "size_bytes": 1234,
      "mtime_unix_ms": 1700000000000,
      "sha256": "..."
    }
  ]
}
```

Notes:

- The server scans `workspace_root/<project>`.
- If the project directory does not exist, the server creates it and returns an
  empty manifest.
- Scanning rejects unsupported entries such as symlinks.

### 6.2 Create Push Plan

```http
POST /v1/projects/{project}/sync/push/plan
Authorization: Bearer <token>
Content-Type: application/json
```

Request:

```json
{
  "entries": [
    {
      "path": "src",
      "kind": "dir",
      "mtime_unix_ms": 1700000000000
    },
    {
      "path": "src/top.tcl",
      "kind": "file",
      "size_bytes": 1234,
      "mtime_unix_ms": 1700000000000,
      "sha256": "..."
    }
  ],
  "delete_extra": false,
  "include_globs": [],
  "exclude_globs": [".git/**", ".Xil/**"]
}
```

Response:

```json
{
  "sync_id": "8f6989d1-8a61-4dbb-98e4-b24a408729e2",
  "upload_files": [
    {
      "path": "src/top.tcl",
      "size_bytes": 1234,
      "mtime_unix_ms": 1700000000000,
      "sha256": "..."
    }
  ],
  "create_dirs": ["src"],
  "delete_files": [],
  "delete_dirs": [],
  "expires_at": "2026-06-27T12:34:56Z"
}
```

Response semantics:

- `upload_files`: files the client must upload before commit.
- `create_dirs`: directories the server will create during commit.
- `delete_files`: server files commit will delete. Extra files appear only with
  `delete_extra=true`; a same-path file/directory type conflict appears even
  when `delete_extra=false`.
- `delete_dirs`: analogous directory deletions. Replacing a non-empty directory
  with a file requires `delete_extra=true`.
- `expires_at`: time after which the server may remove the staging session.

### 6.3 Upload Push File

```http
PUT /v1/projects/{project}/sync/{sync_id}/files/{path}
Authorization: Bearer <token>
Content-Type: application/octet-stream
```

Request body is raw file bytes.

Example:

```sh
curl -X PUT \
  -H "Authorization: Bearer change-me" \
  --data-binary @src/top.tcl \
  "http://127.0.0.1:8080/v1/projects/demo/sync/8f6989d1-8a61-4dbb-98e4-b24a408729e2/files/src/top.tcl"
```

Response:

```json
{
  "path": "src/top.tcl",
  "size_bytes": 1234,
  "sha256": "..."
}
```

Validation:

- Path must be listed in `upload_files`.
- Uploaded byte count must match `size_bytes`.
- Uploaded SHA-256 must match `sha256`.
- Upload body is streamed; clients should not base64 encode file content.

If upload fails with `400`, the client should fix the local manifest/body
mismatch and create a new plan. Reusing the same plan is usually not useful.

### 6.4 Commit Push

```http
POST /v1/projects/{project}/sync/{sync_id}/commit
Authorization: Bearer <token>
Content-Type: application/json
```

Request:

```json
{
  "force": false
}
```

Response:

```json
{
  "sync_id": "8f6989d1-8a61-4dbb-98e4-b24a408729e2",
  "status": "committed",
  "uploaded_files": ["src/top.tcl"],
  "created_dirs": ["src"],
  "deleted_files": [],
  "deleted_dirs": []
}
```

Conflict behavior:

- Commit checks whether affected server files still match the baseline captured
  during planning.
- If a server file changed after planning, commit returns `409`.
- If `force=true`, the server skips this optimistic conflict check and overwrites
  affected files.

Missing upload behavior:

- If any planned upload was not uploaded, commit returns `409`.

Atomicity and concurrency:

- Planning and commit are serialized per project, so two commits cannot both
  pass the same baseline concurrently.
- Commit first moves replaced/deleted paths to a same-filesystem rollback area.
  If a later operation fails, earlier mutations are restored and uploads are
  returned to staging for retry.
- Directories containing entries excluded from the plan are not silently
  deleted; commit returns `409` and rolls back.

Recommended client behavior:

- Default to `force=false`.
- On `409`, show a conflict message and offer to re-plan.
- Use `force=true` only after explicit user confirmation.

### 6.5 Abort Push

```http
DELETE /v1/projects/{project}/sync/{sync_id}
Authorization: Bearer <token>
```

Response:

```json
{
  "sync_id": "8f6989d1-8a61-4dbb-98e4-b24a408729e2",
  "status": "aborted"
}
```

Abort removes the staging directory and does not modify project files. Clients
should call abort when a push is canceled after a plan has been created.

### 6.6 Create Pull Plan

```http
POST /v1/projects/{project}/sync/pull/plan
Authorization: Bearer <token>
Content-Type: application/json
```

Request:

```json
{
  "entries": [
    {
      "path": "src/top.tcl",
      "kind": "file",
      "size_bytes": 1234,
      "mtime_unix_ms": 1700000000000,
      "sha256": "..."
    }
  ],
  "delete_extra": true,
  "include_globs": ["out/**"],
  "exclude_globs": []
}
```

Response:

```json
{
  "download_files": [
    {
      "path": "out/result.txt",
      "size_bytes": 2048,
      "mtime_unix_ms": 1700000010000,
      "sha256": "..."
    }
  ],
  "create_dirs": ["out"],
  "delete_files": ["old.txt"],
  "delete_dirs": []
}
```

Response semantics:

- `download_files`: files the client should download from the server.
- `create_dirs`: local directories the client should create.
- `delete_files`: local files the client should delete only if it requested
  `delete_extra=true`.
- `delete_dirs`: local directories the client should delete only if it requested
  `delete_extra=true`.

### 6.7 Download Server File

```http
GET /v1/projects/{project}/sync/files/{path}
Authorization: Bearer <token>
```

Response headers:

```http
Content-Type: application/octet-stream
Content-Length: <size>
x-sync-size-bytes: <size>
x-sync-mtime-unix-ms: <mtime>
x-sync-sha256: <sha256>
```

Response body is raw file bytes.

Client requirements:

- Stream the response to a temporary local file.
- Hash while downloading.
- Verify byte count and `x-sync-sha256`.
- Move the temporary file into place only after verification succeeds.
- Apply mtime locally if the platform supports it.

## 7. Vivado Session API

The sync API moves files. The session API controls server-side Vivado.

### 7.1 Create Session

```http
POST /v1/sessions
Authorization: Bearer <token>
Content-Type: application/json
```

Request:

```json
{
  "project": "demo",
  "args": ["-mode", "tcl"]
}
```

Response:

```json
{
  "session_id": "0d7e0c3a-2ca7-4725-b875-3e9a9f34bb3c",
  "project": "demo",
  "status": "running",
  "started_at": "2026-06-27T12:00:00Z",
  "last_heartbeat_at": "2026-06-27T12:00:00Z",
  "exit_code": null
}
```

The server starts Vivado in `workspace_root/<project>`.

On Windows, an extensionless absolute `vivado_path` resolves to its adjacent
`.bat` wrapper when available. The wrapper is launched through `cmd.exe`, and
arguments containing cmd metacharacters are rejected. Clients should pass
complex Tcl commands over stdin rather than command-line arguments.

### 7.2 Send Stdin

```http
POST /v1/sessions/{session_id}/stdin
Authorization: Bearer <token>
Content-Type: application/json
```

Request:

```json
{
  "text": "open_project demo.xpr\n"
}
```

The server writes `text` to the Vivado process stdin.
The JSON request is rejected when UTF-8 `text` exceeds `stdin_max_bytes`.

### 7.3 Read Output

```http
GET /v1/sessions/{session_id}/output?cursor=0&timeout_ms=30000
Authorization: Bearer <token>
```

Response:

```json
{
  "cursor": 12,
  "chunks": [
    {
      "seq": 0,
      "timestamp": "2026-06-27T12:00:01Z",
      "text": "Vivado ..."
    }
  ],
  "status": "running",
  "overrun": false
}
```

Client behavior:

- Start with `cursor=0`.
- After every response, set next request cursor to returned `cursor`.
- If `overrun=true`, the server output buffer dropped older chunks. Inform the
  user that output was truncated and continue from the returned cursor.
- `timeout_ms` is capped by the server.

### 7.4 Heartbeat

```http
POST /v1/sessions/{session_id}/heartbeat
Authorization: Bearer <token>
```

The client should call heartbeat before `heartbeat_timeout_secs` expires. A
reasonable interval is one third of the configured timeout.
Heartbeat on a terminal session returns `409`.

### 7.5 Get Or Delete Session

```http
GET /v1/sessions/{session_id}
DELETE /v1/sessions/{session_id}
Authorization: Bearer <token>
```

Statuses:

```text
running
exited
terminated
failed
```

`ended_at` is present for terminal sessions. They remain queryable for
`session_retention_secs` before the reaper removes them.

`DELETE` asks Vivado to exit and then force-kills it after a short grace period.

## 8. Recommended Client Algorithms

### 8.1 Local Manifest Generation

Pseudo-code:

```text
manifest = []
for each filesystem entry under local_project_root:
    rel = normalize to UTF-8 relative path with "/" separators
    reject absolute paths, "..", backslash, drive prefix
    skip .vivado-server-sync/
    apply include/exclude globs
    if symlink:
        reject or skip with warning
    if directory:
        manifest.push({ path, kind: "dir", mtime_unix_ms })
    if regular file:
        sha256 = stream_hash(file)
        manifest.push({
            path,
            kind: "file",
            size_bytes,
            mtime_unix_ms,
            sha256
        })
sort manifest by path
```

Use streaming SHA-256. Do not read large files fully into memory.

### 8.2 Safe Push Implementation

Pseudo-code:

```text
local_manifest = scan_local()
plan = POST push/plan(local_manifest, filters, delete_extra)
try:
    for file in plan.upload_files:
        PUT raw bytes to /sync/{sync_id}/files/{file.path}
    commit = POST /sync/{sync_id}/commit {"force": false}
catch cancellation:
    DELETE /sync/{sync_id}
catch upload/commit failure:
    DELETE /sync/{sync_id} if sync still exists
```

For upload retries:

- Retrying the same `PUT` is acceptable if the body is identical.
- If the local file changed during upload, stop and create a fresh plan.
- If commit returns `409`, re-plan instead of blindly forcing.

### 8.3 Safe Pull Implementation

Pseudo-code:

```text
local_manifest = scan_local()
plan = POST pull/plan(local_manifest, filters, delete_extra)
for dir in plan.create_dirs:
    mkdir -p local_root/dir
for file in plan.download_files:
    download to local_root/file.path.tmp
    verify size and sha256
    rename tmp to local_root/file.path
    apply mtime if possible
if delete_extra:
    delete plan.delete_files
    delete plan.delete_dirs deepest-first
```

Use atomic rename where the target filesystem supports it.

### 8.4 Typical Remote Vivado Flow

```text
1. Push source/project files to server.
2. Create Vivado session for the same project.
3. Send Tcl commands over stdin.
4. Poll output and send heartbeat while running.
5. Delete or wait for session exit.
6. Pull selected output files back to client.
```

## 9. Concurrency And Consistency

The server allows sync while Vivado sessions are running. This is flexible but
clients must be deliberate:

- Avoid pushing source changes while a Vivado run is reading the same project.
- Prefer pushing before starting Vivado.
- Prefer pulling outputs after the Vivado session exits or reaches a known safe
  checkpoint.
- Use `force=false` by default so server-side changes are detected at commit.

## 10. Compatibility Notes

### Windows Clients

- Convert `\` to `/` before sending paths.
- Strip drive prefixes.
- mtime precision may differ by filesystem. The server compares
  `mtime_unix_ms`, so clients should send the best available millisecond value.

### Linux/macOS Clients

- Do not send symlinks in v1.
- Preserve executable bits is not supported by this API version.

### Large Files

- Upload and download as streams.
- Hash while streaming.
- Use temporary files and atomic rename.
- Respect `sync_max_file_bytes`.

## 11. Minimal Push Example

Assume local `src/top.tcl` contains `puts hello\n`.

Create a plan:

```json
POST /v1/projects/demo/sync/push/plan

{
  "entries": [
    {
      "path": "src",
      "kind": "dir",
      "mtime_unix_ms": 1700000000000
    },
    {
      "path": "src/top.tcl",
      "kind": "file",
      "size_bytes": 11,
      "mtime_unix_ms": 1700000000000,
      "sha256": "<sha256>"
    }
  ],
  "delete_extra": false,
  "include_globs": [],
  "exclude_globs": []
}
```

Upload:

```text
PUT /v1/projects/demo/sync/<sync_id>/files/src/top.tcl
```

Commit:

```json
POST /v1/projects/demo/sync/<sync_id>/commit

{
  "force": false
}
```

## 12. Minimal Pull Example

Plan:

```json
POST /v1/projects/demo/sync/pull/plan

{
  "entries": [],
  "delete_extra": false,
  "include_globs": ["out/**"],
  "exclude_globs": []
}
```

Download each returned file:

```text
GET /v1/projects/demo/sync/files/out/result.txt
```

Verify:

```text
sha256(downloaded bytes) == x-sync-sha256
downloaded byte count == x-sync-size-bytes
```

## 13. Endpoint Summary

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | Health check, no auth |
| POST | `/v1/projects/{project}/sync/manifest` | Get server manifest |
| POST | `/v1/projects/{project}/sync/push/plan` | Plan client-to-server sync |
| PUT | `/v1/projects/{project}/sync/{sync_id}/files/{path}` | Upload planned file |
| POST | `/v1/projects/{project}/sync/{sync_id}/commit` | Commit push sync |
| DELETE | `/v1/projects/{project}/sync/{sync_id}` | Abort push sync |
| POST | `/v1/projects/{project}/sync/pull/plan` | Plan server-to-client sync |
| GET | `/v1/projects/{project}/sync/files/{path}` | Download server file |
| POST | `/v1/sessions` | Start Vivado session |
| GET | `/v1/sessions/{session_id}` | Get session status |
| POST | `/v1/sessions/{session_id}/stdin` | Send Vivado input |
| GET | `/v1/sessions/{session_id}/output` | Poll Vivado output |
| POST | `/v1/sessions/{session_id}/heartbeat` | Keep session alive |
| DELETE | `/v1/sessions/{session_id}` | Terminate session |
