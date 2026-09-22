# VivadoServer client guide

This guide describes the Linux workflow API for a trusted native desktop or CLI client. The running service exposes its machine-readable contract at `/openapi.json`. Legacy `/v1/sessions` and `/v1/projects` routes are removed. JSON request objects reject unknown fields.

## 1. Acquire the workflow

Generate a fresh UUID before sending `PUT /v1/workflows/{workflow_id}`. Persist this ID locally so a lost HTTP response can be resolved using the same ID. All protected requests require `Authorization: Bearer <token>`.

<!-- contract:CreateWorkflowRequest -->
```json
{
  "project": "demo",
  "reset_project": true
}
```

The server has one global workflow slot. A different active workflow causes `409 workflow_busy`. Repeating PUT with the same ID and parameters returns that workflow; different parameters cause a conflict. This retry identity is limited to the server process and retention window. Do not reuse an ID for a new workflow.

<!-- contract:WorkflowInfo -->
```json
{
  "workflow_id": "8e47a0ac-c6f5-4af0-a491-f9e7ce98efaf",
  "project": "demo",
  "status": "preparing",
  "session_id": null,
  "requires_full_upload": true,
  "cleanup_pending": false,
  "started_at": "2026-09-12T00:00:00Z",
  "last_heartbeat_at": "2026-09-12T00:00:00Z",
  "ended_at": null,
  "error_code": null,
  "error_message": null
}
```

A project without a valid clean marker requires `reset_project:true`; otherwise creation returns `409 project_reupload_required`. Reset means replacing the entire server project with a complete client manifest at commit. Files omitted from that manifest will be removed. A valid existing project can use `reset_project:false`, optionally perform incremental pushes, and then start Vivado.

Call `POST /v1/workflows/{workflow_id}/heartbeat` throughout preparation, execution, and pulling. The default deadline is 120 seconds; a 30-second client interval leaves room for delays. Activity and output polling do not replace explicit heartbeat calls. Terminal records default to 3600 seconds and at most 128 workflows.

| Workflow status | Allowed next work |
|---|---|
| `preparing` | Manifest, push, or start the one session after required upload |
| `running` | Tcl stdin/output and heartbeat |
| `pulling` | Manifest, pull plan/download, then finish |
| `stopping` | Observe cleanup; capacity is still occupied |
| `completed` / `cancelled` / `failed` | Inspect retained results; create a new workflow for new work |

Use `GET /v1/workflows/{workflow_id}` to observe transitions. `DELETE` cancels the workflow and cleans up accepted work; it is idempotent after a terminal result. `POST .../finish` completes only `pulling` and rejects active transfers with 409. Finish is idempotent after completion. Stop heartbeats only after observing a terminal workflow.

## 2. Paths and manifests

A project is one non-empty ASCII segment containing letters, digits, `_`, `-`, and `.`, at most 64 bytes; `.`, `..`, and the server metadata name `.vivado-server` are reserved. Linux names are case-sensitive.

Sync paths are UTF-8 relative paths with `/` separators, at most 4096 bytes and 255 bytes per segment. Reject empty segments, `.`/`..`, absolute paths, control characters, `: < > " | ? *`, backslashes, and reserved `.vivado-server` segments. Symlinks and non-regular files are unsupported. Encode each URL segment individually, preserving separating slashes.

File entries require `size_bytes`, `mtime_unix_ms`, and a 64-digit SHA-256. The push example below describes the six bytes `hello\n`. Directory entries use `{"path":"rtl","kind":"dir"}`; directory timestamps are not synchronized. Missing parent directories are synthesized. Hash through one local file handle and compare metadata before/after; re-scan if a source changes.

File equality includes size, millisecond mtime, SHA-256, and `executable`. On Linux, any ordinary execute bit means true; installation sets all `0111` bits for true and clears them for false. Exact permission masks, ownership, special bits, and links are not preserved.

For an optional server manifest, send this to `POST .../sync/manifest` in preparing or pulling:

<!-- contract:ManifestRequest -->
```json
{
  "include_globs": [],
  "exclude_globs": []
}
```

Glob lists allow at most 256 patterns, each at most 4096 bytes. Empty includes select everything; exclusions win. A reset push requires both lists empty so the manifest covers the complete replacement project.

## 3. Push and inspect the commit

Send `POST /v1/workflows/{workflow_id}/sync/push/plan`:

<!-- contract:PushPlanRequest -->
```json
{
  "entries": [
    {
      "path": "input.txt",
      "kind": "file",
      "size_bytes": 6,
      "mtime_unix_ms": 1700000000000,
      "sha256": "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
      "executable": false
    }
  ],
  "delete_extra": false,
  "include_globs": [],
  "exclude_globs": []
}
```

`entries` is required for both push and pull. An explicit empty array is valid; for reset it intentionally installs an empty project. The response gives `sync_id`, `upload_files`, `create_dirs`, `delete_files`, `delete_dirs`, and `expires_at`. Keep one open plan at a time; commit or abort it before another plan or session.

A reset requests every manifest file even if the old project has matching bytes. It replaces the complete tree independently of `delete_extra`. An ordinary incremental push uses `delete_extra` only to decide removal of unrelated extra paths. Deletions necessary for file/directory type replacement are still part of that replacement. A non-empty directory cannot be replaced without allowing removal of its contents.

For every returned upload, stream exact raw bytes:

```http
PUT /v1/workflows/{workflow_id}/sync/{sync_id}/files/{path}
Authorization: Bearer <token>
Content-Type: application/octet-stream
Content-Length: <planned size>
```

Known oversized lengths are rejected before body reads; chunked uploads stop when they exceed the plan. Hash and size must match. Uploads have configured idle and total deadlines. A failed retry preserves previously verified staging; retry identical bytes while the plan remains open.

Commit with `POST .../sync/{sync_id}/commit` and exactly this body:

<!-- contract:CommitSyncRequest -->
```json
{}
```

There is no `force` option. Accepted commit work continues if the HTTP connection disappears. Query `GET .../sync/{sync_id}`, or repeat commit while the workflow still permits it, before creating replacement work.

<!-- contract:SyncStatusResponse -->
```json
{
  "sync_id": "c0af7449-eaac-4c61-912f-0d9b4c79d393",
  "project": "demo",
  "status": "committed",
  "cleanup_pending": false,
  "result": {
    "sync_id": "c0af7449-eaac-4c61-912f-0d9b4c79d393",
    "status": "committed",
    "uploaded_files": [
      "input.txt"
    ],
    "created_dirs": [],
    "deleted_files": [],
    "deleted_dirs": []
  }
}
```

Sync status is `open`, `committing`, `committed`, `aborted`, `expired`, or `failed`. `cleanup_pending` is independent: committed plus cleanup pending still means the project modification succeeded. A failure includes `error_code` and `error_message`. `DELETE .../sync/{sync_id}` aborts an open plan, not an executing commit. Results expire according to sync retention.

If the workflow fails or reports `project_reupload_required`, preserve the trusted local source, create a new workflow with reset enabled, and upload its complete manifest. After server restart, old workflow/sync IDs are not durable; inspect a new creation response and perform reset if required.

## 4. Run and finish Vivado

Send `POST /v1/workflows/{workflow_id}/session` after completing preparation:

<!-- contract:StartSessionRequest -->
```json
{
  "args": [
    "-nolog",
    "-nojournal"
  ]
}
```

The server forces `-mode tcl`. Do not send `-mode`, `-gui`, `-batch`, or `-tcl`. Arguments allow at most 128 items and 32 KiB total, without NUL. The project belongs to the workflow and is not a session request field. Each workflow creates at most one session.

Send Tcl to `POST .../session/stdin`:

<!-- contract:SendInputRequest -->
```json
{
  "text": "puts [version -short]\n"
}
```

Input text is bounded by `stdin_max_bytes` and written as UTF-8 bytes through a raw PTY, without kernel echo or CRLF conversion. Terminate through the API instead of expecting stdin control characters to send signals. Output may contain terminal control sequences. Send `GET .../session/output?cursor=0&timeout_ms=30000`, then use each response cursor for the next request:

<!-- contract:OutputResponse -->
```json
{
  "cursor": 1,
  "chunks": [
    {
      "seq": 0,
      "timestamp": "2026-09-12T00:00:00Z",
      "text": "Vivado% "
    }
  ],
  "status": "running",
  "overrun": false,
  "output_truncated": false
}
```

Concatenate chunk text; chunk boundaries are not line or prompt boundaries. `overrun` means older requested output was evicted; `output_truncated` indicates output was truncated, including a bounded final drain. Surface both to the user. The default long poll is 30 seconds, capped at 60 seconds; `timeout_ms=0` returns immediately. Polling with no new output returns a normal response, not a command failure. The server cancels ordinary waits on shutdown.

`GET .../session` reports `running`, `stopping`, `exited`, `terminated`, or `failed`, together with exit code and termination reason. `stopping` is not proof that the process has died. `DELETE .../session` explicitly terminates Vivado and fails an unfinished workflow; it is not the successful exit operation.

For success, send Tcl `exit 0` after checking build results. The workflow enters `pulling` only after natural exit with code zero and confirmed cleanup. Nonzero exit, forced stop, output/input failures, or interrupted execution make the project require full re-upload. Tcl `ERROR` text alone does not decide success: use Tcl `catch` or command-specific status checks and exit nonzero when your build criteria fail.

## 5. Pull and release the slot

Only pulling accepts `POST /v1/workflows/{workflow_id}/sync/pull/plan`:

<!-- contract:PullPlanRequest -->
```json
{
  "entries": [],
  "delete_extra": false,
  "include_globs": [
    "out/**"
  ],
  "exclude_globs": []
}
```

For each `download_files` entry, send its content digest:

```http
GET /v1/workflows/{workflow_id}/sync/files/{path}
Authorization: Bearer <token>
If-Match: "sha256:5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
```

The response includes `ETag`, `x-sync-size-bytes`, `x-sync-mtime-unix-ms`, `x-sync-sha256`, and `x-sync-executable`. A stale content hash returns 412. Metadata-only changes do not change the ETag. Detected changes during pre-response hashing return 409; a later in-place change cannot turn an already-started body into a JSON error.

Stream each body into a local temporary file. Validate complete size/hash and compare response metadata to the plan, close it, atomically install it, then apply mtime/executable state. Discard and re-plan on mismatch or interrupted transfer. Apply returned type-replacement deletions before installing their replacements. `delete_extra` controls unrelated extra paths; it does not suppress required type replacements. Remove directories deepest-first.

After all response bodies finish and local results pass validation, call `POST .../finish`. A 409 for active transfers means close or finish those bodies first and retry. Do not release the workflow while a download is still being consumed.

## 6. Errors and trust

`GET /healthz`, `GET /readyz`, and `GET /openapi.json` are public. Readiness reports service coordination/cleanup health without launching Vivado or proving license availability. All application responses have `x-request-id`; 401 also has `WWW-Authenticate: Bearer`.

```json
{
  "error": {
    "code": "workflow_busy",
    "message": "another workflow is active",
    "request_id": "8e47a0ac-c6f5-4af0-a491-f9e7ce98efaf",
    "details": {}
  }
}
```

`details` is always an object. Record the request ID when reporting failures. Codes distinguish workflow phase, project re-upload, and sync conflicts; do not branch on human-readable messages.

| HTTP status | Meaning |
|---|---|
| 400 / 401 / 404 | Invalid request / authentication / missing or expired resource |
| 408 / 413 | Request-body timeout / body or file size limit |
| 409 | Workflow busy, wrong phase, sync conflict, or full re-upload required |
| 412 / 429 | Stale content precondition / bounded request capacity |
| 500 / 503 | Internal error / shutting down or unavailable |

HTTP cancellation does not undo an accepted state change. Query authoritative state after reconnecting. Do not blindly replay Tcl stdin: it may already have executed, and the stdin endpoint is not an idempotent command queue.

Bearer access permits Tcl `exec` as the service identity. Use a dedicated account, trusted sources, and an internal/VPN network with TLS or a trusted HTTPS boundary. Browser CORS and execution sandboxing are not provided.

Sync results are retained for `sync_result_retention_secs`, with at most 128 settled results across the service. Old results can therefore return 404 before the time limit. A workflow retains references to at most its 128 newest plans. Persist results needed by the client instead of using the service as an audit archive.
