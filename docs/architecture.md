# Architecture and invariants

VivadoServer is a Linux service for one trusted client workflow at a time. The workflow owns the global slot across source preparation, Vivado execution, and output transfer. A token grants administrative Tcl execution as the service account; filesystem validation is not a sandbox for Tcl or for another local writer.

## Ownership and state

`AppRuntime` validates startup input, acquires exclusive workspace ownership, constructs project, workflow, session, and sync services, and owns shutdown. Router construction does not start background tasks. Authentication digests are separate from non-secret runtime configuration; raw configured tokens do not remain in cloned service settings.

The client chooses a UUID for `PUT /v1/workflows/{id}`. The same ID and creation parameters observe the existing workflow; changed parameters conflict. A different ID cannot acquire the single slot while a workflow is active. This identity is retained only within the process and configured terminal retention, so clients must use a new UUID for each new workflow.

```text
preparing -> running -> pulling -> completed
     |          |          |
     +----------+----------+-> stopping -> cancelled
     |          |
     +----------+-> failed
```

Only preparing permits push; at most one push plan is open. Starting a session requires that the complete upload, when required, has committed and no plan remains open. One workflow creates one session. Only natural process exit with code zero permits pulling. Tcl output is diagnostic text: a command error that leaves Vivado running or exits zero does not by itself fail the workflow. The client must encode its own build acceptance criteria in Tcl and the final exit code.

Pulling permits output planning and downloads. Finish succeeds only after active transfers release their leases. It is idempotent after completion. Cancellation stops admission, cancels transfers, completes required process and sync cleanup, then releases the slot. A process still stopping must not free capacity. `cleanup_pending` reports unfinished cleanup separately from the operation's business state.

Heartbeat deadlines use monotonic time and apply throughout preparing, running, and pulling. Wall-clock timestamps are display metadata. Terminal workflows are bounded by age and count; their session output remains bounded as well.

## Project validity and synchronization

The workspace contains server metadata under `.vivado-server`. An exclusive `flock` remains held by service ownership, including background work. Projects have durable clean/dirty markers outside their synchronized trees. A reusable project requires an exact clean marker and a real project directory; absent, unknown, linked, or truncated state never grants reuse.

New or dirty projects require `reset_project:true` and a complete manifest without include/exclude filters. Reset stages a replacement project and requests every file, regardless of existing hashes. Once validated, commit installs that complete tree; paths absent from it are discarded. `entries:[]` deliberately describes an empty project. A valid existing project supports normal incremental planning, with deletion of extra paths controlled by `delete_extra`. Deletions necessary for a file/directory type replacement are part of the replacement operation, including when `delete_extra=false`.

Before modifying a project or launching Vivado, the service persists dirty state. A clean marker is published only after the relevant writers have stopped and data has been synchronized to storage. A failed or forcibly stopped Vivado leaves the project requiring full re-upload.

Staging lives at `<workspace>/.vivado-server/staging/<sync-id>`, outside every project tree. Reset keeps the replaced tree there as rollback material until the operation succeeds. Staging and rollback are process-local transaction support, not a restart journal. Startup discards disposable staging; it does not replay interrupted commits or reconstruct their HTTP results. The trusted client replaces uncertain projects with a complete upload. Operators must never fabricate a clean marker to bypass this boundary.

Push states are `open`, `committing`, `committed`, `aborted`, `expired`, and `failed`; cleanup is a separate Boolean. A committed result remains committed even if disposable staging cleanup needs retry. Settled results are bounded by configured retention and a service-wide count of 128; completed plans release their working buffers. Each workflow also retains at most its 128 newest plan references. `force` is absent: conflicting optimistic baselines require a new workflow or plan as allowed by its phase, never an unchecked overwrite switch.

Uploads stream into random temporary files, enforcing both configured and planned size, idle timeout, total deadline, and SHA-256. Verified bytes replace staging atomically. Failed or cancelled upload retries preserve a previous verified version. Accepted commit/reset work belongs to the manager, so dropping an HTTP waiter does not cancel project mutation or rollback. Rollback is attempted on ordinary in-process failures; failed rollback makes the project unsuitable for incremental reuse.

## Process and output lifecycle

`portable-pty` creates the Linux PTY. The service configures raw terminal mode to avoid canonical line-length truncation, then uses nonblocking file descriptors through Tokio `AsyncFd` for cancellation-aware I/O. Input is written as UTF-8 bytes without kernel echo or newline conversion. Output can still contain terminal control sequences. Vivado always runs in Tcl mode; arguments are bounded and cannot override its mode. A blocked input writer must not prevent stop or shutdown; control characters sent to stdin are not a substitute for the termination API.

Session state distinguishes `running`, `stopping`, `exited`, `terminated`, and `failed`. A terminal state requires confirmed process death and reaping. Successful completion is a client-issued Tcl `exit 0`; explicit termination uses process-group TERM/KILL with bounded deadlines. The outer systemd unit must use `KillMode=control-group`; processes that deliberately create another session are beyond process-group containment.

The output reader incrementally decodes UTF-8 and fills a byte-bounded ring. Cursors identify output chunks, not byte positions. `overrun` means requested history was evicted; `output_truncated` also exposes final drain truncation. Clients must concatenate chunks as a stream, since prompts, lines, and UTF-8 reads need not align with chunk boundaries. Output notification is registered before inspecting the buffer to avoid missed wakeups.

Decoded PTY output can also be copied to `<workspace>/.vivado-server/diagnostics/<session_id>.jsonl`. A bounded queue and a dedicated writer keep archive I/O out of PTY backpressure. JSONL headers and footers identify the session and distinguish process-output truncation from archive loss. The archive limits include record overhead, and retention excludes active sessions. Files survive restart, but workflow/session registries, sync results, and API cursors remain process-local. This best-effort diagnostic store is neither a transaction journal nor a durable client replay API. Archive initialization/write/retention failures are logged and counted without declaring the process successfully cleaned up or blocking PTY reads.

## Observability boundaries

Each runtime owns its metrics registry. `GET /metrics` uses the normal bearer authentication and is omitted entirely when disabled. Metric dimensions use fixed operation/outcome names and route templates; client identifiers, projects, file paths, token values, and raw URLs are never metric labels. Counters restart with the process. See the [observability runbook](observability.md) for exact metric names and deployment examples.

Request spans are INFO and contain the generated request ID, method, and normalized route. Domain spans and events carry workflow, session, and sync identities, including accepted work continuing after an HTTP waiter disappears. Routine business request completions are INFO, client failures/aborts WARN, and server/body failures ERROR; successful health and metrics requests are DEBUG. Internal errors remain available in service logs while the API keeps its existing sanitized error contract. Routine request logs do not include authorization headers, query strings, request bodies, Tcl stdin, or raw PTY text.

HTTP header latency ends when the handler generates the response. Response latency and in-flight ownership extend through the body; dropped or failed bodies are measured separately from complete bodies. Completion and byte counts mean data was handed to the HTTP transport, not acknowledged by the client. The client must still verify download size and hash. Accepted mutation duration has its own domain metric and may continue beyond request cancellation.

`/healthz` checks HTTP liveness. `/readyz` and the readiness gauge check shutdown, workflow/session/sync cleanup health, and unexpected supervised background-task exits. They do not test Vivado startup, licenses, disk capacity, or whether optional logging/archiving collectors keep up. Monitor those failure counters and host storage independently. The stdout logger and PTY writer use separate bounded queues; loss is observable, but neither is a lossless audit channel or an OpenTelemetry exporter.

## Transfers and locking

Workflow metadata locks do not span filesystem, process, or network I/O. Activity leases protect transfers from phase transitions; streamed downloads retain their lease until the response body finishes or is dropped. Finish rejects active leases. Cancellation wakes transfers before waiting for them. The admission gate and manager task registration share shutdown ordering, preventing new accepted mutations after the shutdown barrier.

Sync activity locks coordinate uploads with commit and abort. Project operations serialize mutations without holding locks across a slow HTTP upload. Reapers perform opportunistic cleanup rather than waiting behind client transfers. Shutdown closes admission, wakes long polls and transfers, stops/reaps Vivado, and waits for accepted mutation and cleanup tasks within the configured grace. Exhausting the grace leaves affected projects dirty; it does not certify successful rollback.

A file is opened and hashed using the same handle whose metadata is checked before and after hashing. The download then rewinds that handle and streams it. Content ETags cover SHA-256, not mtime or executable metadata. These checks detect ordinary concurrent changes before response headers; they do not freeze an inode throughout network transfer. Clients must validate complete body size/hash and expected metadata before installing a download.

## Boundary conditions

Project names are validated ASCII segments, at most 64 bytes. Sync paths are UTF-8 relative paths with `/` separators, bounded to 4096 bytes and 255 bytes per segment. Traversal, control characters, ambiguous separators, reserved server metadata, symlinks, and non-regular transfer files are rejected. Linux paths remain case-sensitive. Unix executable state is a Boolean: any ordinary execute bit reads as true, and installation sets or clears all three execute bits. Full modes, ownership, setuid/setgid, links, and directory timestamps are not synchronized.

Path checks use ordinary filesystem APIs and do not eliminate races against another local process with write access. The workspace must belong to the service account. Process groups and file locks similarly do not prevent an authorized Tcl program from escaping the intended workflow; that is why the client and supplied Tcl remain trusted.
