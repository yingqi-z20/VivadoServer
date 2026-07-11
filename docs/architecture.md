# Architecture and invariants

`vivado-server` has two stateful subsystems behind one authenticated Axum
router:

- `SessionManager` owns Vivado PTYs, process lifecycle, output cursors,
  heartbeats, and the active-session semaphore.
- `SyncManager` owns manifest comparison, upload staging, per-project locks,
  optimistic baselines, and commit rollback state.

The health endpoint is intentionally process health only. It does not start
Vivado or scan every project.

## Vivado session lifecycle

Creating a session reserves a semaphore permit before creating the project
directory and spawning the PTY. One blocking task reads PTY bytes, one waits for
process exit, and API calls write stdin through the PTY writer. UTF-8 decoding
retains incomplete trailing byte sequences between OS reads. The exit watcher
briefly waits for the output reader to drain so terminal status does not race
the last output chunk.

Heartbeat timeout and explicit delete use the same termination path: send
`exit`, wait for the child-exit notification, then kill after the grace period.
On Windows the forced path uses `taskkill /T` so the batch wrapper's loader and
Vivado descendants are terminated as one process tree.
Terminal metadata remains queryable until `session_retention_secs`. Normal
server shutdown terminates all live children before the Tokio runtime exits.

On Windows, AMD's `vivado.bat` must prepare the runtime environment. The server
therefore invokes batch entry points through `cmd.exe`, removes the unsupported
verbatim prefix from the ConPTY working directory, and rejects cmd
metacharacters in arguments. `.exe`/`.com` entry points remain direct launches.

## Sync lifecycle

Push is a two-phase operation:

1. Plan scans a filtered server manifest, validates and normalizes the client
   manifest, captures the server baseline, and creates a per-sync staging area.
2. Upload streams each requested file into a random temporary file, verifies
   byte count and SHA-256, then renames it into staging.
3. Commit acquires the project lock, rechecks every affected baseline path,
   moves deletions/replacements into a same-filesystem rollback directory, and
   installs staged files. Any ordinary error reverses applied changes in reverse
   order and restores uploads to staging.

Plans, manifest scans, downloads, and commits share a per-project lock where
they need a consistent server-side view. Uploads do not touch the project tree;
operations within one push session are serialized by its mutex. This prevents
commit/abort from racing an in-flight upload.

`delete_extra=false` protects unrelated extra paths. A conflicting file versus
directory at the exact requested path is still a required type replacement and
is reported in the delete lists. A non-empty directory-to-file replacement
requires `delete_extra=true`; excluded/unplanned directory contents cause a
conflict and rollback rather than silent deletion.

## Path boundary

Project names are one portable path segment. Sync paths are normalized `/`
relative paths. Lexical validation rejects traversal, absolute/drive paths,
Windows device names, control characters, invalid trailing characters, and the
internal staging directory. Before direct access or mutation, the server also
walks existing ancestors with `symlink_metadata`; Windows reparse-point
attributes are checked in addition to ordinary symlinks.

This ancestor check materially limits accidental and adversarial traversal, but
it is not an OS-level `openat` capability. A separate local process with write
access to the workspace can still race filesystem checks. Treat
`workspace_root` as server-owned and do not run multiple server instances over
the same root.

## Failure model

Rollback covers errors returned while the server process remains alive. It is
not a durable transaction journal: power loss or process termination during the
mutation window can leave staging/rollback artifacts or a partially applied
tree. Clients should keep their source manifest, use `force=false`, and re-plan
after any ambiguous connection loss. Generated Vivado outputs should be
reproducible rather than the sole copy of valuable data.

Vivado itself is an external writer and does not participate in the project
lock. Recommended sequencing remains push, run Vivado, then pull. Avoid commit
while Vivado is reading or writing the same paths.

## Verification

Unit and HTTP integration tests cover authentication, error shape and body
limits, PTY output/cursors, heartbeats, path validation, Windows junction
rejection, manifest normalization, type replacement, optimistic conflict,
concurrent commits, abort, and rollback. The Windows launch path has also been
exercised against Vivado 2024.2 through a real PTY by waiting for `Vivado%`,
sending Tcl over stdin, and reading the reported version.
