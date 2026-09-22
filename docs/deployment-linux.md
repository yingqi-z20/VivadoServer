# Linux and systemd deployment

Run one VivadoServer instance per workspace under a dedicated non-root account. The service supports Linux Vivado only. Use a local Linux filesystem that supports `flock`, atomic rename, directory synchronization, and the project durability operations; validate the actual filesystem before deployment.

## 1. Install the service

Build on Linux with Rust 1.95 or newer:

```sh
cargo build --locked --release
sudo useradd --system --create-home --home-dir /var/lib/vivado-server --shell /usr/sbin/nologin vivado-server
sudo install -d -o vivado-server -g vivado-server -m 0700 /srv/vivado-server/workspaces
sudo install -d -o root -g vivado-server -m 0750 /etc/vivado-server
sudo install -d -o root -g root -m 0755 /opt/vivado-server
sudo install -o root -g root -m 0755 target/release/vivado-server /opt/vivado-server/vivado-server
```

All workspace ancestors must be real directories. Do not use symlinked workspace roots or allow unrelated local users to write there. Startup takes an exclusive instance lock under `.vivado-server`; a second service using the same root must fail rather than share project state. This lock does not exclude arbitrary programs running as the service account.

## 2. Authentication and configuration

Generate a token and restrict it to the service account:

```sh
openssl rand -base64 48 | sudo tee /etc/vivado-server/api-token >/dev/null
sudo chown vivado-server:vivado-server /etc/vivado-server/api-token
sudo chmod 0600 /etc/vivado-server/api-token
```

Save `/etc/vivado-server/server.toml` with the actual installation paths:

```toml
listen_addr = "127.0.0.1:8080"
vivado_path = "/opt/Xilinx/Vivado/2024.2/bin/vivado"
workspace_root = "/srv/vivado-server/workspaces"
auth_token_files = ["/etc/vivado-server/api-token"]
allow_plaintext_non_loopback = false
allow_run_as_root = false
heartbeat_timeout_secs = 120
shutdown_grace_secs = 15
```

Tokens contain 32–4096 ASCII Bearer-token characters, with at most one trailing LF or CRLF in a token file. Configuration rejects unknown fields and invalid limits. Inline tokens warn at startup and should be limited to development. See [the example configuration](../config.example.toml) for all defaults, JSON/upload deadlines, retention, and TLS paths.

Keep loopback binding behind a local HTTPS proxy, or configure `[tls]` in VivadoServer. A private address without service TLS requires `allow_plaintext_non_loopback=true` and a verified trusted VPN/proxy boundary. Proxy request size and idle/read timeouts must accommodate configured uploads, long polls, and accepted commits. Do not expose this administrative Tcl execution service directly to the public Internet.

## 3. Vivado environment

Save required environment variables in `/etc/vivado-server/vivado.env`, for example:

```text
XILINXD_LICENSE_FILE=2100@license.internal.example
HOME=/var/lib/vivado-server
LANG=C.UTF-8
```

The service account needs access to the installation, licenses, and board/IP repositories. Test the installation directly using that identity:

```sh
sudo -u vivado-server env XILINXD_LICENSE_FILE=2100@license.internal.example \
  /opt/Xilinx/Vivado/2024.2/bin/vivado -mode tcl
```

Inside Vivado, run `puts [version -short]` and `exit 0`. This checks the account environment; it does not replace validation through the service PTY. Readiness deliberately does not launch Vivado or prove license availability.

## 4. systemd unit

Save `/etc/systemd/system/vivado-server.service`:

```ini
[Unit]
Description=VivadoServer workflow service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=vivado-server
Group=vivado-server
WorkingDirectory=/var/lib/vivado-server
ExecStart=/opt/vivado-server/vivado-server --config /etc/vivado-server/server.toml
EnvironmentFile=-/etc/vivado-server/vivado.env
Restart=on-failure
RestartSec=5s
TimeoutStopSec=20s
KillMode=control-group
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/srv/vivado-server/workspaces /var/lib/vivado-server

[Install]
WantedBy=multi-user.target
```

`KillMode=control-group` provides cleanup for descendants beyond the application's process group. Keep `TimeoutStopSec` above `shutdown_grace_secs`. A process that deliberately detaches can escape process-group cleanup; the service is designed for trusted Tcl, and systemd supplies the outer lifecycle boundary. Site-specific Vivado plugins may need additional paths; add only those observed during deployment testing.

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now vivado-server
sudo systemctl status vivado-server
sudo journalctl -u vivado-server -f
curl --fail http://127.0.0.1:8080/readyz
```

Inspect the JSON readiness status as well as the HTTP result. A healthy coordination layer does not prove a project is reusable or Vivado can start.

## 5. Validate the actual installation

Run the normal gates from [README](../README.md), then the ignored Linux Vivado tests in a disposable workspace:

```sh
mkdir -p /tmp/vivado-validation
VIVADO_PATH=/opt/Xilinx/Vivado/2024.2/bin/vivado \
VIVADO_TEST_EXPECTED_VERSION=2024.2 \
VIVADO_TEST_WORKSPACE_ROOT=/tmp/vivado-validation \
VIVADO_TEST_STARTUP_TIMEOUT_SECS=180 \
  cargo test --locked --test real_vivado -- --ignored --nocapture --test-threads=1
```

The tests require Linux kernel 5.3 or newer for PID-safe emergency cleanup with pidfds. They create their own temporary directories below the supplied root and clean them up. They cover reset/upload, Tcl output and exit, hash-checked pull, finish, forced descendant termination, and shutdown. Set license variables for this process as required. No Linux Vivado test is run by ordinary CI; validate every installation and environment used for deployment.

To repeat under the service identity without installing Rust into its home, build with `cargo test --locked --test real_vivado --no-run`, copy the executable printed by Cargo to a service-readable executable directory, and run that binary as `vivado-server` with the same environment variables and `--ignored --nocapture --test-threads=1`. Ensure its disposable workspace root is writable by that identity.

Finally, start a workflow through the API, then verify stopping the unit leaves no Vivado descendants:

```sh
sudo systemctl stop vivado-server
systemctl status vivado-server
systemd-cgls /system.slice/vivado-server.service
```

## 6. Restart and failed-project handling

Unattended restart is supported through the systemd unit above: its control group must remove the previous Vivado descendants before a replacement server starts. If running the binary directly, stop and verify the old Vivado process group before restarting after an abrupt server death. The workspace file lock excludes another server instance; it does not prove that a former instance's descendants have exited.

A project is reusable only when both its real directory and a durable clean marker exist. New projects, missing/unknown markers, interrupted mutation, and abnormal Vivado execution require a full client upload. A process restart also discards retained workflow and sync IDs; reconnect using a new workflow ID.

Recovery is a client operation: create a workflow with `reset_project:true`, send the complete unfiltered manifest, upload every planned file, and commit. The full replacement discards old project files absent from the manifest. Keep the trusted source independently of the service workspace.

Server-private staging is under `<workspace>/.vivado-server/staging/<sync-id>`. It is disposable and may be cleaned on startup. The service does not replay rollback data after a crash and does not require an operator to reconstruct interrupted renames. Do not manually change a dirty marker to clean. If state paths are malformed or inaccessible, stop the service, investigate permissions and filesystem integrity, and retain any data needed for diagnosis before repair.

Ordinary failures attempt rollback while the process is alive. Rollback failure, abrupt exit, or an exhausted shutdown grace does not guarantee restoration. The clean/dirty boundary prevents incremental reuse of uncertain output; the next trusted complete upload supplies recovery.
