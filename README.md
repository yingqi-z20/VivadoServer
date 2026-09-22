# VivadoServer

VivadoServer 为受信任的原生客户端提供 Vivado Tcl 会话和文件同步，**仅支持 Linux**。一个 workflow 独占服务的工作槽，从准备源码到 Vivado 退出、下载产物，服务端始终维护业务顺序。协议采用 REST 和输出长轮询；旧会话、项目接口及配置已移除。

Bearer token 持有者可通过 Tcl `exec` 以服务账号权限执行系统命令。请使用专用低权限账号，在内网或 VPN 中部署，通过 TLS 或可信 HTTPS 代理保护传输。服务不提供代码沙箱、多租户隔离或浏览器 CORS。

## 构建和启动

需要 Linux、Rust 1.95 或更新版本，以及 Linux Vivado 安装。Windows 开发机可通过 Linux/WSL 构建和测试，不能直接运行 Windows Vivado。

```sh
cargo build --locked --release
cp config.example.toml config.toml
# 编辑 config.toml，填写 Vivado 路径、已存在的 workspace 和 token 文件。
./target/release/vivado-server --config config.toml
```

相对配置路径以配置文件目录为基准；不带目录的 `vivado_path` 还可从 PATH 查找。workspace 必须已经存在，且路径及祖先不得是符号链接。同一个 workspace 由进程文件锁保证只允许一个服务实例使用。

生产认证使用 `auth_token_files`，每个文件只包含一个 32–4096 字符的 ASCII Bearer token，允许末尾一个 LF 或 CRLF；文件不得向 group/other 开放权限。内联 `auth_tokens` 仅供开发，会产生启动警告。配置和 JSON 请求都拒绝未知字段。

默认监听 `127.0.0.1:8080`，默认拒绝 root 运行。非 loopback 且没有服务内 TLS 时，必须明确设置 `allow_plaintext_non_loopback=true`；仅在可信 VPN 或 HTTPS 代理边界内使用此选项。完整配置见 [config.example.toml](config.example.toml)，部署见 [Linux/systemd 指南](docs/deployment-linux.md)。

## 一次完整工作流

1. 客户端生成 UUID，`PUT /v1/workflows/{id}` 创建 workflow；已有相同 ID 和参数可重试。
2. 新工程或需要重传的工程必须设置 `reset_project:true`，提交完整无过滤 manifest，上传所有计划文件并 commit。已有有效工程可增量 push，或直接启动会话。
3. `POST /v1/workflows/{id}/session` 启动唯一 Vivado Tcl 会话，通过 `/session/stdin` 和 `/session/output` 交互。
4. Vivado 自然退出且退出码为 0 后进入 `pulling`，通过 `/sync/pull/plan` 和条件下载拉取产物。
5. 完成所有下载和校验后 `POST /v1/workflows/{id}/finish` 释放工作槽。

各阶段都应定期调用 `/heartbeat`，默认超时 120 秒。强制终止 Vivado 和异常退出不会作为成功构建；运行被中断的工程需要重新全量上传。服务不会依据 Tcl 输出中的 `ERROR` 文本推断业务成功，客户端应显式检查 Tcl 命令结果并正确设置退出码。

只有一个 workflow 可处于活动状态，冲突返回 `409 workflow_busy`。终态默认保留 3600 秒、最多 128 条记录。已接受的后台修改不因 HTTP 断线而取消；重连后查询原 workflow 和 sync ID，再决定下一步。

## 协议与恢复

- 公开 `GET /healthz`、`GET /readyz`、`GET /openapi.json`；其余接口要求 `Authorization: Bearer <token>`。
- 输出游标有界，支持 `overrun` 和最终输出截断标记；停止期间有明确 `stopping` 状态。
- 上传流式检查大小和 SHA-256，使用临时文件原子替换；下载通过 `If-Match`、ETag 和客户端最终校验确认内容。
- clean/dirty 工程标记决定重启后能否复用。中断或不确定的工程通过可信客户端全量重传恢复，不自动猜测断电时的事务进度。
- Linux 进程组 TERM/KILL、退出回收和 systemd `KillMode=control-group` 共同约束 Vivado 生命周期。

客户端算法和示例见 [中文指南](docs/client-development.zh-CN.md) 或 [English guide](docs/client-development.md)。运行中服务的 `/openapi.json` 描述机器可读接口；并发与持久化边界见 [架构文档](docs/architecture.md)。

## 验证

所有检查均在 Linux 执行：

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --document-private-items
cargo build --locked --release --bins
cargo audit
```

普通测试使用受控假 Vivado；真实测试默认忽略，需要另行准备可用的 Linux Vivado 环境。真实测试的 PID 安全清理使用 pidfd，因此测试机需要 Linux 内核 5.3 或更新版本：

```sh
VIVADO_PATH=/opt/Xilinx/Vivado/2024.2/bin/vivado \
  cargo test --locked --test real_vivado -- --ignored --nocapture --test-threads=1
```

可选环境变量统一为 `VIVADO_TEST_EXPECTED_VERSION`（精确版本匹配）、`VIVADO_TEST_WORKSPACE_ROOT`（已存在且可写的临时目录根）和 `VIVADO_TEST_STARTUP_TIMEOUT_SECS`（默认 120 秒，范围 1–3600）。真实测试覆盖完整 push → Tcl → pull、退出状态、后代进程终止和失败后的重传；普通 CI 不证明某个 Vivado 版本已经实测。
