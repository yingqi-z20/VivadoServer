# 日志与可观测性

VivadoServer 提供结构化服务日志、需认证的 Prometheus 指标、进程/工作流生命周期事件和本地 PTY 输出归档。日志用于关联具体操作，指标用于观察趋势和触发告警，归档用于退出或重启后的诊断。外部日志收集、Prometheus、Alertmanager 和 Grafana 需要自行部署；服务不内置可视化界面或 OpenTelemetry 导出器。

## 配置和留存边界

省略 `[observability]` 时使用以下默认值，完整配置见 [config.example.toml](../config.example.toml)：

```toml
[observability]
log_format = "json"                 # 可选 json 或 text
log_filter = "vivado_server=info,tower_http=warn"
metrics_enabled = true
archive_output = true
archive_max_bytes_per_session = 67108864
archive_max_total_bytes = 536870912
archive_retention_secs = 604800
archive_max_sessions = 128
```

可执行服务同时将日志写到 stdout 与 `<workspace_root>/.vivado-server/service-logs/service.log`，默认使用 JSON；`log_format` 同时影响两个目标。文件在 20 MiB 或 UTC 日期变化时轮转，最多保留 5 份（当前文件和 `.1` 到 `.4`），按日志分段日期清理达到 14 天的文件；数量/大小限制可能更早淘汰。启动时执行清理，后续写入时每分钟至多检查一次；完全静默时在下一次启动或写入时清理。目录权限 0700、文件权限 0600。路径逐级拒绝符号链接，文件拒绝硬链接和非普通文件；独立文件锁阻止两个服务同时轮转。单条记录超过 20 MiB 时仅送 stdout，文件写入故障会在 stderr 提示且不阻断 stdout。`vivado_server_log_file_write_failures` 单独统计文件写入、刷新或保留检查失败。库入口 `initialize_logging` 保持仅 stdout 的兼容行为，`initialize_logging_with_root` 启用双写。

stdout 的保存、轮转和转发仍由 systemd/journald 或部署环境负责。服务日志队列最多 4096 条消息；下游阻塞时丢弃新日志并累计 `vivado_server_log_dropped_messages`，避免阻塞业务。正常关闭会尝试排空队列，但进程强杀、崩溃或收集器长期阻塞不能保证日志完整。服务文件日志用于运维诊断，不是可信用户操作审计。初始化日志之前的配置错误和 Rust 原有 panic/backtrace 输出可能是普通 stderr 文本。

`RUST_LOG` 覆盖 `log_filter`，无效或空过滤规则使日志初始化失败。临时调试可在启动进程前设置：

```sh
RUST_LOG=vivado_server=debug,tower_http=debug \
  ./target/release/vivado-server --config config.toml
```

systemd 部署可将 `RUST_LOG=vivado_server=debug,tower_http=debug` 写入已有的 `/etc/vivado-server/vivado.env`，在没有活动工作流的维护窗口重启服务；完成排障后删去该项并重启。配置不支持在线热更新。更细的同步逐文件 TRACE 日志量较大，按模块临时开启，并观察日志丢弃指标。

业务记录、内存输出、PTY 归档有不同生命周期。workflow 默认保留一小时、最多 128 条；实时输出环形缓冲默认 1 MiB；两者随进程重启消失。PTY 文件默认保留七天、每会话 64 MiB、总计 512 MiB、最多 128 个会话，并在重启后继续按保留策略清理。这些限制不能替代工作区、journald 和主机磁盘配额。

## 从一次失败定位到后台工作

客户端应记录错误响应中的 `request_id` 或响应头 `x-request-id`，以及关联的 `workflow_id`、`session_id`、`sync_id`。服务使用自己生成的请求 ID；不要依赖客户端传入的 `x-request-id`。JSON 日志通常包含 `timestamp`、`level`、`target`、`fields`、`span` 和 `spans`；关联 ID 可能出现在事件字段或任一父 span 中。

以下查询兼容事件字段和父 span，并忽略 journald 中的非 JSON 行：

```sh
REQUEST_ID=8e47a0ac-c6f5-4af0-a491-f9e7ce98efaf
journalctl -u vivado-server --since '1 hour ago' -o cat --no-pager |
  jq -Rc --arg key request_id --arg id "$REQUEST_ID" \
    'fromjson? | select(any(.. | objects; .[$key]? == $id))'
```

将 `--arg key request_id` 改为 `workflow_id`、`session_id` 或 `sync_id`，并替换 ID，即可沿业务操作继续查询。先用请求 ID 查 HTTP 错误和业务身份，再按 workflow 查看状态变化，按 session 查看 Vivado 启动/退出/清理，按 sync 查看计划、上传、commit 和清理。已接受的后台操作保留关联上下文，HTTP 中断后仍可能成功；必须结合状态 API 判断能否重试。

查看最近 HTTP 请求完成事件：

```sh
journalctl -u vivado-server --since '15 minutes ago' -o cat --no-pager |
  jq -Rc 'fromjson? | select(.fields.event == "http_response_finished") |
    {timestamp, level, request: .fields}'
```

`http_response_finished` 包含 `request_id`、`method`、`route`、`status`、`outcome`、`duration_ms`、`response_bytes`、`error_code` 和诊断 `error`。`route` 是模板，例如 `/v1/workflows/{workflow_id}/session/output`；未匹配路径为 `unmatched`。`status` 是字符串，生成响应头前取消为 `none`。常规业务成功记录 INFO，4xx/中断记录 WARN，5xx/响应体读取失败记录 ERROR；健康探针和 metrics 成功记录 DEBUG，避免周期抓取刷屏。响应头生成的 `http_response_headers` 事件在 DEBUG。

`outcome=complete` 表示响应体已经交给 HTTP 传输层；`body_error` 表示服务端读取响应体失败；`aborted` 表示处理器或未完成的响应体被丢弃。`complete` 不证明客户端收到、持久化或校验了文件。请求取消也不证明已接受的 commit 被撤销。

工作流记录创建、阶段变化、终态、心跳过期以及失败的原始原因；会话记录启动、PID、停止原因、退出码、输出字节和截断；同步记录计划和提交结果、传输以及清理恢复。正常收尾与失败后的重试分开记录，持续相同清理错误会限频汇总。成功业务状态以 API 和进程退出结果为准，不能只看日志里是否出现文本 `ERROR`。

常规请求日志不记录 Authorization、原始 URL/查询参数、请求正文、Tcl stdin 或 PTY 正文。服务端内部错误保留原始诊断，可能包括文件路径；PTY 本身也可能打印敏感信息。因此服务日志与归档都应按工作区数据保护，不能视为自动脱敏后的公开数据。

## 指标和查询

`GET /metrics` 默认启用，使用普通 Bearer 认证，返回 Prometheus text exposition。关闭 `metrics_enabled` 后路由不存在，带或不带 token 均返回 404。抓取凭据没有只读权限隔离：它与其他 API token 一样允许执行 Tcl，应由受信任的监控服务持有。

| 指标 | 含义和主要标签 |
|---|---|
| `vivado_server_http_requests_total` | 已生成响应头的请求，`method, route, status` |
| `vivado_server_http_request_duration_seconds` | 请求进入至生成响应头的直方图，`method, route, status` |
| `vivado_server_http_responses_total` | 响应生命周期结束次数，增加 `outcome=complete/body_error/aborted` |
| `vivado_server_http_response_duration_seconds` | 至正文结束、失败或丢弃的耗时直方图，包含处理器耗时和 `outcome` |
| `vivado_server_http_response_bytes_total` | 已交给 HTTP 传输层的正文数据字节，`method, route, status` |
| `vivado_server_http_aborted_requests_total` | 中断请求，`method, route, phase=handler/body` |
| `vivado_server_http_in_flight_requests` | 处理器执行或正文仍未结束的请求，`method, route` |
| `vivado_server_operations_total` | 已完成的业务操作，`kind, outcome` |
| `vivado_server_operation_duration_seconds` | 对应业务操作耗时直方图，`kind, outcome` |
| `vivado_server_active` | 当前资源/操作数，`kind`，包括 `workflow`、`session`、`sync_session` 和 `sync_cleanup_pending` |
| `vivado_server_bytes_total` | 业务字节累计，`kind`，例如 `session_output`、`sync_upload`、`sync_download`、`archive_output` |
| `vivado_server_events_total` | 生命周期、丢失和清理等事件，`kind, outcome` |
| `vivado_server_background_task_failures_total` | 受监督任务意外结束次数，`task` |
| `vivado_server_ready` | 当前就绪状态，1 为就绪、0 为未就绪；抓取时更新 |
| `vivado_server_log_dropped_messages` | 有界服务日志队列自进程启动以来的丢弃数；是 gauge，抓取时更新 |
| `vivado_server_log_file_write_failures` | 服务文件写入、刷新或保留检查失败次数；是 gauge，抓取时更新；stdout 可继续输出 |
| `vivado_server_uptime_seconds` / `vivado_server_start_time_seconds` | 运行时存活秒数 / 初始化的 Unix 时间 |
| `vivado_server_build_info` | 构建版本，`version`，值为 1 |

直方图暴露 `_bucket`、`_count`、`_sum`。所有累积值在进程重启后归零，尚未发生的标签组合可能完全不存在。UUID、project、路径、请求 ID 均不进入指标标签。日志保留这些身份以定位个例，Prometheus 标签只使用固定方法、路由和操作/结果名称。

`kind=workflow` 的业务结果为 `completed/cancelled/failed`；`workflow_phase_preparing/running/pulling/stopping` 记录阶段耗时及离开该阶段时的下一状态。`kind=session` 的结果是终止原因，例如 `process_exited`，不是构建成功/失败：自然退出可能是非零退出码，必须结合 workflow 结果和日志。同步 `kind` 包括 `sync_manifest`、`sync_plan`、`sync_reset_plan`、`sync_upload`、`sync_commit`、`sync_pull_plan`、`sync_download` 和 `sync_cleanup`；一般结果为 `success/error/cancelled`。上传字节计数在成功验证后增加，下载字节计数随正文交给 HTTP 层增加，所以二者口径不同。

例如，按实例查看完整 workflow 的 P95 耗时：

```promql
histogram_quantile(0.95,
  sum by (job, instance, le) (
    rate(vivado_server_operation_duration_seconds_bucket{kind="workflow",outcome="completed"}[1h])
  )
)
```

查看下载交付速率和正文失败：

```promql
sum by (job, instance) (rate(vivado_server_bytes_total{kind="sync_download"}[5m]))
sum by (job, instance) (increase(vivado_server_http_responses_total{outcome="body_error"}[5m]))
```

使用直方图分位数时保留 `le` 聚合维度；低流量下 P95 很不稳定，应同时看样本数和原始事件。[Prometheus 直方图说明](https://prometheus.io/docs/practices/histograms/)

## Prometheus 接入和告警

仓库的 [prometheus.yml](../deploy/observability/prometheus.yml) 默认抓取同机 `127.0.0.1:8080`，每 15 秒一次，通过 `authorization.credentials_file` 读取 `/etc/prometheus/secrets/vivado-server-token`。该文件只存 token，不能带 `Bearer ` 前缀；为 Prometheus 身份设置受限权限。VivadoServer 仍从其自己的、符合权限要求的 `auth_token_files` 读取同一 token。远程抓取应改为 HTTPS、配置正确 CA 和服务地址；容器中的 loopback 指向容器自身。[Prometheus 抓取认证配置](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#http_config)

[alerts.yml](../deploy/observability/alerts.yml) 提供以下初始规则，阈值需按实际构建耗时和流量调整：

- 抓取失败、持续未就绪，以及本次进程中出现后台任务失败。
- 5 分钟至少 20 次普通请求且 5xx 超过 5%；排除探针、metrics、输出长轮询和文件传输。
- workflow 状态查询和 heartbeat 的响应头 P95 超过 5 秒；排除长轮询、文件传输和耗时的计划/提交/启动操作，并要求足够样本。
- 正文读取失败、清理失败/积压、PTY 归档损失，以及服务日志队列丢弃。

归档丢失、后台失败和日志丢弃采用“自本次启动以来发生过”的条件，以便发现第一次抓取前的启动失败。这类告警会保持到进程重启；先确认和修复原因，再由运维处置或在 Alertmanager 暂时静默，不能为了清零告警中断活动 workflow。清理失败采用最近时间窗口，结合 pending gauge 和就绪状态观察恢复。

将配置和规则放在同一目录，先验证后加载；按环境另配 Alertmanager 接收器，当前示例不会主动发送通知：

```sh
promtool check config /etc/prometheus/prometheus.yml
promtool check rules /etc/prometheus/alerts.yml
# 在仓库根目录验证随附规则测试
promtool test rules deploy/observability/alerts.test.yml
```

这些命令检查语法和固定样例，不证明生产抓取、通知路由或告警送达。接入后检查 Prometheus targets、规则加载和实际触发/恢复，另为主机磁盘、文件描述符、内存以及外部许可证建立监控。[Prometheus 规则检查](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/#syntax-checking-rules)

## PTY 归档的读取与完整性

文件固定在 `<workspace>/.vivado-server/diagnostics/<session_id>.jsonl`。目录权限为 0700，新文件权限为 0600。归档记录 UTF-8 解码后的 PTY 输出，不是原始二进制流，也不单独记录 stdin。工具自己打印的命令、路径或凭据仍会进入归档。

每个文件以 `event:"started"` 开始，包含 `format:"vivado-server-pty-v1"`、session ID、project 和启动时间。正文记录是 `event:"output"`、`timestamp` 和 `text`；时间戳是归档 writer 写入时间，可能晚于实际输出产生时间。正常收尾追加 `event:"finished"`，包含退出状态和完整性字段：

| 字段 | 解释 |
|---|---|
| `exit_code`, `termination_reason` | Vivado 的进程结果，不能从归档完整性反推构建结果 |
| `output_truncated` | PTY 输出存在截断，例如最终排空超时，与归档存储损失独立 |
| `archive_truncated`, `archive_issues` | 归档是否发生丢失或未确认完整，以及原因列表 |
| `observed_bytes` | 归档入口收到的解码输出字节数 |
| `archived_output_bytes` | 成功写入的 text 字节数，不含 JSON 开销 |
| `dropped_bytes` | 未归档的输出字节数；应结合 truncation 和 issues 判断 |

只读检查指定会话；`jq -j` 保留原有换行，避免人为在 chunk 之间插入换行：

```sh
SESSION_ID=00000000-0000-4000-8000-000000000001
ARCHIVE="/srv/vivado-server/workspaces/.vivado-server/diagnostics/$SESSION_ID.jsonl"
sudo -u vivado-server jq -c 'select(.event == "started" or .event == "finished")' "$ARCHIVE"
sudo -u vivado-server jq -j 'select(.event == "output") | .text' "$ARCHIVE"
```

缺少 `finished` 记录表示会话仍在写入或归档被中断，不能把现有文件当成完整结果；进程已结束时应检查服务日志和指标。损坏的末行、`archive_truncated=true`、`output_truncated=true` 或归档失败事件都需要保留上下文。内存输出 `overrun` 只说明实时缓冲历史被淘汰，不能直接判断归档是否完整；归档也不会修复 API 的过期 cursor。

归档 writer 使用有界异步队列，出现以下原因会留下指标和日志，且不会使 Vivado 输出读取等待磁盘：

| 原因 | 排查方向 |
|---|---|
| `queue_full` | writer 跟不上突发输出或磁盘变慢；该会话停止接收后续归档输出 |
| `session_limit` | 单会话额度耗尽，已含 JSON 记录开销和收尾预留空间 |
| `total_limit` | 清理可淘汰文件后总容量/数量仍不足；活动归档不会被删 |
| `disk_error` | 权限、磁盘空间、文件系统或写入错误 |
| `worker_stopped` / `worker_panicked` | writer 提前退出，查看原始错误 |
| `drain_timeout` / `shutdown_timeout` | 会话等待收尾 500 ms / 服务等待 writer 2 秒超时，不能保证最终落盘完整 |
| `initialization_failed` / `retention_failed` | 归档初始化被禁用 / 自动保留清理失败；服务仍可处理工作流 |

单会话达到额度后停止追加正文，不轮转成无限文件。保留清理在初始化、新建会话、需要腾出总量空间时及后台周期执行（约每 60 秒）；按文件修改时间回收过期或最旧的非活动归档。只删除带正确格式头、匹配 UUID 文件名的普通单链接文件；陌生文件、符号链接及活动归档不参与自动删除。额度只覆盖识别出的归档，因此仍需监控真实磁盘使用。

设置 `archive_output=false` 并重启可停止创建新归档，既有文件不会自动删除，禁用期间也不会执行归档保留清理。需要释放空间或删除敏感留存时，先在维护窗口停止服务，确认诊断目录的真实路径和文件归属，再按明确会话 ID 保存或删除所需文件；不要清理项目 clean/dirty 标记或把归档当作事务恢复数据。

## 健康检查与故障处理

`/healthz` 返回 200 只说明 HTTP 服务有响应。`/readyz` 正常为 200 和 `{"status":"ready"}`；服务关闭、workflow/session/sync 清理退化或受监督后台任务意外结束时返回 503、`status:"degraded"` 和 `reasons`，后台失败还返回 `failed_tasks`。就绪状态变化记录 `event=service.readiness_changed` 和布尔字段 `ready`，相同状态不重复记录。就绪检查不运行 Vivado，不验证许可证、磁盘空间或可选归档 writer。

出现 `workflow_cleanup_failed` 时先按 workflow/session 日志查 PID、终止原因、清理重试和恢复事件，结合状态 API 查看 `cleanup_pending`；在确认进程退出前不要手动释放工作槽。出现 `background_task_failed` 时保留任务名称、panic/错误和关联 ID，按 [部署指南](deployment-linux.md) 的进程清理边界处理后再恢复服务。仅归档失败不会自动使 readiness 失败，应通过独立归档告警发现。

许可证或 Vivado 启动问题仍需在服务身份下检查安装环境和许可证，并运行实际工作流验证。不要把监控端点可用等同于编译成功，也不要因日志/归档缺失就盲目重放 Tcl stdin。

## Private durable history journal

The optional `[history]` configuration enables a journal independent of the
live PTY ring and of clients calling `/session/output`. Platform deployments set
`enabled = true`; standalone deployments default to false. Enabling history
replaces the older diagnostic PTY archive to avoid keeping duplicate output.

```toml
[history]
enabled = true
max_bytes = 536870912
retention_secs = 604800
session_head_bytes = 8388608
session_tail_bytes = 58720256
```

The journal records workflow and session lifecycles, parsed stdin intent and
actual write completion, decoded PTY output, sync operations and upload metadata.
It does not copy source-file contents, authentication headers or bearer tokens.
Stdin and output are intentionally sensitive user history: the platform must
keep these out of ordinary service logs and audit administrative access. Output
has no command request ID because Vivado can emit asynchronous messages; byte
offsets and session IDs identify its ordered stream. Stdin chunks and completion
share an `input_id` and the original request correlation IDs.

`GET /internal/history/v1/events?after=0&limit=256` requires the normal internal
core bearer token. This endpoint must **not** be exposed through the public
native `/v1` gateway. Its page contains `journal_id`, current `instance_id`,
`events`, `next_cursor`, `head_cursor`, `earliest_cursor`, `has_more`, `gaps` and
`recording_state`. Resume from `next_cursor`, the last delivered/scanned sequence;
`head_cursor` is the committed global high sequence. `earliest_cursor` is one
less than the oldest retained sequence. Gaps explicitly report unavailable
ranges, and can overlap retained events when old gap summaries are coalesced.
Never remove an otherwise present event just because it overlaps a gap summary.

Each event has `seq`, its own `instance_id`, RFC3339 `timestamp`, `event`, optional
workflow/session/project/request IDs and `data`. Journal IDs and sequences
survive core restart; instance IDs do not. Restart interruption events retain
the old instance ID and mark unsealed work `unknown`, without claiming that the
Vivado command completed. A newly accepted `workflow.created` event's sequence
identifies a new attempt even when a native client reuses a workflow UUID. A
valid UUID `x-platform-request-id` is copied into `data.parent_request_id` for
correlation only; it never grants authorization.

Output and stdin text are split at UTF-8 boundaries into at most 4096-byte chunks.
Events are at most 32 KiB including JSON escaping, and pages are at most 2 MiB.
The queue is bounded to 8 MiB; ordinary output is batched and fsynced independently
of the live ring. Per session the first 8 MiB and last 56 MiB are retained by
default. Total retained JSON is capped at 512 MiB, seven days and 100,000 events;
whichever limit is reached first applies. Queue pressure, disk errors and
retention produce explicit gap metadata. New work and stdin fail with the
existing `429 capacity_reached` envelope when critical recording cannot be
confirmed. Stop, cancel and heartbeat remain available. Large permitted stdin
is journaled in bounded batches before any of its bytes reach the PTY.

The fixed spool is `.vivado-server/history-v1/{state.json,events.jsonl}` below
the workspace root. Directory/file modes are 0700/0600. The writer pins the
directory descriptor and rejects symlinks, devices and hard-linked files. The
atomic `state.json` (at most 64 KiB) contains the same IDs plus internal
`next_cursor` (the global head), `earliest_cursor`, `session_head_bytes`,
`session_windows`, `gaps` and open lifecycle recovery metadata. `events.jsonl`
is at most 544 MiB including deferred garbage; compaction occurs at bounded
thresholds, rather than rewriting the spool after every event. An offline
reader must omit sequences at or below `earliest_cursor`, and omit output
chunks with `session_head_bytes <= byte_offset < session_windows[session_id]`.
The stopped-container operator uses exactly this format and does not start the
container to export it. A complete line beyond the last persisted state cursor
can be replayed after a crash; an incomplete trailing line is not an event.

This local spool belongs to the same Unix user as Vivado and is a bounded
recovery buffer, not tamper-proof evidence. The manager's archive outside the
container supplies longer retention (180 days), trusted account association,
access control and access auditing. If the manager is unavailable beyond local
retention, missing content is explicitly partial; it cannot be reconstructed
from the live ring or inferred from HTTP success alone. `request.completed`
means response headers were produced, while sync download completion records
actual body consumption by HTTP, not remote receipt or client fsync.
