# VivadoServer 客户端开发文档

本文定义 Linux workflow API，面向受信任的原生桌面或 CLI 客户端。运行中的 `/openapi.json` 提供机器可读契约。旧 `/v1/sessions`、`/v1/projects` 接口已移除；所有 JSON 请求对象拒绝未知字段。

## 1. 获取工作流

客户端先生成一个新 UUID，再发送 `PUT /v1/workflows/{workflow_id}`。本地保存该 ID，避免响应丢失后无法查询结果。所有受保护请求均需要 `Authorization: Bearer <token>`。

<!-- contract:CreateWorkflowRequest -->
```json
{
  "project": "demo",
  "reset_project": true
}
```

服务只有一个全局工作槽。其他 workflow 正在活动时返回 `409 workflow_busy`。使用相同 ID 和创建参数重复 PUT 会返回已有记录；参数不同则冲突。这种幂等身份仅在服务进程及保留期内有效，每次新任务都应生成新 ID。

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

工程没有有效 clean 标记时，必须设置 `reset_project:true`，否则返回 `409 project_reupload_required`。reset 会在提交时用完整客户端 manifest 替换整个服务器工程；manifest 中没有的文件会被移除。已有有效工程可使用 `reset_project:false`，按需增量 push 后运行，或直接启动 Vivado。

准备、执行和拉取的全部阶段都要调用 `POST /v1/workflows/{workflow_id}/heartbeat`。默认超时 120 秒，建议客户端每 30 秒发送一次。传输活动和输出轮询不能替代显式心跳。终态默认保留 3600 秒，最多 128 个 workflow。

| 工作流状态 | 可执行的后续操作 |
|---|---|
| `preparing` | 读取 manifest、push，完成必要上传后启动唯一会话 |
| `running` | Tcl stdin/output 和心跳 |
| `pulling` | manifest、pull plan/download，最后 finish |
| `stopping` | 等待清理，工作槽仍被占用 |
| `completed` / `cancelled` / `failed` | 查询保留结果，新任务使用新 workflow |

通过 `GET /v1/workflows/{workflow_id}` 观察状态。`DELETE` 取消 workflow 并清理已接受的操作，在终态可幂等调用。`POST .../finish` 仅完成 pulling；存在活动传输时返回 409，已完成时可重试。确认 workflow 终态后再停止心跳。

## 2. 路径与 manifest

工程名是一个非空 ASCII 段，可含字母、数字、`_`、`-` 和 `.`，最长 64 字节；`.`、`..` 和内部元数据名 `.vivado-server` 保留。Linux 名称大小写敏感。

同步路径是 UTF-8 相对路径，使用 `/` 分隔，最长 4096 字节，每段最长 255 字节。拒绝空段、`.`/`..`、绝对路径、控制字符、`: < > " | ? *`、反斜线和保留的 `.vivado-server` 段。不支持符号链接和非普通文件。URL 逐段编码，保留段之间的斜线。

文件条目需要 `size_bytes`、`mtime_unix_ms` 和 64 位十六进制 SHA-256。下方 push 示例描述六个字节 `hello\n`。目录条目使用 `{"path":"rtl","kind":"dir"}`，不同步目录时间；缺失的父目录会自动补全。客户端应通过同一文件句柄计算哈希并比较读取前后元数据，文件变化时重新扫描。

文件相等比较大小、毫秒 mtime、SHA-256 和 `executable`。Linux 中任意普通执行位存在即为 true；安装时 true 设置全部 `0111` 位，false 清除全部执行位。不保留精确权限掩码、所有权、特殊权限位或链接。

在 preparing 或 pulling 中，可向 `POST .../sync/manifest` 发送以下请求读取服务器视图：

<!-- contract:ManifestRequest -->
```json
{
  "include_globs": [],
  "exclude_globs": []
}
```

glob 列表最多 256 项，每项最多 4096 字节。include 为空表示全部包含，exclude 优先。reset push 的两个列表必须为空，确保 manifest 描述完整替换工程。

## 3. Push 与提交查询

发送 `POST /v1/workflows/{workflow_id}/sync/push/plan`：

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

push 和 pull 都必须提供 `entries`；显式空数组合法，在 reset 中表示有意安装空工程。响应包含 `sync_id`、`upload_files`、`create_dirs`、`delete_files`、`delete_dirs` 和 `expires_at`。同时只能开放一个计划，先提交或取消该计划，再创建下一个计划或 Vivado 会话。

reset 要求上传 manifest 的全部文件，即使旧工程中有相同内容，也必须重新上传。它替换整棵工程树，不受 `delete_extra` 控制。普通增量 push 的 `delete_extra` 只控制无关额外项的删除；文件/目录类型替换所必需的删除仍属于替换操作。替换非空目录时必须允许删除其内容。

对每个返回的 upload 流式发送精确原始字节：

```http
PUT /v1/workflows/{workflow_id}/sync/{sync_id}/files/{path}
Authorization: Bearer <token>
Content-Type: application/octet-stream
Content-Length: <计划大小>
```

已知长度超限会在读取正文前拒绝，chunked 上传超过计划即停止。正文大小及哈希必须匹配，并受到空闲和总时限约束。失败的重试保留之前验证成功的 staged 文件；计划仍开放时可用相同正文重试。

向 `POST .../sync/{sync_id}/commit` 发送严格空对象：

<!-- contract:CommitSyncRequest -->
```json
{}
```

不提供 `force` 参数。已接受的提交在 HTTP 断线后继续。创建替代任务前，先查询 `GET .../sync/{sync_id}`，或在 workflow 阶段允许时重试 commit。

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

同步状态为 `open`、`committing`、`committed`、`aborted`、`expired` 或 `failed`。`cleanup_pending` 独立于业务结果：committed 且仍待清理表示项目修改已成功。失败提供 `error_code`、`error_message`。`DELETE .../sync/{sync_id}` 取消开放计划，不能打断正在执行的 commit。结果受同步保留期限制。

workflow 失败或返回 `project_reupload_required` 时，保留可信本地源码，用新 UUID 创建 reset workflow，再上传完整 manifest。服务重启后不保留旧 workflow/sync ID；新建时根据响应决定是否必须 reset。

## 4. 运行并结束 Vivado

准备完成后发送 `POST /v1/workflows/{workflow_id}/session`：

<!-- contract:StartSessionRequest -->
```json
{
  "args": [
    "-nolog",
    "-nojournal"
  ]
}
```

服务端强制 `-mode tcl`，不得传入 `-mode`、`-gui`、`-batch` 或 `-tcl`。参数最多 128 项、总计 32 KiB，不能含 NUL。工程由 workflow 指定，会话请求不包含 project。每个 workflow 最多创建一个会话。

向 `POST .../session/stdin` 发送 Tcl：

<!-- contract:SendInputRequest -->
```json
{
  "text": "puts [version -short]\n"
}
```

输入受到 `stdin_max_bytes` 限制，通过 raw PTY 按 UTF-8 字节写入，没有内核回显或 CRLF 转换。终止会话应调用 API，不能假定 stdin 控制字符会触发信号；输出仍可能包含终端控制序列。使用 `GET .../session/output?cursor=0&timeout_ms=30000` 轮询，每次用响应 cursor 发起下次请求：

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

把 chunk text 作为字节解码后的文本流拼接，chunk 边界不代表行或 prompt 边界。`overrun` 表示请求的旧输出被淘汰；`output_truncated` 表示发生输出截断，包括最终排空超时。客户端应显示这两种情况。长轮询默认 30 秒，最大 60 秒，`timeout_ms=0` 立即返回。没有新输出的轮询仍是正常响应，不代表命令失败。服务关闭会取消普通等待。

`GET .../session` 返回 `running`、`stopping`、`exited`、`terminated` 或 `failed`，以及退出码和终止原因。stopping 不表示进程已经死亡。`DELETE .../session` 是显式终止操作，会使未完成 workflow 失败，不能把它当作成功退出。

应在检查构建结果后发送 Tcl `exit 0`。只有自然退出码为零且进程清理已确认，workflow 才进入 pulling。非零退出、强制停止、输出/输入失败或运行中断都会使工程需要全量重传。Tcl 输出含 `ERROR` 本身不决定成功；用 Tcl `catch` 或命令特定状态检查确认构建结果，不满足业务条件时以非零退出码结束。

## 5. Pull 与释放工作槽

只有 pulling 接受 `POST /v1/workflows/{workflow_id}/sync/pull/plan`：

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

对每个 `download_files` 条目携带内容摘要下载：

```http
GET /v1/workflows/{workflow_id}/sync/files/{path}
Authorization: Bearer <token>
If-Match: "sha256:5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
```

响应包含 `ETag`、`x-sync-size-bytes`、`x-sync-mtime-unix-ms`、`x-sync-sha256` 和 `x-sync-executable`。内容哈希过期返回 412；仅修改元数据不会改变 ETag。响应前哈希阶段检测到文件变化返回 409，已经开始的正文无法事后改为 JSON 错误。

每个正文先流式写入本地临时文件。校验完整大小和哈希，同时比较响应元数据与计划，关闭文件后原子安装，再设置 mtime/执行位。遇到不匹配或中断时丢弃临时文件并重新 plan。类型替换所需的删除必须在安装替代项前执行。`delete_extra` 只控制无关额外项，不能用它跳过计划中的类型替换删除。目录按由深到浅的顺序删除。

全部响应体结束、本地结果验证通过后调用 `POST .../finish`。若因活动传输返回 409，先读完或关闭对应正文，再重试。不要在下载正文仍被消费时释放 workflow。

## 6. 错误与信任边界

`GET /healthz`、`GET /readyz`、`GET /openapi.json` 不认证。就绪检查表示服务协调/清理状态，不启动 Vivado，也不证明许可证可用。应用响应都有 `x-request-id`；401 还包含 `WWW-Authenticate: Bearer`。

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

`details` 始终是对象；上报问题时记录 request ID。通过 code 区分 workflow 阶段冲突、工程重传和同步冲突，不要解析供人阅读的 message 决定业务分支。

| HTTP 状态 | 含义 |
|---|---|
| 400 / 401 / 404 | 输入非法 / 认证失败 / 资源不存在或已过期 |
| 408 / 413 | 请求正文超时 / 正文或文件大小超限 |
| 409 | 工作槽占用、阶段错误、同步冲突或需要全量重传 |
| 412 / 429 | 内容前置条件过期 / 有界请求容量耗尽 |
| 500 / 503 | 内部错误 / 服务关闭或不可用 |

HTTP 取消不会撤销已接受的状态变更，重连后查询权威状态。不要盲目重放 Tcl stdin：命令可能已经执行，stdin 接口不提供幂等命令队列。

Bearer 权限允许通过 Tcl `exec` 以服务账号身份执行系统命令。使用专用账号、可信源码及内网/VPN，并由 TLS 或可信 HTTPS 边界保护传输。服务不提供浏览器 CORS 或代码执行沙箱。

同步结果最多保留 `sync_result_retention_secs`，全服务同时最多保留 128 个已完成结果，因此旧结果可能在时间上限之前返回 404。每个工作流最多保留最近 128 个计划的引用。客户端应自行保存需要留存的结果，不将服务端查询接口作为审计归档。
