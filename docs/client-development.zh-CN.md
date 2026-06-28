# Vivado Server 客户端开发文档

本文面向客户端开发人员，说明如何通过 HTTP API 与 `vivado-server`
交互，包括：

- 文件级同步：客户端工程推送到服务器、服务器产物拉回客户端。
- 远程 Vivado 会话：启动 Vivado、发送 Tcl/CLI 输入、轮询输出、保活和终止。

服务端只提供 HTTP API，不提供客户端 CLI。客户端需要自行实现本地文件扫描、
manifest 生成、上传、下载、校验和本地删除。

## 1. 基础约定

### 1.1 Base URL

示例使用：

```text
http://127.0.0.1:8080
```

生产环境可以：

- 直接启用服务端 TLS。
- 或使用 Nginx/Caddy/Traefik 等反向代理终止 HTTPS。

### 1.2 认证

除 `GET /healthz` 外，所有 `/v1/*` 接口都需要 Bearer Token：

```http
Authorization: Bearer <token>
```

未认证或 token 错误时返回：

```http
401 Unauthorized
Content-Type: application/json

{"error":"unauthorized"}
```

### 1.3 请求体格式

- JSON 接口使用 `Content-Type: application/json`。
- 文件上传接口直接发送原始字节流，不要 base64。
- 文件下载接口直接返回原始字节流，校验信息放在响应头中。

### 1.4 错误格式

所有错误响应都是 JSON：

```json
{
  "error": "human-readable message"
}
```

常见状态码：

| 状态码 | 含义 |
|---:|---|
| 400 | 请求格式错误、路径非法、manifest 非法、上传大小或 SHA-256 不匹配 |
| 401 | 未认证或 token 无效 |
| 404 | session、sync session 或文件不存在 |
| 409 | 冲突，例如 commit 前服务器文件被改动、缺少上传文件、会话数量超限 |
| 500 | 服务端内部错误 |

## 2. 项目名和路径规则

### 2.1 Project 名称

`project` 出现在以下 URL 中：

```text
/v1/projects/{project}/...
```

以及创建 Vivado session 时的 JSON body 中：

```json
{
  "project": "demo"
}
```

合法 project 名称：

- 非空。
- 不能是 `.` 或 `..`。
- 只允许 ASCII 字母、数字、`_`、`-`、`.`。

合法示例：

```text
demo
demo-1
board_a.xpr
```

非法示例：

```text
../demo
demo/sub
demo:1
项目
```

服务端会把合法 project 映射到：

```text
workspace_root/<project>
```

### 2.2 同步文件路径

同步路径必须是 project 内的 UTF-8 相对路径，并统一使用 `/` 分隔。

合法示例：

```text
src/top.tcl
rtl/core.v
constraints/top.xdc
out/result.txt
```

非法示例：

```text
                         # 空路径
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

规则：

- Windows 客户端也必须把 `\` 转成 `/`。
- 不能发送绝对路径。
- 不能包含 `.` 或 `..` 段。
- 不能包含 Windows drive prefix，例如 `C:`。
- 不能访问 `.vivado-server-sync/`，这是服务端内部 staging 目录。
- URL 中的 `{path}` 是通配路径。普通 Vivado 工程路径通常可以直接拼到
  `/files/` 后面；如果路径段包含特殊字符，客户端应按 URL path segment
  做 percent-encoding。

## 3. Manifest 数据模型

客户端需要扫描本地目录并生成 manifest。服务端也会扫描服务器 project
目录生成 manifest。

### 3.1 File Entry

```json
{
  "path": "src/top.tcl",
  "kind": "file",
  "size_bytes": 1234,
  "mtime_unix_ms": 1700000000000,
  "sha256": "64位十六进制SHA-256"
}
```

### 3.2 Directory Entry

```json
{
  "path": "src",
  "kind": "dir",
  "mtime_unix_ms": 1700000000000
}
```

字段说明：

| 字段 | file | dir | 说明 |
|---|---|---|---|
| `path` | 必填 | 必填 | 规范化后的相对路径 |
| `kind` | 必填 | 必填 | `file` 或 `dir` |
| `size_bytes` | 必填 | 忽略 | 文件字节数 |
| `mtime_unix_ms` | 必填 | 可选 | Unix epoch 毫秒 |
| `sha256` | 必填 | 忽略 | 文件内容 SHA-256，十六进制 |

v1 只支持普通文件和目录，不支持 symlink、权限位、owner/group、可执行位等元数据。

### 3.3 文件相等判断

服务端认为两个文件相同，当且仅当以下字段全部相同：

- `size_bytes`
- `mtime_unix_ms`
- `sha256`

这意味着如果客户端无法保留 mtime，后续同步可能会再次认为文件发生变化。

### 3.4 服务端限制

默认配置：

```toml
sync_max_file_bytes = 1073741824
sync_max_manifest_entries = 200000
sync_session_ttl_secs = 3600
```

含义：

- 单文件最大 1 GiB。
- 单次 manifest 最多 200000 个 entry。
- push sync session 默认 3600 秒后过期并清理 staging。

## 4. Glob 过滤

同步请求可以携带过滤规则：

```json
{
  "include_globs": ["src/**", "constraints/**"],
  "exclude_globs": ["**/*.tmp", ".git/**"]
}
```

规则：

- 过滤规则只对单次请求生效。
- 服务端没有默认 include/exclude 配置。
- `include_globs` 为空时，表示包含所有未被 exclude 的路径。
- `include_globs` 非空时，路径必须至少匹配一个 include。
- 只要匹配任意 exclude，就会被排除。
- `.vivado-server-sync` 和 `.vivado-server-sync/**` 永远被服务端排除。

Vivado 工程 push 的常见排除项：

```json
[
  ".git/**",
  ".Xil/**",
  "*.jou",
  "*.log",
  "*.str",
  "*.cache/**",
  "*.runs/**",
  "*.sim/**"
]
```

注意：push 时通常排除生成物；pull 时可能正好需要包含某些生成目录，例如
`out/**` 或特定 bitstream 输出目录。

## 5. Push 同步：客户端到服务器

Push 用于把客户端本地工程同步到服务器的 `workspace_root/<project>`。

流程：

1. 客户端扫描本地 project 目录，生成 manifest。
2. 调用 `POST /v1/projects/{project}/sync/push/plan`。
3. 服务端返回 `sync_id`、需要上传的文件、需要创建的目录，以及可选删除列表。
4. 客户端对 `upload_files` 中每个文件调用 `PUT` 上传原始字节流。
5. 客户端调用 `commit`。
6. 服务端把 staging 中的文件移动到 project 目录；如果 `delete_extra=true`，
   则删除服务器端多余文件。

关键语义：

- commit 之前不会覆盖 project 文件。
- `upload_files` 里的文件必须全部上传，否则 commit 返回 `409`。
- `sync_id` 会过期；过期后服务端可删除 staging。
- commit 默认做乐观冲突检测：如果 plan 后服务器目标文件变了，返回 `409`。
- `force=true` 会跳过冲突检测并覆盖目标文件。

### 5.1 创建 Push Plan

```http
POST /v1/projects/{project}/sync/push/plan
Authorization: Bearer <token>
Content-Type: application/json
```

请求：

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

响应：

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

字段含义：

- `upload_files`：客户端必须上传的文件。
- `create_dirs`：commit 时服务端会创建的目录。
- `delete_files`：仅当请求 `delete_extra=true` 时，commit 会删除的服务器文件。
- `delete_dirs`：仅当请求 `delete_extra=true` 时，commit 会删除的服务器目录。
- `expires_at`：sync session 过期时间。

### 5.2 上传文件

```http
PUT /v1/projects/{project}/sync/{sync_id}/files/{path}
Authorization: Bearer <token>
Content-Type: application/octet-stream
```

请求 body 是文件原始字节。

示例：

```sh
curl -X PUT \
  -H "Authorization: Bearer change-me" \
  --data-binary @src/top.tcl \
  "http://127.0.0.1:8080/v1/projects/demo/sync/8f6989d1-8a61-4dbb-98e4-b24a408729e2/files/src/top.tcl"
```

响应：

```json
{
  "path": "src/top.tcl",
  "size_bytes": 1234,
  "sha256": "..."
}
```

服务端校验：

- `path` 必须在 plan 返回的 `upload_files` 中。
- 上传字节数必须等于 `size_bytes`。
- 上传内容 SHA-256 必须等于 `sha256`。
- 文件大小不能超过 `sync_max_file_bytes`。

客户端建议：

- 上传时流式读取本地文件。
- 不要把文件放进 JSON。
- 不要 base64。
- 如果本地文件在上传过程中变化，应取消本轮 sync 并重新 plan。

### 5.3 Commit

```http
POST /v1/projects/{project}/sync/{sync_id}/commit
Authorization: Bearer <token>
Content-Type: application/json
```

请求：

```json
{
  "force": false
}
```

响应：

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

冲突处理：

- `force=false` 时，服务端检查 plan 时的服务器文件 SHA-256 是否仍然匹配。
- 如果目标文件被其他进程或用户改动，返回 `409`。
- `force=true` 跳过冲突检测，直接覆盖本轮 sync 涉及的目标文件。

推荐客户端行为：

- 默认使用 `force=false`。
- 遇到 `409` 时提示用户“服务器文件在同步期间发生变化”，并重新 plan。
- 只有用户明确确认覆盖时才使用 `force=true`。

### 5.4 Abort

```http
DELETE /v1/projects/{project}/sync/{sync_id}
Authorization: Bearer <token>
```

响应：

```json
{
  "sync_id": "8f6989d1-8a61-4dbb-98e4-b24a408729e2",
  "status": "aborted"
}
```

Abort 只删除 staging，不修改 project 文件。客户端在取消上传、上传失败或退出同步流程时，
应尽量调用 abort。

## 6. Pull 同步：服务器到客户端

Pull 用于把服务器生成物或服务器 project 状态同步回客户端。

流程：

1. 客户端扫描本地目录，生成 manifest。
2. 调用 `POST /v1/projects/{project}/sync/pull/plan`。
3. 服务端返回需要下载的文件、需要创建的目录，以及可选本地删除列表。
4. 客户端下载 `download_files` 中每个文件。
5. 客户端校验响应头中的 size 和 SHA-256。
6. 如果 `delete_extra=true`，客户端根据响应里的删除列表删除本地多余文件。

注意：

- Pull 不创建服务端 sync session。
- Pull 响应中的 `delete_files` / `delete_dirs` 是客户端本地删除指令，
  服务端不会替客户端删除本地文件。

### 6.1 创建 Pull Plan

```http
POST /v1/projects/{project}/sync/pull/plan
Authorization: Bearer <token>
Content-Type: application/json
```

请求：

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

响应：

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

字段含义：

- `download_files`：客户端应下载的服务器文件。
- `create_dirs`：客户端应创建的本地目录。
- `delete_files`：仅当请求 `delete_extra=true` 时，客户端应删除的本地文件。
- `delete_dirs`：仅当请求 `delete_extra=true` 时，客户端应删除的本地目录。

### 6.2 下载文件

```http
GET /v1/projects/{project}/sync/files/{path}
Authorization: Bearer <token>
```

响应头：

```http
Content-Type: application/octet-stream
Content-Length: <size>
x-sync-size-bytes: <size>
x-sync-mtime-unix-ms: <mtime>
x-sync-sha256: <sha256>
```

响应 body 是文件原始字节。

客户端要求：

- 下载到临时文件。
- 下载时计算 SHA-256。
- 校验下载字节数等于 `x-sync-size-bytes`。
- 校验 SHA-256 等于 `x-sync-sha256`。
- 校验通过后再原子 rename 到目标路径。
- 平台支持时设置本地 mtime 为 `x-sync-mtime-unix-ms`。

## 7. 获取服务器 Manifest

客户端也可以单独请求服务器 manifest，用于调试或自定义比较逻辑。

```http
POST /v1/projects/{project}/sync/manifest
Authorization: Bearer <token>
Content-Type: application/json
```

请求：

```json
{
  "include_globs": [],
  "exclude_globs": [".git/**", ".Xil/**"]
}
```

响应：

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

如果服务器 project 目录不存在，服务端会创建目录并返回空 manifest。

## 8. Vivado Session API

同步 API 负责文件，session API 负责远程 Vivado 进程控制。

### 8.1 创建 Session

```http
POST /v1/sessions
Authorization: Bearer <token>
Content-Type: application/json
```

请求：

```json
{
  "project": "demo",
  "args": ["-mode", "tcl"]
}
```

响应：

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

服务端会在 `workspace_root/<project>` 中启动 Vivado。`args` 会作为参数数组传给
Vivado，不经过 shell 拼接。

### 8.2 发送输入

```http
POST /v1/sessions/{session_id}/stdin
Authorization: Bearer <token>
Content-Type: application/json
```

请求：

```json
{
  "text": "open_project demo.xpr\n"
}
```

服务端会把 `text` 写入 Vivado 进程 stdin。客户端需要自己追加换行。

### 8.3 轮询输出

```http
GET /v1/sessions/{session_id}/output?cursor=0&timeout_ms=30000
Authorization: Bearer <token>
```

响应：

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

客户端行为：

- 第一次请求用 `cursor=0`。
- 每次响应后，下次请求使用响应里的 `cursor`。
- `timeout_ms` 表示长轮询等待时间，服务端会限制最大值。
- 如果 `overrun=true`，说明服务端输出环形缓冲已经丢弃旧内容。客户端应提示
  “部分输出被截断”，然后继续使用新的 `cursor`。

### 8.4 Heartbeat

```http
POST /v1/sessions/{session_id}/heartbeat
Authorization: Bearer <token>
```

客户端应定期发送 heartbeat。推荐间隔为 `heartbeat_timeout_secs / 3`。

### 8.5 查询和终止 Session

```http
GET /v1/sessions/{session_id}
DELETE /v1/sessions/{session_id}
Authorization: Bearer <token>
```

可能状态：

```text
running
exited
terminated
failed
```

`DELETE` 会先尝试向 Vivado 写入 `exit\n`，短暂等待后强制 kill。

## 9. 推荐客户端算法

### 9.1 本地 Manifest 生成

伪代码：

```text
manifest = []
for each entry under local_project_root:
    rel = convert_to_utf8_relative_path(entry)
    rel = replace "\" with "/"
    reject absolute path, "..", drive prefix
    skip ".vivado-server-sync/"
    apply include/exclude globs
    if symlink:
        reject or skip with warning
    if directory:
        manifest.push({ path: rel, kind: "dir", mtime_unix_ms })
    if regular file:
        sha256 = stream_sha256(file)
        manifest.push({
            path: rel,
            kind: "file",
            size_bytes,
            mtime_unix_ms,
            sha256
        })
sort manifest by path
```

注意：

- SHA-256 必须流式计算。
- 不要一次性把大文件读入内存。
- 上传前最好再次确认文件 mtime/size 未变化；如果变化，重新生成 manifest。

### 9.2 安全 Push

伪代码：

```text
local_manifest = scan_local()
plan = POST push/plan(local_manifest, filters, delete_extra)
try:
    for file in plan.upload_files:
        PUT raw bytes to /sync/{sync_id}/files/{file.path}
    POST /sync/{sync_id}/commit {"force": false}
catch cancellation:
    DELETE /sync/{sync_id}
catch upload_or_commit_error:
    DELETE /sync/{sync_id} if possible
```

重试建议：

- 同一个文件内容未变化时，可以重试同一个 `PUT`。
- 本地文件变化后，不要继续使用旧 plan，应重新 plan。
- commit 返回 `409` 时优先重新 plan，不要自动 force。

### 9.3 安全 Pull

伪代码：

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

删除目录时必须 deepest-first，避免父目录非空导致删除失败。

### 9.4 典型远程 Vivado 流程

```text
1. Push 本地工程源文件到服务器。
2. 创建 Vivado session，project 使用同一个名称。
3. 通过 stdin 发送 Tcl/Vivado 命令。
4. 持续轮询 output，并定期 heartbeat。
5. Vivado 完成后删除 session 或等待其退出。
6. Pull 服务器上的结果文件或日志回客户端。
```

## 10. 并发和一致性

服务端允许 Vivado session 运行时执行同步。客户端需要自己控制时机：

- 推荐先 push，再启动 Vivado。
- 不建议在 Vivado 正在读写同一工程时 push 覆盖源文件。
- 推荐 Vivado 完成或进入明确 checkpoint 后再 pull 输出。
- push commit 默认使用 `force=false`，这样可以发现 plan 后服务器文件被改动的情况。

## 11. 平台兼容说明

### Windows 客户端

- 本地路径分隔符 `\` 必须转换为 `/` 后再发送。
- 不要发送 drive prefix。
- NTFS/FAT 的 mtime 精度可能不同，客户端应发送能获得的毫秒级 mtime。

### Linux/macOS 客户端

- v1 不支持 symlink。
- v1 不同步权限位、owner/group、可执行位。

### 大文件

- 上传和下载都应使用流式 I/O。
- 下载写入临时文件，校验成功后 rename。
- 遵守 `sync_max_file_bytes`。

## 12. 最小 Push 示例

假设本地 `src/top.tcl` 内容为 `puts hello\n`。

创建 plan：

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

上传：

```text
PUT /v1/projects/demo/sync/<sync_id>/files/src/top.tcl
```

Commit：

```json
POST /v1/projects/demo/sync/<sync_id>/commit

{
  "force": false
}
```

## 13. 最小 Pull 示例

创建 pull plan：

```json
POST /v1/projects/demo/sync/pull/plan

{
  "entries": [],
  "delete_extra": false,
  "include_globs": ["out/**"],
  "exclude_globs": []
}
```

下载返回的每个文件：

```text
GET /v1/projects/demo/sync/files/out/result.txt
```

校验：

```text
sha256(downloaded bytes) == x-sync-sha256
downloaded byte count == x-sync-size-bytes
```

## 14. Endpoint 总表

| Method | Path | 用途 |
|---|---|---|
| GET | `/healthz` | 健康检查，无需认证 |
| POST | `/v1/projects/{project}/sync/manifest` | 获取服务器 manifest |
| POST | `/v1/projects/{project}/sync/push/plan` | 规划客户端到服务器同步 |
| PUT | `/v1/projects/{project}/sync/{sync_id}/files/{path}` | 上传计划内文件 |
| POST | `/v1/projects/{project}/sync/{sync_id}/commit` | 提交 push sync |
| DELETE | `/v1/projects/{project}/sync/{sync_id}` | 取消 push sync |
| POST | `/v1/projects/{project}/sync/pull/plan` | 规划服务器到客户端同步 |
| GET | `/v1/projects/{project}/sync/files/{path}` | 下载服务器文件 |
| POST | `/v1/sessions` | 启动 Vivado session |
| GET | `/v1/sessions/{session_id}` | 查询 session 状态 |
| POST | `/v1/sessions/{session_id}/stdin` | 发送 Vivado 输入 |
| GET | `/v1/sessions/{session_id}/output` | 轮询 Vivado 输出 |
| POST | `/v1/sessions/{session_id}/heartbeat` | session 保活 |
| DELETE | `/v1/sessions/{session_id}` | 终止 session |
