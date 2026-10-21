# mcp-server（Rust）

`mcp-server` 的完整运行功能已迁入此 Cargo workspace，提供独立 `mcp-server` 可执行文件和可嵌入的 Rust 库。生产构建和运行无需 Go。

```sh
cargo build --locked --release -p mcp-server
./target/release/mcp-server -transport stdio -workdir /data
./target/release/mcp-server -transport http -addr :3001 -workdir /data
```

HTTP 接口为 `/mcp`。支持原有 `-env-socket`、`-core-addr`、`WORK_DIR` 和 CLI 参数语义。运行环境需提供 `bash`、`rg`；`bash` 支持 Unix 进程组、TERM/KILL、请求取消及后台输出文件。

| 功能 | 实现 |
| --- | --- |
| MCP | 官方 rmcp SDK、2026-07-28、server/discover、tools/list、tools/call、stdio 和 Streamable HTTP |
| 内置工具 | bash、read、write、edit、grep、glob |
| 原有别名 | run_in_terminal、file_read、file_write、file_edit、grep_run、glob_run |
| 文件 | 原子覆盖、符号链接、权限、BOM/CRLF、唯一替换、diff、分页、输出持久化 |
| 图片 | PNG/JPEG/GIF/WebP、尺寸/编码预算、缩放、透明度、JPEG 和 PNG 兼容编码 |
| 搜索 | ripgrep、排序/分页、glob brace/extglob/字符类/递归/排除规则 |
| 运行集成 | Unix socket 环境快照、变量覆盖、交互命令提示、插件生命周期和服务停止客户端 |

库入口为 `Registry`、`Tool`、`Server`、`EnvironmentProvider` 和 `plugins::Manager`。主项目通过 `internal::mcp::servers::runtime_tools` 重导出该库。

使用标准库文件和进程接口、Tokio、Hyper/Axum、serde、reqwest/rustls、regex 和 image/gif/image-webp。JSON、错误文本、图片整数精度和压缩决策的兼容适配独立在专用模块；无需调用参考程序或持有原项目。图片派生算法许可见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。

## Bash 执行结果

`bash` 和 `run_in_terminal` 声明相同的 `outputSchema`，并返回三种兼容表示：原有的 `content[0]` 文本、`structuredContent` 执行结果，以及 `content[1]` 中的同一结果 JSON。其他工具保持原有格式。

执行结果独立提供 `stdout`、`stderr`、`exit_code`、`status`、`duration_ms`、`timed_out`、`cancelled`，以及进程、信号和各流字节数、截断标记、保留文件路径。`status` 包括 `not_started`、`running`、`completed`、`timed_out`、`cancelled`；`completed` 表示已退出，成功与否由退出码和 `isError` 判断。未启动和仍在后台运行时，`exit_code` 为 null。

完成后的流预览各限制为末尾 30000 字节，后台快照各限制为末尾 2000 字节。后台执行、截断或读取失败时保留相应日志；普通完成且未截断的日志会清理。合并日志保留原有状态行，独立流文件只包含命令输出。后台后代持续持有输出管道时，主进程退出后的采集等待最多 2 秒，采集失败会进入 `error` 并设置 `isError`。

## MCP 2026-07-28

协议和传输使用官方 [`rmcp`](https://github.com/modelcontextprotocol/rust-sdk) 3.5.1。服务仅接受 `2026-07-28`，不提供 `initialize`、旧版 `ping`、HTTP 会话、独立 GET/DELETE 流或旧版回退。`server/discover` 返回所支持的版本、工具能力和服务标识。

每个请求必须在 `params._meta` 中携带版本及客户端能力：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "bash",
    "arguments": {"command": "printf hello"},
    "_meta": {
      "io.modelcontextprotocol/protocolVersion": "2026-07-28",
      "io.modelcontextprotocol/clientCapabilities": {}
    }
  }
}
```

HTTP POST `/mcp` 还需要 `Content-Type: application/json`、`Accept: application/json, text/event-stream`、`MCP-Protocol-Version: 2026-07-28` 和与消息方法一致的 `Mcp-Method`；`tools/call` 还需要与工具名一致的 `Mcp-Name`。SDK 验证头与消息体是否匹配，也处理 `x-mcp-header` 参数映射及 Base64 编码。结果包含 `resultType: complete` 和服务标识元数据。工具参数与结构化结果按 JSON Schema 2020-12 校验；外部 schema 引用不会自动访问网络或文件。

stdio 通过 `notifications/cancelled` 和 `params.requestId` 取消，输入读取可与工具执行并发。HTTP 通过关闭对应请求的响应流取消，不再使用 `X-OMA-MCP-Scope` 或单独的取消 POST。取消后工具清理进程组，SDK 不发送该请求的后续响应；其他请求可以继续执行。设置 `_meta.progressToken` 时，工具开始执行前会发送进度通知。

HTTP 默认允许 loopback Host，拒绝所有带有未授权 Origin 的浏览器请求。独立可执行文件可用逗号分隔的 `MCP_ALLOWED_HOSTS` 和 `MCP_ALLOWED_ORIGINS` 配置部署域名及浏览器来源；嵌入时使用 `Config.allowed_hosts` 和 `Config.allowed_origins`。

## environment-manager 启动集成

`environment-manager task-run` 在环境初始化成功后自动启动嵌入的 MCP HTTP 服务，再启动 Claude 执行器。setup-only 和跳过 Claude 的模式同样启动服务。服务默认监听 `0.0.0.0:3001`，接口为 `/mcp`；`ENVRUNNER_RUNTIME_MCP_ADDR` 可以覆盖监听地址。

嵌入服务使用环境初始化后的工作目录和环境变量。默认允许外部客户端的 Host；可通过 `MCP_ALLOWED_HOSTS` 限定域名。浏览器 Origin 仍由 `MCP_ALLOWED_ORIGINS` 显式配置。端口绑定失败会使 manager 启动失败；正常结束、取消或后续启动错误都会停止服务并释放端口。服务随 manager 在进程内运行，无需再启动独立 `mcp-server` 子进程。

## 验证

```sh
cargo test --locked -p mcp-server
cargo clippy --locked -p mcp-server --all-targets -- -D warnings
```

`tests/protocol_2026.rs` 通过真实 SDK stdio 和 HTTP 传输验证发现、元数据、版本拒绝、路由头、来源检查、并发取消及请求隔离。`tests/bash_execution.rs` 验证结构化 Bash 结果和输出采集；`tests/compatibility.rs` 保留文件、图片、环境和插件的行为回归。

历史 Go 二进制对照及验收记录见 [迁移验收记录](../../tests/mcp_compatibility/VERIFICATION.md)。该记录针对旧版协议；当前仅支持新版协议，因此旧协议端到端零差异脚本不能作为当前协议验收门禁。主项目自身限定 Linux GNU，workspace 全量门禁需在相应平台运行。
