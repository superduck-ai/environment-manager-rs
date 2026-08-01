# 确定性差分范围

`scripts/compat/run_differential.sh` 是 Linux ARM64 的统一差分入口。它先执行 27 个
CLI 场景，再生成并比较 `http_api`、`input`、`mcp`、`process_edge`、
`tunnel_protojson`、`tunnel_http_runtime` 和 `tunnel_ws_runtime` 七份 transcript。
Go 侧使用固定 Go 1.25.7 digest 镜像和 `go test -overlay`；overlay backing file
位于 Rust 仓库，Go 工作树始终以只读方式挂载。`process_edge` 是只依赖 Go 标准库的
独立 probe，仍在同一个固定镜像内执行，不向 Go 工作树注入或写入文件。

除此之外，Linux ARM64 总门禁还固定运行
`scripts/compat/run_logger_source_differential.sh`。它不是七 transcript comparator
的可选 normalizer，而是单独的严格证据链：先从固定 Go 1.25.7 `-trimpath` ARM64 ELF
提取每个生产 slog 调用表达式中的 PC，再由同一个 ELF 通过 `runtime.CallersFrames`
重建 `function/file/line` oracle。候选 PC 是静态近似：已由 canonical CLI 实际输出证明
不同的项目必须逐项登记在 `compat/go-log-source-canonical-overrides.tsv`，同时钉住
probe 旧值与 canonical 新值；任一侧漂移都会失败，不能扩大成 source normalizer。
catalog probe 只在容器临时副本中参与链接；一个
不可达环境变量分支负责保留链接器原本会裁掉的调用点，既不执行业务逻辑，也不写入
Go 工作树。

`go-rust-log-sources.tsv` 必须 100% 为 `mapped-oracle`。审计同时从 Rust 向 Go 检查
runtime 规则，并从 Go 向 Rust 检查 683 个生产调用点均被恰好一个 Rust callsite 消费；
零消费、多消费、identity 冲突和无来源规则都失败。生产 `tracing::*` 宏由独立扫描器
一律作为 `tracing-event-not-source-compatible` blocker，不能绕过 Logger source
映射。之后门禁逐字节比较 Go/Rust `LogWriter` 的完整行和截断行 source。transcript
刻意不写 time；source 三字段永不替换、trim、路径归一化或忽略。生产 Rust 调用未映射
时输出 `<unmapped-go-log-source>`，因此不会以 Rust caller fallback 制造假阳性。

协议 transcript 外层 JSON 只是 probe 容器，比较时 object key 顺序不重要。下面这些
字段保存真正的 wire 字节并按字符串精确比较，不会 parse、trim 或重排：

- HTTP API 的请求 `body`；
- MCP 的 `request_body` 和 `response_body`；
- Tunnel 的 ProtoJSON `wire`。

固定 MCP debug token 为 `fixed-mcp-token`，随机 loopback 端口只用于连接且不写入
对应 transcript。input probe 则固定使用 `127.0.0.1:41791`，producer 不改写 URL。
唯一不可逐值复现的字段是 parser 的 `parse_duration_ms`：Go/Rust transcript 都保留
原始非负整数，comparator 只按 manifest 中唯一审核过的精确路径规则验证并替换比较
副本；原始文件和摘要仍保留真实值。

`process_edge` 的 Go/Rust producer 先分别保留一份 raw transcript，再通过共同的
`scripts/compat/process_edge_transcript.py` 转换为 canonical JSON。转换器只允许两类
已审核的不可预测值，并在替换前验证其结构：

- probe 显式声明的临时根原始字节必须是无 NUL 的绝对 Unix 路径，而且必须精确出现
  三次；只把这段完全相同的字节替换为 `{temp_root}`；
- ExtraFiles 的三个 parent source FD 必须互不相同、均不小于 64 且全部设置
  `FD_CLOEXEC`，随后才按参数顺序替换为 `{source_fd_0..2}`。

PID 只用于 probe 内部读取 `/proc` 和清理，不写入 transcript。zombie 状态、child 是否
存在、future/channel 是否 pending、signal、errno、LookPath 结果和错误原始字节均逐值
比较；不允许归一化 timing、state 或 error 语义。对于这些协议 transcript，除了上述
producer 转换和唯一的 input duration comparator 规则之外，不允许任何
normalizer。CLI 的独立 normalizer 则必须按下方用例逐项审核。

## Input parser cases

- `v0_full`：通过真实 V0 parser 覆盖原始 stdin bytes、URL 补 scheme、git source、
  inline/file MCP、environment 未知字段、四类 auth、有效与被跳过的 repository outcome，
  并比较 input/auth logger 的级别、消息、属性顺序和值。
- `v0_null_optional`：固定 source/outcomes/MCP/工具/map/auth 的显式 JSON `null` 到 Go
  零值语义；`v0_missing_auth_invalid_mcp_base64` 同时区分 auth missing，并验证坏的标准
  Base64 只跳过 MCP 内容 DEBUG 日志，不使整体解析失败。
- `v0_invalid_environment`：比较显式 null environment 的错误顺序和已产生日志。
- `v0_missing_startup_context`：Go overlay 用 `recover`、Rust probe 用 `catch_unwind`
  分别记录 panic/error 分支，固定当前 Go nil dereference 行为；不能把崩溃场景从覆盖
  清单删除。`v0_null_startup_context` 独立比较显式 null 创建零值 context 后的校验错误。
- 真实 CLI 另有 `task_run_v0_missing_startup_context_panic`：它不捕获 panic，而是比较
  完整进程的 stdout、stderr 和退出码。独立 Go 进程捕获证明，仅 receiver、heap
  slice/对象、argv 与 working-directory 字符串 data 指针所在的九个固定 frame 参数槽
  会变化，因此该用例只对这些槽启用 `go_panic_runtime_hex`。读取 stdin
  的 `read_duration_ms` 同样受 Linux 调度影响；`structured_log_read_duration_ms`
  先确认整行是 JSON 对象、顶层 `msg` 唯一且精确为 `Read stdin data`，再且只替换
  唯一的顶层同名数值。该 normalizer
  在 manifest 校验中被锁定到本用例，原始 capture 仍保留真实耗时。signal 的
  `addr=0xc0`/`pc=0x61a128`、静态指针、长度、
  panic 文本、函数名、文件、行号、稳定 `+0x` frame 偏移、frame 顺序和 exit 2 仍
  逐字节比较。Rust 输出资产必须由固定 Go 1.25.7 ARM64 oracle 通过
  `scripts/compat/generate_v0_panic_asset.py` 捕获，普通 Rust panic hook 不参与该分支。
  `run_differential.sh` 会在 Rust 构建前以 `--check` 再运行 oracle 两次；资产缺 frame、
  frame 顺序改变或除白名单十六进制值外任一字节陈旧都会直接阻断门禁，验证过程不会
  自动改写已提交资产。
- `v1_code_session_full`：比较 padded URL-safe Base64 secret、source/auth、顶层 ingress
  token 覆盖、Claude args、MCP file、environment variables 和 working-directory fallback。
- `v1_healthcheck_null_optional`：比较 work envelope、null-to-zero secret 和最小 healthcheck
  结果/日志，确保不调用 session-context provider。
- `v1_session_context_full`、`v1_session_context_missing`、
  `v1_session_context_null`：固定 provider 参数和 full/missing/explicit-null RawMessage 差别，
  覆盖 prompt、model、cwd fallback、inline MCP presence 以及 repository outcomes。
- `v1_invalid_base64`：比较生产 DecodeWorkSecret 的外层错误文本和失败前日志。

Input transcript 故意不比较 logger `source`，因为 Go/Rust 文件名与函数名属于不同语言，
且完整 caller/source 精确映射由独立 logger source 门禁覆盖；这里专注 parser 的业务
事件、级别、顺序和属性。事件时间同样不进入 transcript。

## HTTP API cases

- `success`：真实客户端向 `/ok?mode=success` 发送含 HTML/行分隔符的有序 JSON，检查
  POST、Bearer、Content-Type、runner version、自定义 header、Go 默认 User-Agent、
  Transport 自动 `Accept-Encoding: gzip`、原始请求 body 及 gzip 成功响应解码。
- `not_implemented`：mock 返回 501 和 `missing`，比较 Go sentinel 对外错误文本。
- `non_ok`：mock 返回 418 和 `teapot`，比较普通非 OK 错误文本。
- `cancelled_no_retry`：预先取消一次零重试 POST，比较最后 attempt 的 Go exhausted
  包装顺序，防止 Rust 提前返回不同的 cancellation 文本。
- `connection_refused`：连接刚关闭的 loopback 端口，仅在 probe 内把随机地址替换为
  `{address}`，其余 `url.Error`、`dial tcp` 与 Linux 错误文本逐字比较。

## MCP cases

- `unauthorized`：缺少 Bearer，比较 401、`http.Error` content type 和原始 body。
- `initialize`：协商 `2025-03-26`，比较 capabilities 与 serverInfo 的完整 JSON-RPC。
- `initialized_notification`：无 ID notification 必须返回 202 且 body 为空。
- `tools_list`：比较工具说明、inputSchema、annotations、omitempty 和字段顺序。
- `tools_call`：字符串 ID、未知顶层/params 字段和工具参数经过真实 handler，返回
  `echo:hello` 文本 content。
- `tool_not_found`：比较 JSON-RPC -32602 及完整错误文本。
- `method_not_found`：比较 JSON-RPC -32601 及完整错误文本。
- `parse_error`：非法 JSON，比较 HTTP status、content type、JSON-RPC -32700 与换行。

## Linux process-edge cases

- `lookpath_missing`、`lookpath_non_executable`：比较 PATH 查找失败的候选路径和错误
  原始字节。
- `lookpath_errdot`、`lookpath_errdot_last_zero`、`lookpath_errdot_last_one`：固定空 PATH
  element 的 ErrDot 结果，以及 `GODEBUG=execerrdot` 重复键最后值生效的语义。
- `lookpath_non_utf8_directory`、`lookpath_non_utf8_name`：验证 PATH 目录和命令名包含非法
  UTF-8 字节时不经过 lossy String 转换。
- `run_non_utf8_enoent`、`run_non_utf8_eacces`：比较显式 slash spawn 的 PathError 类型、
  errno 与完整原始路径字节。
- `run_pre_cancel_non_utf8_enoent`、`run_pre_cancel_missing_basename`：固定预取消优先级；
  direct path 返回 `context canceled`，basename 仍先执行 LookPath 并返回 `exec.Error`。
- `combined_raw_bytes`：比较 CombinedOutput 的非法 UTF-8、NUL、exit error 和 `%w, %s`
  包装结果，不把 error/output 转成 Unicode。
- `reader_first_reap`、`combined_output_reap`：分别固定生产 reader-first 和标准库
  wait-first 的 zombie、pipe EOF 与 direct child 回收顺序。
- `cancel_reader_first`、`cancel_combined_output`：确认取消只 SIGKILL direct child，
  descendant 持有 pipe 时结果仍 pending，释放后错误为 `signal: killed`。
- `extra_files_fd3`：验证 parent source FD 的 CLOEXEC、child FD 3/4/5 payload 顺序，
  以及 source FD 不泄漏到 child。

## Tunnel ProtoJSON cases

- `http_request`、`http_cancel`：检查 oneof lowerCamel 名称、重复 header、query path、
  端口和 bytes 的标准 padded Base64。
- `ws_open`：检查 WebSocket path、header 与子协议顺序。
- `ws_binary_message`：检查包含 NUL/非 UTF-8 字节的 Base64 和 `isBinary: true`。
- `ws_text_message_default_bool`：检查文本 bytes Base64，且 proto3 false 字段被省略。
- `ws_close`：检查 close code/reason。
- `http_response_headers`、`http_response_chunk`：检查状态、sequence、final 和 bytes。
- `ws_response_error`：检查 WebSocket error oneof 与字段名称。

## HTTP Tunnel runtime cases

- `stream_flush_and_request`：真实转发 POST body，并核对 request-target、Host、默认
  User-Agent、自定义 header；服务端分两次 flush 很小的 chunk，门禁要求事件边界、
  sequence 和 final 顺序与 Go 一致。
- `custom_reason_phrase`、`empty_reason_phrase`：raw HTTP/1.1 mock 分别返回自定义及空
  reason phrase，比较后端收到的 `status_text` 原文。
- `invalid_method`、`invalid_header`：覆盖请求构造和 transport 校验失败时的 sender
  事件顺序、错误文本及函数返回值，不依赖外部端口。
- `fragment_header_error`、`fragment_connection_refused`：固定 Go `EscapedFragment`
  在请求构造错误和拨号错误中的 URL 展示，确保 fragment 不进入 request-target，
  但仍以 Go 的逐字节 escaping 出现在错误文本中。
- `raw_query_printable_bytes`：比较包含空格、保留标点、非 ASCII 字节和非法 `%zz`
  的 Go `RawQuery` 原始 request-target，禁止 Rust URI parser 预先拒绝或重新编码。
- `connection_refused`：连接刚关闭的 loopback 端口；仅把本次动态端口替换为
  `{port}`，其余 `dial tcp`、地址族、标点和 OS 错误文本逐字比较。
- `slow_connect_outside_header_timeout`、`response_before_slow_body_complete`、
  `slow_response_header_timeout`：固定 `ResponseHeaderTimeout` 只在完整请求写出后启动，
  拨号和慢上传不消耗门限，服务端提前响应也不会等待请求 body 写完。
- `multi_member_auto_gzip`、`invalid_auto_gzip`、`truncated_auto_gzip`、
  `partial_truncated_auto_gzip`、`checksum_auto_gzip`：固定 Transport 自动 gzip 的逐
  member chunk 边界、部分 payload 先行交付，以及 header、EOF、checksum 错误文本。
- `early_eof_response`、`malformed_status_response`、`invalid_header_response`、
  `conflicting_content_length`、`malformed_chunk`、`connection_reset_before_headers`：
  覆盖 Linux raw HTTP 的 EOF、状态行、MIME header、Content-Length、chunk framing
  和 TCP RST，逐字比较 Go Transport 包装层及本地/远端地址文本。
- `cancel`：mock 确认请求已到达后调用真实 `CancelRequest`。含随机端口的原始错误先
  在 probe 内严格检查固定上下文，再只输出 `context_canceled` 等稳定分类；比较器仍
  不做任何 normalizer。

## WebSocket Tunnel runtime cases

- `success_frames_and_close`：建立真实本地 WebSocket，比较 Host、自定义握手 header、
  子协议报价与选中结果，以及双向 text/binary frame、Base64 bytes 和主动 close。
- `bad_handshake`：真实 403 握手失败，比较 Gorilla 对外的稳定错误文本及事件顺序。
- `abnormal_eof`：raw mock 完成合法 101 后直接关闭 TCP，比较 Gorilla 在无 close
  frame 时产生的 1006 close code 与 `unexpected EOF` reason。

ProtoJSON cases 固化后端 payload 编码；两个 runtime transcript 则覆盖真实 HTTP 和
WebSocket 本地传输。ping/pong、重连等更长生命周期仍由 Linux 集成测试覆盖。所有
probe 都只连接进程内 loopback，不替代后续 staging E2E；随机端口只参与连接，不写入
transcript。

输出位于：

- `target/differential/protocol/go/*.json`
- `target/differential/protocol/rust/*.json`
- `target/differential/protocol/go/process_edge.raw`
- `target/differential/protocol/rust/process_edge.raw`
- `target/differential/protocol/report.json`
- `target/differential/logger-source/go.jsonl`
- `target/differential/logger-source/rust.jsonl`
- `target/differential/logger-source/go-log-source-oracle.tsv`

比较器还用 manifest 固定 case 名称和顺序，所以同时从 Go/Rust probe 删除同一场景也
会失败，不能用“双方一起漏测”制造假阳性。
