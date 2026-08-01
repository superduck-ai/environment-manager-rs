# 已接受的 Go/Rust 行为差异

本文记录经过明确决策、不会继续通过私有依赖补丁复刻的行为。这里的项目必须仍通过
正常业务路径测试；新增差异不能仅凭修改测试写入本清单。

## WebSocket：采用上游 Tungstenite 的标准行为

Rust 实现自 2026-08-01 起不再 vendoring `tungstenite`，主 Tunnel 和本地 WebSocket
Tunnel 统一使用 `tokio-tungstenite 0.29`。正常的标准握手、text/binary、分片、
ping/pong、close、HTTP/WS 转发和断线重连仍属于必须支持的业务行为。

相对 Go Gorilla WebSocket，明确接受以下差异：

- `Connection` 和 `Upgrade` 响应头必须符合上游 Tungstenite 的严格握手校验，不接受
  仅靠 token-list 中包含目标 token 的非标准响应。
- 服务端选择的子协议必须来自客户端报价列表；未报价的子协议会导致握手失败。
- text frame 必须是合法 UTF-8；非法或跨分片组合后非法的 text 消息会上报协议错误，
  不再按 `is_binary=false` 透明转发原始字节。
- 本地 socket 首次写失败或超时后，Rust 立即移除该 tunnel、取消读循环并丢弃连接；
  不复刻 Gorilla 保留连接和永久复用首个写错误的行为。

这些差异只覆盖异常或非标准 peer。受控后端和本地开发服务器必须使用标准 WebSocket；
任意二进制 payload 必须发送 binary frame，不能伪装为 text frame。

## URL：采用上游 WHATWG 点段归一化

Rust 实现自 2026-08-01 起不再 vendoring `url`，统一使用 crates.io 的 `url 2.5.8`。
后端及其 ID 格式由项目控制，动态 session、environment、work 等 ID 不允许使用完整的
`.` 或 `..` 路径段；正常 UUID 和其他合法 ID 的转义行为仍由 `go_path_escape` 保持一致。

相对 Go `net/url`，明确接受以下差异：Go `url.PathEscape` 会保留恰好为 `.` 或 `..`
的动态 path segment，而 Rust 上游 `url` 按 WHATWG 规则删除 `.` 或回退 `..`。因此，
如果违反后端 ID 约束传入这两个值，Rust 最终 request-target 可能与 Go 不同。该输入不再
属于受支持协议，差分门禁也不再构造它；相对重定向等标准 URL 行为继续由上游实现负责。
