# Qoder MCP Rust 迁移验收

## 最新现成二进制功能对照

2026-10-10 新增独立 [MCP 功能 E2E](binary_e2e/README.md)，直接运行用户提供的 x86-64 Go 可执行文件，不从源码编译参考服务。环境快照标量边界及 debug 大快照超时已修复；当前 Rust release 和 debug 分别对照 502 个步骤，均为零差异，独立功能断言全部通过。报告位于 `target/qoder-binary-e2e-fixed-release/` 和 `target/qoder-binary-e2e-fixed-debug/`。下文 7,046 项零差异记录属于此前的源码构建参考服务及专项语料。

## 既有源码专项对照

参考源码：`/Users/arthur/GolandProjects/qoder-mcp-server`，`go.mod` 指定 Go 1.26.8、`golang.org/x/image v0.16.0`。对照时两份服务器使用相同的 Ubuntu 24.04 ARM64 容器、工具版本和隔离 fixture。Rust 使用 1.97.0，GNU ARM64 target。生产 crate 不调用 Go，也不加载原项目。

## MCP 对照结果

| 语料 | 请求/用例数 | 差异数 |
| --- | ---: | ---: |
| 基础差分（协议、工具、别名） | 452 | 0 |
| glob 模式语料 | 279 | 0 |
| grep/glob 参数和执行对照 | 1,696 | 0 |
| 协议、CLI、HTTP、环境快照 | 914 | 0 |
| 文件、权限、磁盘写满、rename 回退 | 2,020 | 0 |
| 进程、信号、超时、后台、HTTP 取消 | 1,477 | 0 |
| 严格字面量及受控 rg argv | 56 | 0 |
| 大输出、图片、工具组合 | 75 | 0 |
| HTTP/环境传输补充 | 24 | 0 |
| JPEG/PNG/GIF/WebP 编解码扩展 | 53 | 0 |
| 合计 | **7,046** | **0** |

比较完整 JSON、声明/schema/description、内容块省略规则、错误文本、HTTP 状态/响应头、stderr 生命周期日志、真实文件字节/权限/符号链接、输出产物和图像 base64。进程时间使用相同边界判断，同时验证 TERM 宽限、进程组停止、客户端断开取消和后台继续执行。

基础语料只归一化 Go map 的工具枚举顺序、系统生成的临时文件名/PID/日志时间。56 个严格字面量用例检查用户文本不被归一化掩盖。53 个编解码扩展用例逐个比较完整 JSON 和原始 base64，覆盖渐进 JPEG、灰度、CMYK、采样比例、重启标记、有损/无损 WebP、PNG、GIF 透明度和偏移首帧。

另外使用 Rust 独立回归检查精确 PNG/JPEG/GIF 编码 golden、字段顺序/数字格式、无效 UTF-8、IPv4/IPv6 通配监听、插件生命周期、服务 DELETE、文件更新以及进程取消；正常 `cargo test` 无需 Go 参考源码。

## 项目质量门禁

Ubuntu 24.04 ARM64、Rust 1.97.0：

- `cargo test --locked --workspace -- --test-threads=1`：508 个测试通过。
- `cargo clippy --locked --workspace --all-targets -- -D warnings`：通过。
- `cargo fmt --all --check`：通过。
- `RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps`：通过。
- `scripts/linux-arm64/verify-cargo-dependencies.sh .`：通过。
- 中文模块/函数注释和 unsafe 说明审计已覆盖新 crate。

测试运行使用容器内的普通工作区副本，避免 macOS worktree 的绝对 `.git` 指针和 AppleDouble 元数据影响 Linux 测试。现有测试涉及进程级环境变量，因此全量门禁采用单线程执行。

## 复现

```sh
QODER_GO_SOURCE=/Users/arthur/GolandProjects/qoder-mcp-server tests/qoder_compatibility/run.sh
# 也可只重跑指定语料：
tests/qoder_compatibility/run.sh protocol_alignment image_formats
```

逐项报告及 SHA256 位于 `target/qoder-compatibility/recovery/evidence/`。兼容结论表示上述可重复语料全部零差异，不将有限回归测试解释成对所有可能输入的数学证明。未执行部署或改变原 Go 项目的源码。
