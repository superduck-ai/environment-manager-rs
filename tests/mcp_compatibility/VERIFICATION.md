# MCP Rust 迁移验收

## 最新现成二进制功能对照

2026-10-10 新增独立 [MCP 功能 E2E](binary_e2e/README.md)，直接运行用户提供的 x86-64 Go 可执行文件，不从源码编译参考服务。环境快照标量边界及 debug 大快照超时已修复；当前 Rust release 和 debug 分别对照 502 个步骤，均为零差异，独立功能断言全部通过。当前脚本对应的报告目录为 `target/mcp-binary-e2e-fixed-release/` 和 `target/mcp-binary-e2e-fixed-debug/`；历史产物未随命名修改迁移。

## 历史项目质量门禁

Ubuntu 24.04 ARM64、Rust 1.97.0：

- `cargo test --locked --workspace -- --test-threads=1`：508 个测试通过。
- `cargo clippy --locked --workspace --all-targets -- -D warnings`：通过。
- `cargo fmt --all --check`：通过。
- `RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps`：通过。
- `scripts/linux-arm64/verify-cargo-dependencies.sh .`：通过。
- 中文模块/函数注释和 unsafe 说明审计已覆盖新 crate。

测试运行使用容器内的普通工作区副本，避免 macOS worktree 的绝对 `.git` 指针和 AppleDouble 元数据影响 Linux 测试。现有测试涉及进程级环境变量，因此全量门禁采用单线程执行。

## 复现现成二进制对照

参考二进制的位置通过 `--go-binary` 显式指定，执行方式见 [MCP 功能 E2E](binary_e2e/README.md)。

这些历史对照针对旧版协议，不能作为当前协议的验收门禁。当前行为由 crate 内 Rust 测试验证，运行方式见 [验证说明](../../crates/mcp-server/README.md#验证)。
