//! Environment Manager 的可测试库入口。
//!
//! 模块布局机械映射自 Go 项目；二进制入口只负责参数分发、运行时初始化和退出码处理。
//! 业务模块默认限制在 crate 内部，避免在迁移期间无意建立新的公共 Rust API。

// 项目依赖 Linux 的信号、`/proc` 和 GNU libc 语义，因此仍在源码层拒绝
// 其他操作系统或 libc；CPU 架构由构建 target 决定，不再限定为 AArch64。
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
compile_error!("仅支持 Linux GNU 构建目标，不限定 CPU 架构");

pub mod cmd;
// Edition 2024 将 `gen` 作为保留字；raw identifier 只影响 Rust 源码写法，
// 文件路径和公开模块名称仍保持 Go 的 `gen`，便于逐项对照。
pub mod r#gen;
pub mod internal;
