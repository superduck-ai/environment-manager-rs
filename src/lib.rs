//! Environment Manager 的可测试库入口。
//!
//! 模块布局机械映射自 Go 项目；二进制入口只负责参数分发、运行时初始化和退出码处理。
//! 业务模块默认限制在 crate 内部，避免在迁移期间无意建立新的公共 Rust API。

// 这是发布契约而非可移植性提示：项目只接受 Linux GNU ARM64。
// 在源码层直接拒绝其他 target，可防止开发机或 CI 误产生未测试的二进制。
#[cfg(not(all(target_os = "linux", target_arch = "aarch64", target_env = "gnu")))]
compile_error!("仅支持构建目标 aarch64-unknown-linux-gnu");

pub mod cmd;
// Edition 2024 将 `gen` 作为保留字；raw identifier 只影响 Rust 源码写法，
// 文件路径和公开模块名称仍保持 Go 的 `gen`，便于逐项对照。
pub mod r#gen;
pub mod internal;
