//! Sandbox 的公共类型重导出入口。
//!
//! 配置和 runtime 的行为实现分别位于 `config.rs` 与 `runtime.rs`；本文件只重导出
//! 主要数据类型，避免复制文件所有权状态。

pub use super::config::{FilesystemConfig, NetworkConfig, SrtConfig};
pub use super::runtime::SandboxRuntime;
