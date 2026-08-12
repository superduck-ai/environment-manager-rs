//! Sandbox 的公共类型重导出入口。
//!
//! 配置和 runtime 的行为实现分别位于 `config.rs` 与 `runtime.rs`；本文件只重导出
//! 恢复出的主要数据类型，避免复制文件所有权状态，同时保留逐文件符号映射。

pub use super::config::{FilesystemConfig, NetworkConfig, SrtConfig};
pub use super::runtime::SandboxRuntime;
