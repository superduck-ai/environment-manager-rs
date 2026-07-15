//! Anthropic 环境的配置、初始化、内置技能和语言安装脚本。
//!
//! 目录逐文件对应 Go `internal/envtype/anthropic`。源码处理复用独立 `sources`
//! 子系统；临时脚本和 Git 外部命令使用 Tokio，并保持取消只终止直接子进程。

// 保留 Go `anthropic/anthropic.go` 到 Rust `anthropic/anthropic.rs` 的机械映射。
#[allow(clippy::module_inception)]
mod anthropic;
mod config;
mod embedded;
pub mod install_scripts;
mod zz_recovered_types;

pub use anthropic::{REGISTRATION, new};
pub use config::decode_config;
pub use zz_recovered_types::{
    AnthropicEnvironmentType, AnthropicError, Config, Language, RunScriptError,
};

#[cfg(test)]
mod anthropic_test;
