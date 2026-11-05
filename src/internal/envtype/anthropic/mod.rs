//! Anthropic 环境的配置、初始化和语言安装脚本。
//!
//! 源码处理复用独立 `sources` 子系统；临时脚本和 Git 外部命令使用 Tokio，并保持取消
//! 只终止直接子进程。

// 允许实现模块与父模块同名。
#[allow(clippy::module_inception)]
mod anthropic;
mod config;
pub mod install_scripts;
mod types;

pub use anthropic::{REGISTRATION, new};
pub use config::decode_config;
pub use types::{AnthropicEnvironmentType, AnthropicError, Config, Language, RunScriptError};

#[cfg(test)]
mod anthropic_test;
