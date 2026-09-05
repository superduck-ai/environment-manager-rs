//! 启动上下文、源码仓库、MCP 与会话模式配置。
//!
//! 所有 wire 类型都使用 Serde；MCP 对象通过 `flatten` 保留未知字段，便于把未来后端字段
//! 无损传给下游。

pub(crate) mod git_resources;
#[path = "config.rs"]
mod implementation;
pub mod session_mode;
pub mod types;

pub use implementation::{ConfigError, default_claude_config};
pub use session_mode::{SessionMode, SessionModeParseError, parse_session_mode};
pub use types::{
    BaseSource, ClaudeConfig, GitCheckout, GitInfo, GitOutcomeInfo, GitRepositorySource, McpConfig,
    McpConfigFile, McpServerConfig, McpServerConfigTool, OutcomeField, PermissionPolicy, Session,
    Source, StartupContext,
};

#[cfg(test)]
mod config_test;
