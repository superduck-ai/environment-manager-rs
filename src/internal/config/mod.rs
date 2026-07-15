//! 启动上下文、源码仓库、MCP 与会话模式配置。
//!
//! 本模块机械对应 Go 的 `internal/config` 包。所有 wire 类型都使用 Serde，
//! MCP 对象通过 `flatten` 保留未知字段，便于把未来后端字段无损传给下游。

#[path = "config.rs"]
mod implementation;
pub mod session_mode;
pub mod zz_recovered_types;

pub use implementation::{ConfigError, default_claude_config};
pub use session_mode::{SessionMode, SessionModeParseError, parse_session_mode};
pub use zz_recovered_types::{
    BaseSource, ClaudeConfig, GitInfo, GitOutcomeInfo, GitRepositorySource, McpConfig,
    McpConfigFile, McpServerConfig, McpServerConfigTool, OutcomeField, PermissionPolicy, Session,
    Source, StartupContext,
};

#[cfg(test)]
mod config_test;
