//! Claude Code 安装、初始化与会话执行层。
//!
//! 子进程由 Tokio 与取消令牌管理。所有取消路径只终止直接子进程，不创建或清理进程组；
//! token 仍通过从 3 开始的继承文件描述符传递，避免进入 argv 或普通环境变量。

mod agent_proxy;
mod claude_code_executor;
mod config_helpers;
mod init;
mod install;
mod outcomes;
mod session_urls;
mod types;

pub use claude_code_executor::new_claude_code_executor;
pub use config_helpers::get_claude_path;
pub use init::{run_init, run_init_with_observability};
pub use install::{
    install_or_update_claude_code, install_or_update_claude_code_with_observability,
};
pub use outcomes::Outcomes;
pub use session_urls::build_session_urls;
pub use types::{
    ClaudeCodeExecutor, ClaudeError, ClaudeObservability, Executor, McpServerRegistration,
};

#[cfg(test)]
#[path = "claude_code_executor_test.rs"]
mod claude_code_executor_test;

#[cfg(test)]
#[path = "install_outcomes_test.rs"]
mod install_outcomes_test;

#[cfg(test)]
#[path = "process_edge_test.rs"]
mod process_edge_test;
