//! 会话管理器、技能包准备和跨子系统生命周期编排。
//!
//! 本目录机械对应 Go `internal/manager`。Tokio 主状态机编排 environment、MCP、plugin、
//! lease、Claude executor 与 tunnel；技能 ZIP 和 symlink 仍保持独立的非致命准备步骤。

// 保留 Go `internal/manager/manager.go` 的文件基名，因此允许与父模块同名。
#[allow(clippy::module_inception)]
mod manager;
mod mcp;
mod skill_extraction;
mod skill_symlink;
mod tunnel_register;
mod zz_recovered_types;

pub use manager::{
    is_tunnel_enabled, new_manager_with_executor_and_claude_config, run_init_only,
    wait_for_shutdown_signal,
};
pub use skill_extraction::extract_skill_zips;
pub use skill_symlink::link_workspace_skills_dir_to_claude_dir;
pub use zz_recovered_types::{
    ClaudeExecutorFactory, CreateTunnelClientFunc, EnvironmentLeaseAdapter, Manager,
    ManagerApiClient, ManagerApiClientAdapter, ManagerError, ManagerLeaseRequest,
    ManagerLeaseRuntime, ManagerObservability, TunnelClient,
};

#[cfg(test)]
#[path = "manager_test.rs"]
mod manager_test;

#[cfg(test)]
#[path = "skill_extraction_test.rs"]
mod skill_extraction_test;

#[cfg(test)]
#[path = "skill_symlink_test.rs"]
mod skill_symlink_test;
