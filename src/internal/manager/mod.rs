//! 会话管理器与跨子系统生命周期编排。
//!
//! 本目录机械对应 Go `internal/manager`。Tokio 主状态机编排 environment、MCP、plugin、
//! lease、Claude executor 与 tunnel；旧 skill ZIP 解压和 symlink 准备流程已随 Go 删除。

// 保留 Go `internal/manager/manager.go` 的文件基名，因此允许与父模块同名。
#[allow(clippy::module_inception)]
mod manager;
mod mcp;
mod tunnel_register;
mod types;

pub use manager::{
    is_tunnel_enabled, new_manager_with_executor_and_claude_config, run_init_only,
    wait_for_shutdown_signal,
};
pub use types::{
    ClaudeExecutorFactory, CreateTunnelClientFunc, EnvironmentLeaseAdapter, Manager,
    ManagerApiClient, ManagerApiClientAdapter, ManagerError, ManagerLeaseRequest,
    ManagerLeaseRuntime, ManagerObservability, TunnelClient,
};

#[cfg(test)]
#[path = "manager_test.rs"]
mod manager_test;
