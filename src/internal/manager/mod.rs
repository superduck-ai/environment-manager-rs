//! 会话管理器与跨子系统生命周期编排。
//!
//! Tokio 主状态机编排 environment、MCP、plugin、lease、Claude executor 与 tunnel。

// 允许实现模块与父模块同名。
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
