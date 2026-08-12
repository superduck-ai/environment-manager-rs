//! 环境 work 轮询与单 session 调度。
//!
//! 文件名和公开符号逐项对应 Go `internal/orchestrator`；同步 `context.Context` API
//! 机械映射为 Tokio `CancellationToken` + async 方法，构造函数采用 snake_case。

mod hooks;
// 保留 Go `orchestrator/orchestrator.go` 的机械文件映射，模块同名是刻意的兼容边界。
#[allow(clippy::module_inception)]
mod orchestrator;
mod poll_hook;
mod poller;
mod types;
mod whoami;

pub(crate) use hooks::{
    log_command_wrapped_with_sandbox, log_sandbox_cleanup_failed, run_command_capture,
    run_command_combined_capture,
};
pub use orchestrator::new_orchestrator;
pub use poll_hook::new_poll_hook;
pub use poller::new_poller_with_worker_id;
pub(crate) use types::SessionData;
pub use types::{
    CommandOutcome, Hook, NoopOrchestratorObserver, Orchestrator, OrchestratorConfig,
    OrchestratorError, OrchestratorEvent, OrchestratorObserver, OrchestratorResult, PollHook,
    PollHookContext, Poller, PollerInterface, RawMessage, SandboxAdapter, SandboxRequest,
    SessionOutput, StdoutSessionOutput, WhoamiClient, WhoamiResponse, WorkData, WorkResponse,
};
pub use whoami::new_whoami_client;

#[cfg(test)]
#[path = "orchestrator_test.rs"]
mod orchestrator_test;
