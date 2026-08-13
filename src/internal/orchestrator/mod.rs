//! 环境 work 轮询与单 session 调度。
//!
//! 取消由 Tokio `CancellationToken` 表达，I/O 接口使用 async 方法，构造函数采用
//! snake_case。

mod hooks;
// 模块同名是刻意保留的 crate 内边界。
#[allow(clippy::module_inception)]
mod orchestrator;
mod poll_hook;
mod poller;
mod types;
mod whoami;

pub(crate) use hooks::{
    log_command_wrapped_with_sandbox, log_sandbox_cleanup_failed, run_command_capture,
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
