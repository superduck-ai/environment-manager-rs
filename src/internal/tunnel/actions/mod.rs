//! Tunnel 控制 action 注册、调度与内置实现。
//!
//! 对应 Go `internal/tunnel/actions`。Registry 保证同名 action 单实例运行，使用
//! 有界超时和取消令牌执行，并把进度及最终结果编码成与 Go 一致的 NDJSON 分片。

mod registry;
mod zz_recovered_types;

pub mod deploy;
pub mod snapshot;
pub mod status;

pub use registry::{DEFAULT_ACTION_TIMEOUT, new_registry};
pub use zz_recovered_types::{
    Action, ActionError, ActionRequest, ActionResult, ProgressReporter, ProgressUpdate, Registry,
    ResponseSender,
};

#[cfg(test)]
mod registry_test;
