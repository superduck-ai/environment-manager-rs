//! 子进程和临时脚本执行。
//!
//! 本模块对应 Go 的 `internal/process`，使用 Tokio 同时读取 stdout/stderr，
//! 并通过取消 token 复现 `exec.CommandContext` 在上层取消时终止进程的行为。

// 保留 Go `process.go` 到 Rust `process.rs` 的逐文件映射，因此有意允许同名子模块。
#[allow(clippy::module_inception)]
mod process;
mod script;
mod zz_recovered_types;

pub use process::execute;
pub use script::{ScriptError, execute_script};
pub use zz_recovered_types::{
    GoCommandError, ProcessError, ProcessExitError, ProcessResult, Result,
};
pub(crate) use zz_recovered_types::{go_spawn_error, resolve_go_command};

#[cfg(test)]
#[path = "process_test.rs"]
mod process_test;
