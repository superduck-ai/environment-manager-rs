//! 子进程和临时脚本执行。
//!
//! 使用 Tokio 统一处理子进程的捕获、流式输出、取消、超时和回收。
//! 非零退出是带 `ExitStatus` 的正常完成；只有执行基础设施失败才返回错误。

mod command;
// 保留 public process API 的文件边界，因此有意允许同名子模块。
#[allow(clippy::module_inception)]
mod process;
mod script;
mod types;

pub use command::{CaptureMode, CapturedOutput, CommandFailure, CommandFailureKind, CommandOutput};
pub(crate) use command::{run_command, terminate_and_reap};
pub use process::execute;
pub use script::{ScriptError, execute_script};
pub use types::{ProcessError, ProcessExitError, ProcessResult};

#[cfg(test)]
#[path = "process_test.rs"]
mod process_test;
