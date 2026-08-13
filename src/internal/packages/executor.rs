//! 固定 package-manager argv 的 Linux 子进程执行器。
//!
//! 每次调用使用 `/home/user` 工作目录、丢弃
//! stdout/stderr，并创建独立进程组；取消时终止整个组，避免 package manager 留下
//! 下载器或安装脚本。进程输出和错误正文都不会进入协议结果或日志。

use std::{io, path::PathBuf, process::Stdio, time::Duration};

use async_trait::async_trait;
use nix::{
    errno::Errno,
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use std::os::unix::process::ExitStatusExt as _;
use tokio::process::Command as TokioCommand;

use super::application::{Command, CommandResult, CommandRunner, ProvisionContext};

/// 取消后等待直接子进程被回收的固定预算。
const PROCESS_WAIT_DELAY: Duration = Duration::from_secs(2);

/// 使用 Tokio 启动真实 Linux package manager 的 executor。
pub(super) struct ExecCommandRunner {
    work_dir: PathBuf,
}

impl ExecCommandRunner {
    /// 创建使用指定工作目录的 executor；生产调用固定传入 `/home/user`。
    #[must_use]
    pub(super) fn new(work_dir: impl Into<PathBuf>) -> Self {
        Self {
            work_dir: work_dir.into(),
        }
    }
}

#[async_trait]
impl CommandRunner for ExecCommandRunner {
    /// 执行一个无 shell 的固定 argv。
    ///
    /// # 副作用
    /// 创建独立 Linux 进程组并把两个输出流连接到 `/dev/null`；取消时向负 PGID 发送
    /// `SIGKILL`。`kill_on_drop` 只作为 panic/任务 abort 的最后保护，正常路径总会 wait。
    async fn run(&self, context: &ProvisionContext, command: &Command) -> CommandResult {
        let mut process = TokioCommand::new(&command.path);
        process
            .args(&command.args)
            .current_dir(&self.work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true);

        let mut child = match process.spawn() {
            Ok(child) => child,
            Err(error) => {
                return CommandResult {
                    exit_code: 0,
                    started: false,
                    signaled: false,
                    error: Some(error),
                };
            }
        };
        let process_id = child.id().and_then(|id| i32::try_from(id).ok());

        tokio::select! {
                   status = child.wait() => {
                       let mut result = completed_command_result(status);
        // 在 Wait 返回后再次检查 ctx.Err，并用取消原因覆盖原结果。两者
        // 同时就绪时不能让 Tokio select 的随机分支把已取消安装误报成成功。
                       if context.is_cancelled() {
                           result.error = Some(cancelled_error());
                       }
                       result
                   },
                   () = context.cancelled() => {
                       let kill_error = process_id.and_then(kill_process_group);
                       match tokio::time::timeout(PROCESS_WAIT_DELAY, child.wait()).await {
                           Ok(status) => {
                               let mut result = completed_command_result(status);
        // CommandContext 在 context 完成后用 ctx.Err 覆盖 Wait 错误；
        // 应用层随后优先按 context 分类，所以这里只需确保 error 非空。
                               result.error = Some(kill_error.unwrap_or_else(cancelled_error));
                               result
                           }
                           Err(_) => CommandResult {
                               exit_code: -1,
                               started: true,
                               signaled: true,
                               error: Some(kill_error.unwrap_or_else(|| {
                                   io::Error::new(io::ErrorKind::TimedOut, "package manager wait timed out")
                               })),
                           },
                       }
                   }
               }
    }
}

/// 把 Tokio wait 结果压缩成不含子进程输出的应用层状态。
fn completed_command_result(status: io::Result<std::process::ExitStatus>) -> CommandResult {
    match status {
        Ok(status) => CommandResult {
            exit_code: status.code().unwrap_or(-1),
            started: true,
            signaled: process_was_signaled(&status),
            error: (!status.success())
                .then(|| io::Error::other(format!("package manager exited with status {status}"))),
        },
        Err(error) => CommandResult {
            exit_code: 0,
            started: true,
            signaled: false,
            error: Some(error),
        },
    }
}

/// 判断 Linux wait status 是否由信号结束。
///
/// `ExitStatusExt::signal` 只在 Unix 可用；本项目支持 Linux GNU 的任意 CPU
/// 架构，因此这里不增加其他操作系统的退化分支。
#[must_use]
fn process_was_signaled(status: &std::process::ExitStatus) -> bool {
    status.signal().is_some()
}

/// 向以 direct child PID 为 PGID 的整个 package manager 进程组发送 `SIGKILL`。
fn kill_process_group(process_id: i32) -> Option<io::Error> {
    match kill(Pid::from_raw(-process_id), Signal::SIGKILL) {
        Ok(()) | Err(Errno::ESRCH) => None,
        Err(error) => Some(io::Error::from_raw_os_error(error as i32)),
    }
}

/// 创建不包含用户输入的取消占位错误。
fn cancelled_error() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "package provisioning cancelled")
}
