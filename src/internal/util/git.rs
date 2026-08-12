//! Git 引用维护工具。
//!
//! 本文件对应 Go 的 `internal/util/git.go`。模块只通过外部 `git update-ref` 更新
//! remote-tracking ref，并由调用方注入的 [`Logger`] 记录结构化事件；当调用方
//! 没有 logger 时，退回进程级 Rust tracing logger。
//!
//! Rust 与 Go 的结构差异仅在取消信号：Go 使用 `context.Context`，Rust 使用
//! [`CancellationToken`]。两者都只终止当前直接子进程，不创建或清理进程组。

use std::{
    os::unix::process::ExitStatusExt as _,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
};

use nix::{fcntl::OFlag, unistd::pipe2};
use thiserror::Error;
use tokio::{io::AsyncReadExt as _, process::Command};
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{LogAttributes, Logger, default_logger};

/// 更新 remote-tracking ref 时的兼容错误。
///
/// Go 使用 `%w` 包装 `exec.Cmd.CombinedOutput` 返回的错误；这里保存同样的 ref 与
/// Go 风格错误文本，使 `Display` 成为调用方可直接继续包装的稳定外部消息。
#[derive(Debug, Error)]
#[error("failed to update remote tracking branch {reference}: {error}")]
pub struct GitRefError {
    /// `refs/remotes/<remote>/<branch>` 形式的目标引用。
    reference: String,
    /// 启动、等待、取消或非零退出的 Go `os/exec` 风格文本。
    error: String,
}

/// 外部 `git update-ref` 失败以及失败前捕获的原始合并输出。
#[derive(Debug)]
struct CombinedCommandFailure {
    /// 对应 Go `CombinedOutput` 的 error 文本。
    error: String,
    /// stdout 与 stderr 共享 pipe 后得到的原始字节，包含尾随换行。
    output: Vec<u8>,
}

/// 对应 Go：`internal/util/git.go` 中的 `UpdateRemoteTrackingBranch`。
///
/// 调用系统 `git update-ref`，将 `refs/remotes/<remote>/<branch>` 原子更新到指定
/// revision。函数不通过 shell 拼接参数，因此 remote、branch 与 revision 不会被
/// shell 再解释。
///
/// # 参数
/// - `cancellation`：调用链的取消信号；预取消时不启动进程，运行中取消只 kill 并
///   回收直接 `git` 子进程。
/// - `logger`：业务结构化 logger；`None` 时使用进程级 Rust tracing logger。
/// - `repo_path`：Git 工作区目录，仅在子进程执行期间借用。
/// - `remote_name`：remote 名称，作为 ref 路径的一部分原样使用。
/// - `branch_name`：branch 名称，作为 ref 路径的一部分原样使用。
/// - `revision`：Git 可解析的目标 revision，例如 `HEAD` 或对象 ID。
///
/// # 返回
/// 更新成功返回 `Ok(())`；启动、取消、wait、pipe 读取或 Git 非零退出时，返回包含
/// `failed to update remote tracking branch <ref>` 包装的 [`GitRefError`]。
///
/// # 副作用
/// 创建一个继承当前环境的 `git` 子进程、修改仓库 ref，并分别记录成功 Info 或失败
/// Warn。失败日志中的 `output` 保留 `CombinedOutput` 原始换行和 stdout/stderr 交错。
///
/// # 兼容性
/// stdout 与 stderr 必须共享同一条 `O_CLOEXEC` pipe；不能改回两个缓冲区后拼接，
/// 否则输出交错顺序以及尾随换行都会偏离 Go。
pub async fn update_remote_tracking_branch(
    cancellation: &CancellationToken,
    logger: Option<&Logger>,
    repo_path: impl AsRef<Path>,
    remote_name: &str,
    branch_name: &str,
    revision: &str,
) -> Result<(), GitRefError> {
    // 显式 match 允许 `'static` 默认引用安全缩短到调用方借用期；`unwrap_or_else`
    // 会把泛型返回值错误推导成 `'static`，进而要求传入引用逃逸函数体。
    let logger = match logger {
        Some(logger) => logger,
        None => default_logger(),
    };
    let repo_path: PathBuf = repo_path.as_ref().to_owned();
    let reference = format!("refs/remotes/{remote_name}/{branch_name}");

    match run_update_ref(cancellation, &repo_path, &reference, revision).await {
        Ok(_) => {
            let mut attributes = LogAttributes::new();
            attributes.insert("ref".to_owned(), serde_json::json!(&reference));
            attributes.insert("revision".to_owned(), serde_json::json!(revision));
            logger.info("Updated remote tracking branch", attributes);
            Ok(())
        }
        Err(failure) => {
            // 属性插入顺序必须与 Go 的 `WarnContext` 参数顺序一致；JSON handler 会
            // 保留该顺序，差分测试把它视为外部协议的一部分。
            let mut attributes = LogAttributes::new();
            attributes.insert(
                "repo_path".to_owned(),
                serde_json::json!(repo_path.to_string_lossy()),
            );
            attributes.insert("remote".to_owned(), serde_json::json!(remote_name));
            attributes.insert("branch".to_owned(), serde_json::json!(branch_name));
            attributes.insert("revision".to_owned(), serde_json::json!(revision));
            attributes.insert(
                "output".to_owned(),
                serde_json::json!(String::from_utf8_lossy(&failure.output)),
            );
            attributes.insert("error".to_owned(), serde_json::json!(&failure.error));
            logger.warn("Failed to update remote tracking branch", attributes);

            Err(GitRefError {
                reference,
                error: failure.error,
            })
        }
    }
}

/// 使用 Go `exec.Cmd.CombinedOutput` 的 pipe 结构执行单次 `git update-ref`。
///
/// # 参数
/// - `cancellation`：控制直接子进程生命周期的取消信号。
/// - `repo_path`：子进程的 current directory。
/// - `reference`：要更新的完整 Git ref。
/// - `revision`：ref 的新目标。
///
/// # 返回
/// 命令成功时返回合并输出原始字节；失败时同时返回 Go 风格错误文本与失败前输出。
///
/// # 副作用
/// 创建共享 stdout/stderr pipe 和一个 `git` 子进程；取消时发送 SIGKILL 并 wait。
async fn run_update_ref(
    cancellation: &CancellationToken,
    repo_path: &Path,
    reference: &str,
    revision: &str,
) -> Result<Vec<u8>, CombinedCommandFailure> {
    if cancellation.is_cancelled() {
        return Err(CombinedCommandFailure {
            error: "context canceled".to_owned(),
            output: Vec::new(),
        });
    }

    // `pipe2(O_CLOEXEC)` 必须原子完成。若先 pipe 再 fcntl，并发 spawn 可能短暂
    // 继承写端，导致 git 已退出但 reader 仍观察不到 EOF。
    let (read_end, write_end) =
        pipe2(OFlag::O_CLOEXEC).map_err(|error| CombinedCommandFailure {
            error: error.to_string(),
            output: Vec::new(),
        })?;
    let stderr_end = write_end
        .try_clone()
        .map_err(|error| CombinedCommandFailure {
            error: error.to_string(),
            output: Vec::new(),
        })?;

    let mut command = Command::new("git");
    command
        .args(["update-ref", reference, revision])
        .current_dir(repo_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(write_end))
        .stderr(Stdio::from(stderr_end))
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| CombinedCommandFailure {
        error: format_start_error(&error),
        output: Vec::new(),
    })?;
    // `Command` 不再需要，立即释放其父进程侧 Stdio 状态，确保 reader 最终能 EOF。
    drop(command);

    let mut reader = tokio::fs::File::from_std(std::fs::File::from(read_end));
    let reader_task = tokio::spawn(async move {
        let mut output = Vec::new();
        let result = reader.read_to_end(&mut output).await;
        (output, result)
    });

    let status = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            // Go `CommandContext` 的默认 Cancel 只 kill direct child；不得扩大到进程组。
            let _direct_child_kill = child.start_kill();
            child.wait().await
        }
        result = child.wait() => result,
    };
    let reader_result = reader_task.await;

    // Go `Cmd.Wait` 优先返回进程退出错误，只有进程成功时才暴露 pipe copy 错误。
    if let Ok(status) = status.as_ref()
        && !status.success()
    {
        let output = reader_result.map_or_else(|_| Vec::new(), |(output, _)| output);
        return Err(CombinedCommandFailure {
            error: format_exit_status(*status),
            output,
        });
    }
    if let Err(error) = status {
        let output = reader_result.map_or_else(|_| Vec::new(), |(output, _)| output);
        return Err(CombinedCommandFailure {
            error: error.to_string(),
            output,
        });
    }

    match reader_result {
        Ok((output, Ok(_))) => Ok(output),
        Ok((output, Err(error))) => Err(CombinedCommandFailure {
            error: error.to_string(),
            output,
        }),
        Err(error) => Err(CombinedCommandFailure {
            error: error.to_string(),
            output: Vec::new(),
        }),
    }
}

/// 把 `git` 的 Linux spawn 失败收敛为 Go `os/exec` 常见文本。
///
/// # 参数
/// - `error`：Tokio 启动进程返回的 I/O 错误。
///
/// # 返回
/// PATH 中找不到 Git 时返回 Go `LookPath` 文本；其他 errno 保留系统描述。
fn format_start_error(error: &std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::NotFound {
        return "exec: \"git\": executable file not found in $PATH".to_owned();
    }
    error.to_string()
}

/// 把 Linux wait status 格式化为 Go `exec.ExitError.Error()` 文本。
///
/// # 参数
/// - `status`：已经 wait 回收的直接子进程状态。
///
/// # 返回
/// 普通退出为 `exit status N`；常见信号使用 Go signal 名称。
fn format_exit_status(status: ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    let signal = status.signal().unwrap_or_default();
    let name = match signal {
        libc::SIGINT => "interrupt",
        libc::SIGKILL => "killed",
        libc::SIGPIPE => "broken pipe",
        libc::SIGTERM => "terminated",
        _ => return format!("signal: {signal}"),
    };
    format!("signal: {name}")
}

#[cfg(test)]
#[path = "git_test.rs"]
mod git_test;
