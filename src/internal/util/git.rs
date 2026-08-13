//! Git 引用维护工具。
//!
//! 通过外部 `git update-ref` 更新 remote-tracking ref，并通过 `tracing` 记录
//! 结构化事件。命令执行与取消统一交给共享 Tokio 进程层。

use std::path::{Path, PathBuf};

use thiserror::Error;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::internal::process::{CaptureMode, CommandFailure, CommandOutput, run_command};

/// 更新 remote-tracking ref 时的错误。
#[derive(Debug, Error)]
#[error("failed to update remote tracking branch {reference}: {error}")]
pub struct GitRefError {
    /// `refs/remotes/<remote>/<branch>` 形式的目标引用。
    reference: String,
    /// 启动、等待、取消或非零退出的 Rust 原生文本。
    error: String,
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
/// Warn。失败日志中的 `output` 保留原始换行和 stdout/stderr 交错。
pub async fn update_remote_tracking_branch(
    cancellation: &CancellationToken,
    repo_path: impl AsRef<Path>,
    remote_name: &str,
    branch_name: &str,
    revision: &str,
) -> Result<(), GitRefError> {
    let repo_path: PathBuf = repo_path.as_ref().to_owned();
    let reference = format!("refs/remotes/{remote_name}/{branch_name}");

    let result = run_update_ref(cancellation, &repo_path, &reference, revision).await;
    let (error, combined) = match result {
        Ok(output) if output.status.success() => {
            tracing::info!(reference, revision, "Updated remote tracking branch");
            return Ok(());
        }
        Ok(output) => (output.status.to_string(), output.output.expect_combined()),
        Err(failure) => {
            let error = failure.to_string();
            (error, failure.output.expect_combined())
        }
    };
    tracing::warn!(
        repo_path = %repo_path.display(),
        remote = remote_name,
        branch = branch_name,
        revision,
        output = %String::from_utf8_lossy(&combined),
        error,
        "Failed to update remote tracking branch"
    );
    Err(GitRefError { reference, error })
}

/// 执行单次 `git update-ref` 并捕获两个输出流的交错顺序。
///
/// # 参数
/// - `cancellation`：控制直接子进程生命周期的取消信号。
/// - `repo_path`：子进程的 current directory。
/// - `reference`：要更新的完整 Git ref。
/// - `revision`：ref 的新目标。
///
/// # 返回
/// 命令成功时返回合并输出原始字节；失败时同时返回错误文本与失败前输出。
///
/// # 副作用
/// 创建共享 stdout/stderr pipe 和一个 `git` 子进程；取消时发送 SIGKILL 并 wait。
async fn run_update_ref(
    cancellation: &CancellationToken,
    repo_path: &Path,
    reference: &str,
    revision: &str,
) -> Result<CommandOutput, CommandFailure> {
    let mut command = Command::new("git");
    command
        .args(["update-ref", reference, revision])
        .current_dir(repo_path);
    run_command(command, cancellation, None, None, CaptureMode::Combined).await
}

#[cfg(test)]
#[path = "git_test.rs"]
mod git_test;
