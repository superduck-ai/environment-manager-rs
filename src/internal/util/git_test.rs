//! 对应 Go `internal/util/git_test.go` 的 Git 引用兼容测试。
//!
//! 测试使用临时真实仓库验证 `git update-ref` 的成功路径和错误包装；仅在权威
//! Linux ARM64 测试机执行，避免依赖编辑主机上的 Git 行为。

use std::{path::Path, process::Command as StdCommand};

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use super::update_remote_tracking_branch;

/// 对应 Go：`git_test.go` 中的 `runGitCommand`。
///
/// 在指定测试仓库中执行同步 Git 命令，并返回去除首尾空白的标准输出。命令失败
/// 会立即终止当前测试，同时保留 Git stderr 便于定位 fixture 初始化问题。
fn run_git_command(repo: &Path, args: &[&str]) -> String {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .expect("Linux ARM64 测试镜像必须提供 git");
    assert!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// 对应 Go：`git_test.go` 中的 `TestUpdateRemoteTrackingBranch`。
///
/// 创建最小仓库后更新 remote-tracking ref，并逐字比较它与 `HEAD` 的对象 ID。
#[tokio::test]
async fn test_update_remote_tracking_branch() {
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    if StdCommand::new("git").arg("--version").output().is_err() {
        return;
    }
    let temp = TempDir::new().unwrap();
    run_git_command(temp.path(), &["init"]);
    run_git_command(temp.path(), &["config", "user.email", "test@example.com"]);
    run_git_command(temp.path(), &["config", "user.name", "Test User"]);
    std::fs::write(temp.path().join("README.md"), "# test\n").unwrap();
    run_git_command(temp.path(), &["add", "."]);
    run_git_command(temp.path(), &["commit", "-m", "initial"]);
    update_remote_tracking_branch(
        &CancellationToken::new(),
        temp.path(),
        "origin",
        "main",
        "HEAD",
    )
    .await
    .unwrap();

    assert_eq!(
        run_git_command(temp.path(), &["rev-parse", "HEAD"]),
        run_git_command(temp.path(), &["rev-parse", "refs/remotes/origin/main"],)
    );
}

/// 对应 Go：`git_test.go` 中的 `TestUpdateRemoteTrackingBranchWrapsGitError`。
///
/// 使用不存在的 revision 触发 Git 失败，并验证错误文本包含 Go 版本相同的目标
/// remote-tracking 引用，确保上层日志仍能直接定位失败分支。
#[tokio::test]
async fn test_update_remote_tracking_branch_wraps_git_error() {
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    if StdCommand::new("git").arg("--version").output().is_err() {
        return;
    }
    let temp = TempDir::new().unwrap();
    run_git_command(temp.path(), &["init"]);
    let error = update_remote_tracking_branch(
        &CancellationToken::new(),
        temp.path(),
        "origin",
        "main",
        "missing-revision",
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed to update remote tracking branch refs/remotes/origin/main")
    );
}
