//! 对应 Go `internal/util/git_test.go` 的 Git 引用兼容测试。
//!
//! 测试使用临时真实仓库验证 `git update-ref` 的成功路径和错误包装；仅在权威
//! Linux ARM64 测试机执行，避免依赖编辑主机上的 Git 行为。

use std::{
    path::Path,
    process::Command as StdCommand,
    sync::{Arc, Mutex},
};

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger,
};

use super::update_remote_tracking_branch;

/// 保存 util Git 测试产生的完整结构化记录，便于校验消息与属性顺序。
#[derive(Clone, Default)]
struct RecordingHandler {
    records: Arc<Mutex<Vec<LogRecord>>>,
}

impl LogHandler for RecordingHandler {
    /// Go 测试中的 TextHandler 默认启用 INFO 及以上级别。
    fn enabled(&self, level: LogLevel) -> bool {
        level >= LogLevel::Info
    }

    /// 按调用顺序保存记录，不修改属性或 source。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(record.clone());
        Ok(())
    }

    /// 测试路径不会绑定属性；仍共享同一个记录容器以满足 handler 接口。
    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }

    /// 测试路径不会创建 group；仍共享同一个记录容器以满足 handler 接口。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 创建 logger，并返回可在调用结束后读取的记录容器。
fn recording_logger() -> (Logger, Arc<Mutex<Vec<LogRecord>>>) {
    let handler = RecordingHandler::default();
    let records = Arc::clone(&handler.records);
    (Logger::new(Arc::new(handler)), records)
}

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
    let (logger, records) = recording_logger();

    update_remote_tracking_branch(
        &CancellationToken::new(),
        Some(&logger),
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

    let records = records
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].level, LogLevel::Info);
    assert_eq!(records[0].message, "Updated remote tracking branch");
    assert_eq!(
        records[0]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["ref", "revision"]
    );
    assert_eq!(
        records[0].attributes["ref"],
        serde_json::json!("refs/remotes/origin/main")
    );
    assert_eq!(records[0].attributes["revision"], serde_json::json!("HEAD"));
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
    let (logger, records) = recording_logger();
    let error = update_remote_tracking_branch(
        &CancellationToken::new(),
        Some(&logger),
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

    let records = records
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].level, LogLevel::Warn);
    assert_eq!(
        records[0].message,
        "Failed to update remote tracking branch"
    );
    assert_eq!(
        records[0]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [
            "repo_path",
            "remote",
            "branch",
            "revision",
            "output",
            "error"
        ]
    );
    assert_eq!(
        records[0].attributes["repo_path"],
        serde_json::json!(temp.path().to_string_lossy())
    );
    assert_eq!(records[0].attributes["remote"], serde_json::json!("origin"));
    assert_eq!(records[0].attributes["branch"], serde_json::json!("main"));
    assert_eq!(
        records[0].attributes["revision"],
        serde_json::json!("missing-revision")
    );
    assert_eq!(
        records[0].attributes["error"],
        serde_json::json!("exit status 128")
    );
    let output = records[0].attributes["output"].as_str().unwrap();
    assert!(output.contains("missing-revision"));
    assert!(
        output.ends_with('\n'),
        "CombinedOutput 的尾随换行不得被 trim"
    );
}

/// 对应 Go `CommandContext` 在启动前 context 已取消时的兼容分支。
///
/// 预取消不能创建 Git 子进程，但仍必须像 Go 一样记录 Warn，并把空 output 与
/// `context canceled` 同时写入日志和外层 `%w` 错误。
#[tokio::test]
async fn test_update_remote_tracking_branch_logs_pre_cancelled_error() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let (logger, records) = recording_logger();

    let error = update_remote_tracking_branch(
        &cancellation,
        Some(&logger),
        "/path/does/not/need/to/exist",
        "origin",
        "main",
        "HEAD",
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "failed to update remote tracking branch refs/remotes/origin/main: context canceled"
    );
    let records = records
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].level, LogLevel::Warn);
    assert_eq!(records[0].attributes["output"], serde_json::json!(""));
    assert_eq!(
        records[0].attributes["error"],
        serde_json::json!("context canceled")
    );
}
