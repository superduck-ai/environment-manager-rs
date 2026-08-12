//! snapshot action 的目录边界、容量上限、symlink 和 Git 元数据测试。
//!
//! 对应 Go `internal/tunnel/actions/snapshot/action_test.go`。测试辅助函数保留与 Go
//! 相同的职责和机械 snake_case 名称，便于符号映射逐项审计。

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::internal::tunnel::actions::{
    Action, ActionError, ActionRequest, ActionResult, ProgressReporter, ProgressUpdate,
};

use super::action::{PER_FILE_CAP, TOTAL_CAP};
use super::{SnapshotAction, new_snapshot_action};

/// 不记录进度的 reporter；snapshot 当前没有进度事件，但 trait 参数仍需传入。
struct NoopProgress;

#[async_trait]
impl ProgressReporter for NoopProgress {
    /// 接收并忽略 snapshot 的进度事件。
    async fn send_progress(&self, _update: ProgressUpdate) -> Result<(), ActionError> {
        Ok(())
    }
}

/// 对应 Go `TestNewSnapshotActionDefaults`，锁定仅支持 Linux 的固定项目路径。
#[test]
fn test_new_snapshot_action_defaults() {
    let action = new_snapshot_action();

    assert_eq!(
        action.drafts_path,
        PathBuf::from("/home/claude/project/.baku/drafts")
    );
    assert_eq!(
        action.explorations_path,
        PathBuf::from("/home/claude/project/.baku/explorations")
    );
    assert_eq!(action.project_path, PathBuf::from("/home/claude/project"));
}

/// 对应 Go `TestSnapshotExecuteCollectsFilesAndGitMetadata`。
#[tokio::test]
async fn test_snapshot_execute_collects_files_and_git_metadata() {
    // Git 只读 PATH，但必须与并行测试的临时 PATH 写入串行，否则 fixture
    // 初始化和生产 snapshot Git 查询会随机观察到空 PATH。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    require_git();

    let temporary = tempfile::tempdir().unwrap();
    let project = temporary.path();
    write_file(project.join(".baku/drafts/draft.txt"), "draft body");
    write_file(
        project.join(".baku/explorations/explore.md"),
        "explore body",
    );
    write_file(project.join("src/app.ts"), "console.log('v1')\n");
    run_git(project, &["init"]);
    run_git(project, &["config", "user.email", "test@example.com"]);
    run_git(project, &["config", "user.name", "Test User"]);
    run_git(project, &["add", "."]);
    run_git(project, &["commit", "-m", "initial"]);
    write_file(project.join("src/app.ts"), "console.log('v2')\n");
    run_git(project, &["add", "."]);
    run_git(project, &["commit", "-m", "app update"]);

    let result = execute_snapshot(&new_test_snapshot_action(project)).await;
    assert_eq!(result.status_code, 200);

    let response: serde_json::Value = serde_json::from_slice(&result.body).unwrap();
    assert_file_entry(&response["drafts"], "draft.txt", "draft body");
    assert_file_entry(&response["explorations"], "explore.md", "explore body");
    assert_eq!(response["has_built_content"], true);
    assert_eq!(response["commit_count"], 2);
    assert_eq!(response["truncated"], false);
    assert_eq!(response["app_modified"], true);
}

/// 对应 Go `TestSnapshotExecuteReturnsProjectResolveError`。
#[tokio::test]
async fn test_snapshot_execute_returns_project_resolve_error() {
    let temporary = tempfile::tempdir().unwrap();
    let action = SnapshotAction {
        drafts_path: temporary.path().join("drafts"),
        explorations_path: temporary.path().join("explorations"),
        project_path: temporary.path().join("missing"),
    };

    let error = action
        .execute(
            CancellationToken::new(),
            ActionRequest::default(),
            Arc::new(NoopProgress),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("failed to resolve project path"));
}

/// 对应 Go `TestReadDirRejectsDirectoryOutsideProject`。
#[test]
fn test_read_dir_rejects_directory_outside_project() {
    let project = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    write_file(outside.path().join("leak.txt"), "secret");
    let resolved_project = project.path().canonicalize().unwrap();
    let action = new_test_snapshot_action(project.path());

    let (entries, bytes_read, truncated) = action.read_dir(
        &CancellationToken::new(),
        outside.path(),
        &resolved_project,
        0,
    );
    assert!(entries.is_empty());
    assert_eq!(bytes_read, 0);
    assert!(!truncated);
}

/// 对应 Go `TestReadDirTruncatesWhenTotalCapAlreadyReached`。
#[test]
fn test_read_dir_truncates_when_total_cap_already_reached() {
    let project = tempfile::tempdir().unwrap();
    let action = new_test_snapshot_action(project.path());
    write_file(action.drafts_path.join("draft.txt"), "draft body");
    let resolved_project = project.path().canonicalize().unwrap();

    let (entries, bytes_read, truncated) = action.read_dir(
        &CancellationToken::new(),
        &action.drafts_path,
        &resolved_project,
        TOTAL_CAP,
    );
    assert!(entries.is_empty());
    assert_eq!(bytes_read, 0);
    assert!(truncated);
}

/// 对应 Go `TestReadFileSafeSkipsOversizedAndSymlinkFiles`。
#[test]
fn test_read_file_safe_skips_oversized_and_symlink_files() {
    use std::os::unix::fs::symlink;

    let project = tempfile::tempdir().unwrap();
    let action = new_test_snapshot_action(project.path());
    let drafts = &action.drafts_path;
    write_file_bytes(
        drafts.join("large.txt"),
        &vec![b'x'; PER_FILE_CAP as usize + 1],
    );
    assert!(
        action
            .read_file_safe(drafts, OsStr::new("large.txt"))
            .is_none()
    );

    write_file(drafts.join("small.txt"), "small");
    symlink("small.txt", drafts.join("link.txt")).unwrap();
    assert!(
        action
            .read_file_safe(drafts, OsStr::new("link.txt"))
            .is_none()
    );
}

/// 对应 Go `executeSnapshot`，统一构造带超时语义的 action 调用。
async fn execute_snapshot(action: &SnapshotAction) -> ActionResult {
    let cancellation = CancellationToken::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        action.execute(
            cancellation,
            ActionRequest {
                session_id: "session-1".to_owned(),
                ..ActionRequest::default()
            },
            Arc::new(NoopProgress),
        ),
    )
    .await
    .expect("snapshot timeout")
    .expect("snapshot execute")
}

/// 对应 Go `assertFileEntry`，验证响应数组中唯一文件的名称和文本内容。
fn assert_file_entry(entries: &serde_json::Value, filename: &str, content: &str) {
    let entries = entries.as_array().expect("file entry array");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["filename"], filename);
    assert_eq!(entries[0]["content"], content);
}

/// 对应 Go `newTestSnapshotAction`，把固定生产路径重定向至临时项目。
fn new_test_snapshot_action(project: &Path) -> SnapshotAction {
    SnapshotAction {
        drafts_path: project.join(".baku/drafts"),
        explorations_path: project.join(".baku/explorations"),
        project_path: project.to_owned(),
    }
}

/// 对应 Go `requireGit`；权威 Linux 镜像必须提供用于兼容测试的外部 Git。
fn require_git() {
    let status = Command::new("git").arg("--version").status().unwrap();
    assert!(status.success(), "git executable is required");
}

/// 对应 Go `runGit`，在指定临时仓库运行外部 Git 并验证退出状态。
fn run_git(directory: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// 对应 Go `writeFile`，以 UTF-8 文本写入 fixture。
fn write_file(path: impl AsRef<Path>, content: &str) {
    write_file_bytes(path, content.as_bytes());
}

/// 对应 Go `writeFileBytes`，创建父目录后写入任意字节 fixture。
fn write_file_bytes(path: impl AsRef<Path>, content: &[u8]) {
    let path = path.as_ref();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}
