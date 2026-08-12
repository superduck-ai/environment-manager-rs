//! Baku 草稿快照 action 实现。
//!
//! 对应 Go `internal/tunnel/actions/snapshot/action.go`。文件读取采用 `O_NOFOLLOW`、
//! canonical path containment、单文件 512 KiB 与总计 5 MiB 限制，防止控制请求越界
//! 读取 project 之外的内容；Git 元数据继续通过外部 `git` 命令取得。

use std::fs::OpenOptions;
use std::io::Read as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{LogAttributes, Logger, default_slog_logger};
use crate::internal::tunnel::actions::{
    Action, ActionError, ActionRequest, ActionResult, ProgressReporter,
};

use super::types::{FileEntry, SnapshotAction, SnapshotResponse};

/// 对应 Go `snapshotTimeout`：快照 action 最多运行三十秒。
pub const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(30);
/// 对应 Go `perFileCap`：单文件最多读取 512 KiB。
pub(super) const PER_FILE_CAP: u64 = 512 * 1024;
/// 对应 Go `totalCap`：草稿和探索文件合计最多读取 5 MiB。
pub(super) const TOTAL_CAP: usize = 5 * 1024 * 1024;

/// 对应 Go：`snapshot.NewSnapshotAction`。
///
/// 固定路径仅面向唯一支持的 Linux 容器布局；logger 为 `None` 时回退到进程默认
/// logger，复刻 Go `(*SnapshotAction).log` 的行为。
#[must_use]
pub fn new_snapshot_action(logger: Option<Logger>) -> SnapshotAction {
    SnapshotAction {
        logger,
        drafts_path: PathBuf::from("/home/claude/project/.baku/drafts"),
        explorations_path: PathBuf::from("/home/claude/project/.baku/explorations"),
        project_path: PathBuf::from("/home/claude/project"),
    }
}

#[async_trait]
impl Action for SnapshotAction {
    /// 对应 Go：`(*SnapshotAction).Execute`。
    ///
    /// 先解析 project 真路径，再读取受限目录，最后通过可取消子进程查询 Git。
    /// project 不存在是请求级错误；草稿目录或个别文件失败则记录并跳过。
    async fn execute(
        &self,
        cancellation: CancellationToken,
        request: ActionRequest,
        _progress: Arc<dyn ProgressReporter>,
    ) -> Result<ActionResult, ActionError> {
        let resolved_project = self.project_path.canonicalize().map_err(|error| {
            format!(
                "failed to resolve project path {}: {error}",
                self.project_path.display()
            )
        })?;

        let (drafts, draft_bytes, drafts_truncated) =
            self.read_dir(&cancellation, &self.drafts_path, &resolved_project, 0);
        let (explorations, exploration_bytes, explorations_truncated) = self.read_dir(
            &cancellation,
            &self.explorations_path,
            &resolved_project,
            draft_bytes,
        );
        let commit_count = self.git_commit_count(&cancellation).await;
        let app_modified = self.git_app_modified(&cancellation, commit_count).await;
        let response = SnapshotResponse {
            drafts,
            explorations,
            has_built_content: commit_count > 1,
            commit_count,
            truncated: drafts_truncated || explorations_truncated,
            app_modified,
        };

        let logger = self.log();
        logger.info(
            "snapshot_complete",
            values_to_attributes([
                ("session_id", serde_json::Value::String(request.session_id)),
                (
                    "drafts_count",
                    serde_json::Value::from(response.drafts.len()),
                ),
                (
                    "explorations_count",
                    serde_json::Value::from(response.explorations.len()),
                ),
                (
                    "files_cached",
                    serde_json::Value::from(draft_bytes + exploration_bytes),
                ),
                (
                    "commit_count",
                    serde_json::Value::from(response.commit_count),
                ),
                ("truncated", serde_json::Value::Bool(response.truncated)),
                (
                    "app_modified",
                    serde_json::Value::Bool(response.app_modified),
                ),
            ]),
        );

        Ok(ActionResult {
            status_code: 200,
            body: serde_json::to_vec(&response)?,
        })
    }

    /// 对应 Go：`(*SnapshotAction).Name`。
    fn name(&self) -> &str {
        "snapshot"
    }

    /// 对应 Go：`(*SnapshotAction).Timeout`。
    fn timeout(&self) -> Duration {
        SNAPSHOT_TIMEOUT
    }
}

impl SnapshotAction {
    /// 对应 Go：`(*SnapshotAction).readDir`。
    ///
    /// 只接受 canonical 后仍位于 project 内的目录；目录条目按文件名排序以复刻
    /// `os.ReadDir`，并在两类目录共享的总字节上限达到时标记截断。
    pub(super) fn read_dir(
        &self,
        _cancellation: &CancellationToken,
        directory: &Path,
        resolved_project: &Path,
        start_bytes: usize,
    ) -> (Vec<FileEntry>, usize, bool) {
        let resolved_directory = match directory.canonicalize() {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return (Vec::new(), 0, false);
            }
            Err(error) => {
                let logger = self.log();
                logger.warn(
                    "snapshot_resolve_error",
                    values_to_attributes([
                        (
                            "dir",
                            serde_json::Value::String(directory.display().to_string()),
                        ),
                        ("error", serde_json::Value::String(error.to_string())),
                    ]),
                );
                return (Vec::new(), 0, false);
            }
        };
        if resolved_directory.strip_prefix(resolved_project).is_err() {
            let logger = self.log();
            logger.warn(
                "snapshot_dir_escapes_project",
                values_to_attributes([
                    (
                        "dir",
                        serde_json::Value::String(directory.display().to_string()),
                    ),
                    (
                        "resolved_dir",
                        serde_json::Value::String(resolved_directory.display().to_string()),
                    ),
                    (
                        "project_path",
                        serde_json::Value::String(resolved_project.display().to_string()),
                    ),
                ]),
            );
            return (Vec::new(), 0, false);
        }

        let mut directory_entries = match std::fs::read_dir(&resolved_directory) {
            Ok(entries) => entries.filter_map(Result::ok).collect::<Vec<_>>(),
            Err(error) => {
                let logger = self.log();
                logger.warn(
                    "snapshot_readdir_error",
                    values_to_attributes([
                        (
                            "dir",
                            serde_json::Value::String(resolved_directory.display().to_string()),
                        ),
                        ("error", serde_json::Value::String(error.to_string())),
                    ]),
                );
                return (Vec::new(), 0, false);
            }
        };
        directory_entries.sort_by_key(std::fs::DirEntry::file_name);

        let mut entries = Vec::new();
        let mut bytes_read = 0usize;
        for entry in directory_entries {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }
            if start_bytes.saturating_add(bytes_read) >= TOTAL_CAP {
                return (entries, bytes_read, true);
            }
            let Some(content) = self.read_file_safe(&resolved_directory, &entry.file_name()) else {
                continue;
            };
            bytes_read = bytes_read.saturating_add(content.len());
            entries.push(FileEntry {
                filename: entry.file_name().to_string_lossy().into_owned(),
                content: String::from_utf8_lossy(&content).into_owned(),
            });
        }
        (entries, bytes_read, false)
    }

    /// 对应 Go：`(*SnapshotAction).readFileSafe`。
    ///
    /// 使用 `O_NOFOLLOW` 原子拒绝最后一个 path component 为 symlink 的竞态，随后
    /// 验证打开后的 fd 是普通文件且不超过单文件上限，再从同一 fd 读取内容。
    pub(super) fn read_file_safe(
        &self,
        directory: &Path,
        name: &std::ffi::OsStr,
    ) -> Option<Vec<u8>> {
        let path = directory.join(name);
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
        {
            Ok(file) => file,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(libc::ELOOP) =>
            {
                return None;
            }
            Err(error) => {
                let logger = self.log();
                logger.warn(
                    "snapshot_open_error",
                    values_to_attributes([
                        (
                            "path",
                            serde_json::Value::String(path.display().to_string()),
                        ),
                        ("error", serde_json::Value::String(error.to_string())),
                    ]),
                );
                return None;
            }
        };
        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                let logger = self.log();
                logger.warn(
                    "snapshot_stat_error",
                    values_to_attributes([
                        (
                            "path",
                            serde_json::Value::String(path.display().to_string()),
                        ),
                        ("error", serde_json::Value::String(error.to_string())),
                    ]),
                );
                return None;
            }
        };
        if !metadata.is_file() {
            return None;
        }
        if metadata.len() > PER_FILE_CAP {
            let logger = self.log();
            logger.warn(
                "snapshot_file_oversized",
                values_to_attributes([
                    (
                        "path",
                        serde_json::Value::String(path.display().to_string()),
                    ),
                    ("size", serde_json::Value::from(metadata.len())),
                    ("max_size", serde_json::Value::from(PER_FILE_CAP)),
                ]),
            );
            return None;
        }

        let mut content = Vec::with_capacity(metadata.len() as usize);
        if let Err(error) = file.read_to_end(&mut content) {
            let logger = self.log();
            logger.warn(
                "snapshot_read_error",
                values_to_attributes([
                    (
                        "path",
                        serde_json::Value::String(path.display().to_string()),
                    ),
                    ("error", serde_json::Value::String(error.to_string())),
                ]),
            );
            return None;
        }
        Some(content)
    }

    /// 对应 Go：`(*SnapshotAction).gitCommitCount`。
    ///
    /// 运行 `git rev-list --count HEAD`；Git 不可用、取消或输出非法时兼容地返回零。
    async fn git_commit_count(&self, cancellation: &CancellationToken) -> i32 {
        let project = self.project_path.to_string_lossy();
        let arguments = ["-C", project.as_ref(), "rev-list", "--count", "HEAD"];
        let output = match self.git_output(cancellation, &arguments).await {
            Ok(output) => output,
            Err(error) => {
                let logger = self.log();
                logger.warn(
                    "snapshot_git_error",
                    values_to_attributes([
                        (
                            "project_path",
                            serde_json::Value::String(self.project_path.display().to_string()),
                        ),
                        ("error", serde_json::Value::String(error)),
                    ]),
                );
                return 0;
            }
        };
        String::from_utf8_lossy(&output)
            .trim()
            .parse::<i32>()
            .unwrap_or(0)
    }

    /// 对应 Go：`(*SnapshotAction).gitAppModified`。
    ///
    /// 从仓库根提交到 HEAD 检查 `src/` 是否发生变化；不足两个提交或 Git 失败返回
    /// false，不把诊断性元数据失败升级为整个快照失败。
    async fn git_app_modified(&self, cancellation: &CancellationToken, commit_count: i32) -> bool {
        if commit_count <= 1 {
            return false;
        }
        let project = self.project_path.to_string_lossy();
        let root_arguments = [
            "-C",
            project.as_ref(),
            "rev-list",
            "--max-parents=0",
            "HEAD",
        ];
        let root_output = match self.git_output(cancellation, &root_arguments).await {
            Ok(output) => output,
            Err(error) => {
                let logger = self.log();
                logger.warn(
                    "snapshot_git_root_error",
                    values_to_attributes([
                        (
                            "project_path",
                            serde_json::Value::String(self.project_path.display().to_string()),
                        ),
                        ("error", serde_json::Value::String(error)),
                    ]),
                );
                return false;
            }
        };
        let root_commit = String::from_utf8_lossy(&root_output).trim().to_owned();
        let range = format!("{root_commit}..HEAD");
        let diff_arguments = [
            "-C",
            project.as_ref(),
            "diff",
            "--name-only",
            range.as_str(),
            "--",
            "src/",
        ];
        let diff_output = match self.git_output(cancellation, &diff_arguments).await {
            Ok(output) => output,
            Err(error) => {
                let logger = self.log();
                logger.warn(
                    "snapshot_git_diff_error",
                    values_to_attributes([
                        (
                            "project_path",
                            serde_json::Value::String(self.project_path.display().to_string()),
                        ),
                        ("error", serde_json::Value::String(error)),
                    ]),
                );
                return false;
            }
        };
        !String::from_utf8_lossy(&diff_output).trim().is_empty()
    }

    /// 运行一个只捕获 stdout 的可取消 Git 子进程。
    ///
    /// `kill_on_drop` 确保 Registry 超时导致 future 被丢弃时直接子进程被终止；不创建
    /// 或终止进程组，保持 Go 当前只处理直接子进程的语义。
    async fn git_output(
        &self,
        cancellation: &CancellationToken,
        arguments: &[&str],
    ) -> Result<Vec<u8>, String> {
        let mut command = Command::new("git");
        command
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let child = command.spawn().map_err(|error| error.to_string())?;
        tokio::select! {
            // 丢弃 `wait_with_output` future 会触发 `kill_on_drop`，只终止直接子进程；
            // 返回文本匹配 Go `exec.CommandContext` 被取消后常见的 `ExitError`。
            () = cancellation.cancelled() => Err("signal: killed".to_owned()),
            output = child.wait_with_output() => {
                let output = output.map_err(|error| error.to_string())?;
                if output.status.success() {
                    Ok(output.stdout)
                } else {
                    Err(git_exit_error(&output.status))
                }
            }
        }
    }

    /// 返回快照 action 配置的 logger；缺省时严格回退到 Go 的 `slog.Default()`。
    fn log(&self) -> &Logger {
        match self.logger.as_ref() {
            Some(logger) => logger,
            None => default_slog_logger(),
        }
    }
}

/// 把 Unix 子进程终态格式化为 Go `os/exec.ExitError.Error()` 的常见文本。
///
/// 普通非零退出使用 `exit status N`；信号终止保留常见 Unix signal 名称。该文本只
/// 进入诊断日志，不改变 snapshot 的容错返回值。
fn git_exit_error(status: &ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    match status.signal() {
        Some(libc::SIGKILL) => "signal: killed".to_owned(),
        Some(libc::SIGTERM) => "signal: terminated".to_owned(),
        Some(libc::SIGINT) => "signal: interrupt".to_owned(),
        Some(libc::SIGPIPE) => "signal: broken pipe".to_owned(),
        Some(signal) => format!("signal {signal}"),
        None => "exit status unknown".to_owned(),
    }
}

/// 将定长日志属性数组转成确定顺序 map。
fn values_to_attributes<const N: usize>(values: [(&str, serde_json::Value); N]) -> LogAttributes {
    values
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}
