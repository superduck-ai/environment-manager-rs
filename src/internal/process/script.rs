//! Go `internal/process/script.go` 对应的临时脚本安全创建、执行与清理。
//!
//! 临时文件先以默认 0600 创建，完整写入并 `sync_all` 后再改为 0700；
//! 因此其他用户在内容未完成时不能执行它，且脚本只对当前用户可见和可执行。

use std::{
    fs::File,
    io::{self, Write},
    os::fd::{IntoRawFd, RawFd},
    os::unix::ffi::OsStrExt as _,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use nix::unistd::close;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::internal::util::OutputStreamer;

use super::{
    ProcessError, ProcessResult, execute,
    types::{go_errno_text, go_utf8_error_view},
};

/// 临时脚本准备或执行错误。
#[derive(Debug, Error)]
pub enum ScriptError {
    /// 空脚本通常表示上游配置缺失，应在创建文件前拒绝。
    #[error("script content is empty")]
    EmptyContent,
    /// 无法安全创建临时文件。
    #[error("failed to create temp file for script: {0}")]
    Create(#[source] std::io::Error),
    /// 无法完整写入脚本。
    #[error("failed to write script to temp file: {0}")]
    Write(#[source] std::io::Error),
    /// 无法把脚本数据同步到文件系统。
    #[error("failed to sync script file: {0}")]
    Sync(#[source] std::io::Error),
    /// 无法关闭已经写完的临时脚本。
    #[error("failed to close temp file: {0}")]
    Close(#[source] std::io::Error),
    /// 无法设置 Unix 0700 权限。
    #[error("failed to make script executable: {0}")]
    Permissions(#[source] std::io::Error),
    /// 脚本已经创建，但启动执行失败。
    #[error(transparent)]
    Execute(#[from] ProcessError),
}

/// 保存临时脚本的文件句柄、路径和清理日志上下文。
///
/// Go 在 `os.CreateTemp` 成功后立即注册 `defer os.Remove`，因此写入、同步、关闭、
/// chmod 或执行任一阶段失败都必须尝试删除。本 guard 把该顺序固化在 `Drop` 中，
/// 同时避免 `tempfile` 自己静默吞掉删除错误。
struct TemporaryScript {
    file: Option<File>,
    path: PathBuf,
}

impl Drop for TemporaryScript {
    /// 关闭尚未显式关闭的文件，并按 Go defer 语义删除临时路径。
    ///
    /// 写入或同步失败时 Go 会忽略 `file.Close` 的错误后再删除；这里同样只对删除
    /// 失败写 warning。正常路径已由 `close_temporary_script_file` 显式检查 close。
    fn drop(&mut self) {
        // 必须先释放写句柄再 remove，复刻 Go 各个 early-return 分支的顺序。
        drop(self.file.take());
        if let Err(error) = std::fs::remove_file(&self.path) {
            let path_text = go_utf8_error_view(self.path.as_os_str().as_bytes());
            let removal_error = format!("remove {}: {}", path_text, go_errno_text(&error));
            tracing::warn!("path" = ?(path_text), "error" = ?(removal_error), "{}", "Failed to remove temp script file");
        }
    }
}

/// 对应 Go：`internal/process/script.go` 中的 `ExecuteScript`。
///
/// 把脚本文本写入临时可执行文件，运行后无条件删除。
///
/// `pattern` 接受 Go `CreateTemp` 常见的 `prefix*suffix` 形式；最后一个星号之前
/// 作为前缀、之后作为后缀。临时文件依次经历 write、fsync、close、chmod 和执行，
/// 顺序不可调整，否则会改变错误优先级及其他进程观察到的文件状态。
///
/// # 参数
/// - `cancellation`：上层 context 的取消令牌，仅传给 direct child 执行阶段。
/// - `content`：完整脚本字节的 UTF-8 表示；空字符串在创建文件前拒绝。
/// - `pattern`：传给临时文件命名器的 Go `CreateTemp` 兼容 pattern。
/// - `streamer`：接收脚本 stdout/stderr chunk 的同步回调。
///
/// # 返回
/// 成功启动时返回子进程结果，包括非零退出码；创建、写入、同步、关闭、chmod 或
/// spawn 失败返回对应 `ScriptError`。无论哪个阶段结束，返回前都尝试删除临时文件。
///
/// # 副作用
/// 在系统临时目录创建 0600 文件，写入并同步后关闭，将权限改为 0700，等待 10ms，
/// 创建 direct child，最后删除文件；删除失败只写 Go 同名 warning，不覆盖原返回值。
pub async fn execute_script(
    cancellation: &CancellationToken,
    content: &str,
    pattern: &str,
    streamer: OutputStreamer,
) -> Result<ProcessResult, ScriptError> {
    if content.is_empty() {
        return Err(ScriptError::EmptyContent);
    }

    let (prefix, suffix) = split_pattern(pattern);
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    if !suffix.is_empty() {
        builder.suffix(suffix);
    }
    let file = builder.tempfile().map_err(ScriptError::Create)?;
    let (file, temporary_path) = file.into_parts();
    // Linux 上 `TempPath::keep` 不执行系统调用，只关闭 tempfile 自己的静默清理；
    // 文件从此由 TemporaryScript 独占删除，才能观察并记录 Go defer 的 remove 错误。
    let path = temporary_path
        .keep()
        .map_err(|error| ScriptError::Create(error.error))?;
    let mut temporary_script = TemporaryScript {
        file: Some(file),
        path,
    };
    temporary_script
        .file
        .as_mut()
        .expect("temporary script file must exist before close")
        .write_all(content.as_bytes())
        .map_err(ScriptError::Write)?;
    temporary_script
        .file
        .as_ref()
        .expect("temporary script file must exist before close")
        .sync_all()
        .map_err(ScriptError::Sync)?;

    let script_path = temporary_script.path.clone();
    let file = temporary_script
        .file
        .take()
        .expect("temporary script file must exist before close");
    close_temporary_script_file(file, &script_path).map_err(ScriptError::Close)?;

    #[cfg(unix)]
    std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o700))
        .map_err(ScriptError::Permissions)?;

    // 保留 Go 实现中的短暂延迟，规避部分 overlay/container 文件系统的可执行可见性延迟。
    tokio::time::sleep(Duration::from_millis(10)).await;
    let result = execute(cancellation, &script_path, streamer).await;
    // 显式 drop 保证删除与其 warning 在函数返回前完成，而不是依赖局部变量隐式顺序。
    drop(temporary_script);
    result.map_err(ScriptError::from)
}

/// 显式关闭写完的临时脚本，并生成 Go `os.File.Close` 风格错误文本。
///
/// # 参数
/// - `file`：独占的脚本文件句柄，调用后不再可用。
/// - `path`：只用于错误中的 Go `PathError` 路径文本。
///
/// # 返回
/// close 成功返回 `Ok(())`；失败时保留 Linux errno，并添加 `close <path>:` 前缀。
fn close_temporary_script_file(file: File, path: &Path) -> io::Result<()> {
    let raw_fd = file.into_raw_fd();
    close_raw_temporary_script_file(raw_fd, path)
}

/// 对一个已经脱离 Rust RAII 的 raw fd 执行单次 Linux close。
///
/// 单独保留这一层是为了让失败注入测试能传入无效 fd，覆盖 Go 的 close 错误分支，
/// 而不需要构造会在 drop 时二次关闭的伪 `File`。
///
/// # 参数
/// - `raw_fd`：调用方独占且在返回后视为已消费的 Linux 文件描述符。
/// - `path`：只用于兼容 Go 错误字符串，不参与系统调用。
///
/// # 返回
/// 成功关闭返回 `Ok(())`；失败返回包含 `close <path>:` 的 `io::Error`。
fn close_raw_temporary_script_file(raw_fd: RawFd, path: &Path) -> io::Result<()> {
    close(raw_fd).map_err(|error| {
        let error = io::Error::from(error);
        let path = go_utf8_error_view(path.as_os_str().as_bytes());
        io::Error::other(format!("close {}: {}", path, go_errno_text(&error)))
    })
}

/// 把 Go `os.CreateTemp` pattern 拆分为 tempfile builder 的两个部分。
///
/// Go 只把最后一个 `*` 替换为随机串；前面的星号是文件名字面量。
/// 使用 `rsplit_once` 可避免 `a*b*c` 被错误创建成 `a<random>b*c`。
///
/// # 参数
/// - `pattern`：不含目录的 Go 临时文件命名模式。
///
/// # 返回
/// 有星号时返回最后一个星号前后的 prefix/suffix；否则整个
/// pattern 作为 prefix，suffix 为空。
pub(super) fn split_pattern(pattern: &str) -> (&str, &str) {
    pattern.rsplit_once('*').unwrap_or((pattern, ""))
}
