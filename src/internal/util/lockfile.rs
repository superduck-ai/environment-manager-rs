//! 基于 `flock(2)` 语义的进程互斥锁。
//!
//! `fs2::FileExt::try_lock_exclusive` 在 Unix 上使用非阻塞 `flock`，与 Go 的
//! `LOCK_EX | LOCK_NB` 一致。锁通过 RAII guard 释放，避免错误路径漏掉清理闭包。

use std::{
    fmt,
    fs::{File, OpenOptions},
    io,
    os::fd::IntoRawFd,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use fs2::FileExt;
use thiserror::Error;

use super::go_path_escape;

const CONTAINER_LOCK_PATH: &str = "/tmp/environment-manager-container.lock";

/// 获取文件锁时可能发生的错误。
#[derive(Debug, Error)]
pub enum LockError {
    /// 无法创建或打开容器级锁文件。
    #[error("failed to open container lockfile: {source}")]
    ContainerOpen {
        /// 锁文件路径；仅供结构化诊断，不重复写入外层文本。
        path: PathBuf,
        /// 原始文件系统错误。
        #[source]
        source: io::Error,
    },
    /// 其他 orchestrator/session 已占用容器级锁。
    #[error("container is already in use by another orchestrator/session (lock held at {path})")]
    ContainerAlreadyHeld {
        /// 冲突的锁文件路径。
        path: PathBuf,
    },
    /// 容器级 `flock` 调用因非竞争原因失败。
    #[error("failed to acquire container lock: {source}")]
    ContainerAcquire {
        /// 锁文件路径；仅供结构化诊断。
        path: PathBuf,
        /// 原始系统错误。
        #[source]
        source: io::Error,
    },
    /// 无法创建或打开 session 锁文件。
    #[error("failed to open lockfile: {source}")]
    SessionOpen {
        /// 锁文件路径；仅供结构化诊断。
        path: PathBuf,
        /// 原始文件系统错误。
        #[source]
        source: io::Error,
    },
    /// 同一 session 已被另一个进程持有。
    #[error("session {session_id} already running (lock held)")]
    SessionAlreadyHeld {
        /// 原始 session ID，必须保留 Go 错误文本中的未转义形式。
        session_id: String,
        /// 冲突的锁文件路径；仅供结构化诊断。
        path: PathBuf,
    },
    /// session `flock` 调用因非竞争原因失败。
    #[error("failed to acquire lock: {source}")]
    SessionAcquire {
        /// 锁文件路径；仅供结构化诊断。
        path: PathBuf,
        /// 原始系统错误。
        #[source]
        source: io::Error,
    },
}

/// 不带业务上下文的底层 open/flock 失败。
///
/// 容器锁与 session 锁共用同一套系统调用，但 Go 对两者有
/// 不同的外部错误文本。该私有类型先保留失败阶段，再由入口
/// 函数映射成 [`LockError`]，避免把 Rust 的通用 path 文本泄露给 CLI。
#[derive(Debug)]
enum AcquirePathError {
    /// 打开或创建锁文件失败。
    Open { path: PathBuf, source: io::Error },
    /// 非阻塞排他锁被其他进程持有。
    AlreadyHeld { path: PathBuf },
    /// `flock` 因非 EWOULDBLOCK/EAGAIN 原因失败。
    Acquire { path: PathBuf, source: io::Error },
}

/// 已持有的文件锁。
///
/// guard 被丢弃时先解锁、再关闭文件、最后删除锁文件，顺序与 Go 清理闭包一致。
/// 锁的正确性来自打开文件描述上的 `flock`，不是锁文件是否存在。
pub struct FileLockGuard {
    file: Option<File>,
    path: PathBuf,
    scope: LockScope,
}

/// 区分两个 Go 清理闭包各自的消息、属性和级别。
#[derive(Debug)]
enum LockScope {
    /// `/tmp/environment-manager-container.lock` 的容器级互斥。
    Container,
    /// 按原始 session ID 创建的单 session 互斥。
    Session { session_id: String },
}

impl fmt::Debug for FileLockGuard {
    /// 输出锁路径和作用域。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileLockGuard")
            .field("path", &self.path)
            .field("scope", &self.scope)
            .field("is_open", &self.file.is_some())
            .finish()
    }
}

impl FileLockGuard {
    /// 返回用于诊断的锁文件路径。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 显式释放锁。
    ///
    /// 通常直接依赖 `Drop` 即可；该方法适合需要在较大作用域结束前提前释放的场景。
    pub fn release(mut self) {
        self.release_inner();
    }

    /// 执行幂等的解锁、关闭和删除流程。
    fn release_inner(&mut self) {
        if let Some(file) = self.file.take() {
            if let Err(error) = FileExt::unlock(&file) {
                self.log_unlock_error(&error);
            }
            // `File` 的 Drop 无法观察 close(2) 的返回值，而 Go 清理闭包会记录该
            // 错误。转移 fd 所有权后使用 nix 的安全封装关闭，既避免 double-close，
            // 也保留这条极少出现但外部可见的 warning。
            if let Err(error) = nix::unistd::close(file.into_raw_fd()) {
                self.log_close_error(&error);
            }
        }

        match std::fs::remove_file(&self.path) {
            Ok(()) => self.log_released(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => self.log_remove_error(&error),
        }
    }

    /// 记录 Go 清理闭包中的 `Flock(LOCK_UN)` 失败。
    fn log_unlock_error(&self, error: &dyn fmt::Display) {
        let path = self.path.display().to_string();
        match &self.scope {
            LockScope::Container => {
                tracing::warn!("lockfile_path" = ?(path), "error" = ?(error.to_string()), "{}", "Failed to unlock container lockfile")
            }
            LockScope::Session { session_id } => {
                tracing::warn!("session_id" = ?(session_id), "lockfile_path" = ?(path), "error" = ?(error.to_string()), "{}", "Failed to unlock file")
            }
        }
    }

    /// 记录 Go 清理闭包中的文件描述符关闭失败。
    fn log_close_error(&self, error: &dyn fmt::Display) {
        let path = self.path.display().to_string();
        match &self.scope {
            LockScope::Container => {
                tracing::warn!("lockfile_path" = ?(path), "error" = ?(error.to_string()), "{}", "Failed to close container lockfile during cleanup")
            }
            LockScope::Session { session_id } => {
                tracing::warn!("session_id" = ?(session_id), "lockfile_path" = ?(path), "error" = ?(error.to_string()), "{}", "Failed to close lockfile during cleanup")
            }
        }
    }

    /// 记录锁文件删除失败；不存在与 Go `os.IsNotExist` 一样保持静默。
    fn log_remove_error(&self, error: &dyn fmt::Display) {
        let path = self.path.display().to_string();
        match &self.scope {
            LockScope::Container => {
                tracing::debug!("lockfile_path" = ?(path), "error" = ?(error.to_string()), "{}", "Could not remove container lockfile")
            }
            LockScope::Session { session_id } => {
                tracing::debug!("session_id" = ?(session_id), "lockfile_path" = ?(path), "error" = ?(error.to_string()), "{}", "Could not remove lockfile")
            }
        }
    }

    /// 记录成功释放；该事件在 panic 展开期间也必须进入 Go 兼容 stderr JSON。
    fn log_released(&self) {
        let path = self.path.display().to_string();
        match &self.scope {
            LockScope::Container => {
                tracing::info!("lockfile_path" = ?(path), "{}", "Released container-level lock")
            }
            LockScope::Session { session_id } => {
                tracing::info!("session_id" = ?(session_id), "lockfile_path" = ?(path), "{}", "Released lockfile")
            }
        }
    }
}

impl Drop for FileLockGuard {
    /// 在所有退出路径上释放内核锁并尽力清理临时文件。
    fn drop(&mut self) {
        self.release_inner();
    }
}

/// 获取容器级全局锁。
///
/// 锁路径固定为 Go 实现使用的 `/tmp/environment-manager-container.lock`，
/// 用于阻止同一容器同时运行多个 orchestrator/session。
pub fn acquire_container_lock() -> Result<FileLockGuard, LockError> {
    let path = PathBuf::from(CONTAINER_LOCK_PATH);
    let file = acquire_path(path.clone()).map_err(|error| match error {
        AcquirePathError::Open { path, source } => LockError::ContainerOpen { path, source },
        AcquirePathError::AlreadyHeld { path } => {
            tracing::error!("lockfile_path" = ?(path.display().to_string()), "{}", "Another orchestrator/session is already running on this container");
            LockError::ContainerAlreadyHeld { path }
        }
        AcquirePathError::Acquire { path, source } => LockError::ContainerAcquire { path, source },
    })?;
    tracing::info!("pid" = ?(std::process::id()), "lockfile_path" = ?(CONTAINER_LOCK_PATH), "{}", "Acquired container-level lock");
    Ok(FileLockGuard {
        file: Some(file),
        path,

        scope: LockScope::Container,
    })
}

/// 获取某个 session 的非阻塞排他锁。
///
/// session ID 按 URL path segment 规则转义，避免 `/`、Unicode 或 `..` 改变
/// 锁文件目录。竞争时立即返回 [`LockError::SessionAlreadyHeld`]，
/// 不会阻塞 Tokio worker，错误文本保留 Go 中的原始 session ID。
pub fn acquire_lock(session_id: &str) -> Result<FileLockGuard, LockError> {
    let path = get_lock_path(session_id);
    let file = acquire_path(path.clone()).map_err(|error| match error {
        AcquirePathError::Open { path, source } => LockError::SessionOpen { path, source },
        AcquirePathError::AlreadyHeld { path } => {
            tracing::error!("session_id" = ?(session_id), "lockfile_path" = ?(path.display().to_string()), "{}", "Session already running (lock held)");
            LockError::SessionAlreadyHeld {
                session_id: session_id.to_owned(),
                path,
            }
        }
        AcquirePathError::Acquire { path, source } => LockError::SessionAcquire { path, source },
    })?;
    tracing::debug!("session_id" = ?(session_id), "pid" = ?(std::process::id()), "lockfile_path" = ?(path.display().to_string()), "{}", "Acquired lockfile using flock");
    Ok(FileLockGuard {
        file: Some(file),
        path,

        scope: LockScope::Session {
            session_id: session_id.to_owned(),
        },
    })
}

/// 打开指定路径并执行非阻塞排他 `flock`。
fn acquire_path(path: PathBuf) -> Result<File, AcquirePathError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    options.mode(0o644);

    let file = options
        .open(&path)
        .map_err(|source| AcquirePathError::Open {
            path: path.clone(),
            source,
        })?;

    if let Err(source) = file.try_lock_exclusive() {
        if source.kind() == io::ErrorKind::WouldBlock {
            return Err(AcquirePathError::AlreadyHeld { path });
        }
        return Err(AcquirePathError::Acquire { path, source });
    }

    Ok(file)
}

/// 构造 session 锁文件的临时目录路径。
fn get_lock_path(session_id: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "environment-manager-{}.lock",
        go_path_escape(session_id)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    const LOCK_PROBE_ENVIRONMENT_KEY: &str = "ENV_MANAGER_LOCKFILE_PROBE";
    const LOCK_PROBE_EXPECTATION_KEY: &str = "ENV_MANAGER_LOCKFILE_EXPECTATION";
    const LOCK_PROBE_TEST_NAME: &str = "internal::util::lockfile::tests::lockfile_subprocess_probe";

    /// 验证同一路径上的第二个非阻塞锁会立即报告竞争。
    #[test]
    fn lock_rejects_concurrent_owner_and_cleans_up() {
        let unique = format!("test-{}-{}", std::process::id(), line!());
        let first = acquire_lock(&unique).expect("first lock should succeed");
        let path = first.path().to_owned();
        let error = acquire_lock(&unique).unwrap_err();
        assert!(matches!(&error, LockError::SessionAlreadyHeld { .. }));
        assert_eq!(
            error.to_string(),
            format!("session {unique} already running (lock held)")
        );
        drop(first);
        assert!(!path.exists());
    }

    /// 供父测试在独立 Linux 进程中探测同一个 flock 和新文件 mode。
    ///
    /// 未设置探针变量的常规全量执行立即返回；子进程根据 expectation 验证竞争或
    /// 成功获取，避免用同一进程内的 flock 行为代替真正的跨进程证据。
    #[test]
    fn lockfile_subprocess_probe() {
        let Some(session_id) = std::env::var_os(LOCK_PROBE_ENVIRONMENT_KEY) else {
            return;
        };
        let session_id = session_id.to_string_lossy();
        match std::env::var(LOCK_PROBE_EXPECTATION_KEY).as_deref() {
            Ok("held") => assert!(matches!(
                acquire_lock(&session_id),
                Err(LockError::SessionAlreadyHeld { .. })
            )),
            Ok("mode-0644") => {
                let guard = acquire_lock(&session_id).unwrap();
                assert_eq!(
                    std::fs::metadata(guard.path())
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o644
                );
                drop(guard);
            }
            expectation => panic!("unknown lock probe expectation: {expectation:?}"),
        }
    }

    /// 验证 LOCK_EX|LOCK_NB 对另一个进程立即报告竞争。
    #[test]
    fn flock_rejects_owner_in_another_process() {
        let unique = format!("cross-process-{}-{}", std::process::id(), line!());
        let first = acquire_lock(&unique).unwrap();
        let output = run_lock_probe(&unique, "held", false);
        assert_probe_succeeded(&output);
        drop(first);
    }

    /// 验证 `umask 000` 下新锁文件仍以 Go 指定的 0644 为权限上限。
    #[test]
    fn lockfile_mode_matches_go_with_zero_umask() {
        let unique = format!("mode-{}-{}", std::process::id(), line!());
        let output = run_lock_probe(&unique, "mode-0644", true);
        assert_probe_succeeded(&output);
        assert!(!get_lock_path(&unique).exists());
    }

    /// 验证 session ID 不能通过斜杠或 Unicode 逃离临时目录。
    #[test]
    fn lock_path_percent_encodes_unsafe_bytes() {
        let path = get_lock_path("../会话/a b;,:@&=+$");
        let name = path.file_name().unwrap().to_string_lossy();
        assert_eq!(
            name,
            "environment-manager-..%2F%E4%BC%9A%E8%AF%9D%2Fa%20b%3B%2C:@&=+$.lock"
        );
        assert_eq!(path.parent(), Some(std::env::temp_dir().as_path()));
    }

    /// 启动只运行 flock helper 的当前 Rust 测试二进制。
    ///
    /// `zero_umask` 通过 shell 在子进程内设置，避免修改并行测试共享的全局 umask。
    fn run_lock_probe(
        session_id: &str,
        expectation: &str,
        zero_umask: bool,
    ) -> std::process::Output {
        let test_binary = std::env::current_exe().unwrap();
        let mut command = if zero_umask {
            let mut command = std::process::Command::new("/bin/sh");
            command
                .arg("-c")
                .arg("umask 000; exec \"$TEST_BINARY\" --exact \"$TEST_NAME\" --nocapture")
                .env("TEST_BINARY", &test_binary)
                .env("TEST_NAME", LOCK_PROBE_TEST_NAME);
            command
        } else {
            let mut command = std::process::Command::new(test_binary);
            command.args(["--exact", LOCK_PROBE_TEST_NAME, "--nocapture"]);
            command
        };
        command
            .env(LOCK_PROBE_ENVIRONMENT_KEY, session_id)
            .env(LOCK_PROBE_EXPECTATION_KEY, expectation)
            .output()
            .unwrap()
    }

    /// 将子测试失败时的 stdout/stderr 一并带入断言，便于 ARM64 CI 定位。
    fn assert_probe_succeeded(output: &std::process::Output) {
        assert!(
            output.status.success(),
            "lock probe failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
