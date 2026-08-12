//! 通过 npm 安装或升级 `@anthropic-ai/sandbox-runtime`。
//!
//! 如果系统已有可执行 `srt`，npm 升级失败只记录 warning 并继续使用旧版本；
//! 若不存在可回退版本，则返回错误。取消时子进程通过 `kill_on_drop` 终止。

use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
};

#[cfg(unix)]
use std::os::unix::{ffi::OsStrExt as _, fs::PermissionsExt, process::ExitStatusExt as _};

use nix::{fcntl::OFlag, unistd::pipe2};
use thiserror::Error;
use tokio::{io::AsyncReadExt as _, process::Command};
use tokio_util::sync::CancellationToken;

const PACKAGE_NAME: &str = "@anthropic-ai/sandbox-runtime";

/// sandbox-runtime 安装失败。
#[derive(Debug, Error)]
pub enum SandboxInstallError {
    /// npm 启动、取消、等待或非零退出，且系统没有旧安装可回退。
    #[error("npm install failed: {error}, output: {output}")]
    NpmFailed {
        /// Go `exec.Cmd` 风格的启动或退出错误。
        error: String,
        /// 合并后的 stdout/stderr。
        output: String,
    },
    /// npm 成功，但 PATH 中仍找不到 SRT，也没有旧安装。
    #[error(
        "srt binary not found after installation: exec: {binary:?}: executable file not found in $PATH"
    )]
    BinaryNotFound {
        /// 寻找的 executable 名称或路径。
        binary: String,
    },
}

/// 执行 `CombinedOutput` 风格子进程时的错误与已捕获字节。
///
/// Go 在命令失败时仍返回失败前写入的合并输出；sandbox 安装
/// 需要用这些字节记录 warning 或构造最终 CLI 错误。
#[derive(Debug)]
struct CombinedCommandError {
    /// Go `exec.Cmd` 风格的启动、wait 或信号文本。
    error: String,
    /// 子进程失败前已写入共享 pipe 的原始字节。
    output: Vec<u8>,
}

/// 对应 Go：`internal/sandbox/install.go` 中的 `InstallSandboxRuntime`。
///
/// 安装指定版本的 SRT；空版本等价于 `latest`，`current` 直接跳过。安装前先保存
/// 当前 PATH 中的 SRT 位置，因此升级失败时可以按照 Go 的规则继续使用旧版本。
///
/// # 参数
/// - `cancellation`：与 Go `context.Context` 对应；取消只终止 npm 的直接子进程。
/// - `target_version`：npm 版本或 tag；空字符串解释为 `latest`。
///
/// # 返回
/// 安装成功、明确使用 `current`，或升级失败但已有 SRT 时返回成功；否则返回保留
/// npm 合并输出或 `exec.LookPath` 文本的错误。
///
/// # 副作用
/// 查询 PATH、执行 `npm install -g`，并写入结构化 tracing 事件。
pub async fn install_sandbox_runtime(
    cancellation: &CancellationToken,
    target_version: &str,
) -> Result<(), SandboxInstallError> {
    let target_version = if target_version.is_empty() {
        "latest"
    } else {
        target_version
    };
    if target_version == "current" {
        tracing::debug!(
            "{}",
            "Skipping sandbox-runtime installation (version=current)"
        );
        return Ok(());
    }

    let srt_binary = std::env::var("SRT_BINARY_PATH")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "srt".to_owned());
    let existing = find_executable(&srt_binary);
    if let Some(path) = &existing {
        tracing::debug!("path" = ?(path.to_string_lossy()), "{}", "Found existing sandbox-runtime installation");
    } else {
        tracing::debug!("{}", "No existing sandbox-runtime installation found");
    }

    let install_target = format!("{PACKAGE_NAME}@{target_version}");
    tracing::info!("package" = ?(PACKAGE_NAME), "version" = ?(target_version), "{}", "Installing sandbox-runtime via npm");
    let install_arguments = [
        OsStr::new("install"),
        OsStr::new("-g"),
        OsStr::new(&install_target),
    ];
    let combined_output = match run_combined_command(
        cancellation,
        OsStr::new("npm"),
        &install_arguments,
    )
    .await
    {
        Ok(output) => output,
        Err(failure) if existing.is_some() => {
            tracing::warn!("error" = ?(failure.error), "output" = ?(String::from_utf8_lossy(&failure.output)), "{}", "sandbox-runtime upgrade failed, continuing with existing installation");
            return Ok(());
        }
        Err(failure) => {
            return Err(SandboxInstallError::NpmFailed {
                error: failure.error,
                output: String::from_utf8_lossy(&failure.output).into_owned(),
            });
        }
    };
    tracing::debug!("output" = ?(String::from_utf8_lossy(&combined_output)), "{}", "npm install completed");

    match find_executable(&srt_binary) {
        Some(path) => {
            tracing::info!("version" = ?(target_version), "path" = ?(path.to_string_lossy()), "{}", "sandbox-runtime installation completed");
            Ok(())
        }
        None if existing.is_some() => {
            tracing::warn!("error" = ?(look_path_not_found_error(&srt_binary)), "{}", "srt not found after install, using existing installation");
            Ok(())
        }
        None => Err(SandboxInstallError::BinaryNotFound { binary: srt_binary }),
    }
}

/// 为安装后的第二次 PATH 查询生成 Go `exec.LookPath` 的缺失错误文本。
fn look_path_not_found_error(binary: &str) -> String {
    if binary.as_bytes().contains(&b'/') {
        format!("exec: {binary:?}: stat {binary}: no such file or directory")
    } else {
        format!("exec: {binary:?}: executable file not found in $PATH")
    }
}

/// 以 Go `exec.Cmd.CombinedOutput` 的方式执行一个 direct child。
///
/// stdout 与 stderr 共享同一个 `pipe2(O_CLOEXEC)` 写端，因此按内核接收
/// 顺序捕获，不在 Rust 端拼接或补换行。取消只 SIGKILL 直接子
/// 进程，不扩大为 process group；这会保留 Go `CommandContext` 的现有副作用。
///
/// # 参数
/// - `cancellation`：上层 context 的取消信号；预取消时不启动进程。
/// - `program`：可执行文件名称或路径。
/// - `arguments`：原样传给 child 的 argv，不经 shell 解析。
///
/// # 返回
/// 退出码为零时返回合并原始字节；启动、取消、wait、读 pipe
/// 或非零退出时返回错误文本和失败前输出。
async fn run_combined_command(
    cancellation: &CancellationToken,
    program: &OsStr,
    arguments: &[&OsStr],
) -> Result<Vec<u8>, CombinedCommandError> {
    if cancellation.is_cancelled() {
        return Err(CombinedCommandError {
            error: "context canceled".to_owned(),
            output: Vec::new(),
        });
    }

    // CLOEXEC 必须与 pipe 创建原子完成，否则并发 spawn 可在
    // fcntl 前继承写端，让 reader 在 npm 退出后仍等不到 EOF。
    let (read_end, write_end) = pipe2(OFlag::O_CLOEXEC).map_err(|error| CombinedCommandError {
        error: error.to_string(),
        output: Vec::new(),
    })?;
    let stderr_end = write_end
        .try_clone()
        .map_err(|error| CombinedCommandError {
            error: error.to_string(),
            output: Vec::new(),
        })?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::from(write_end))
        .stderr(Stdio::from(stderr_end))
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| CombinedCommandError {
        error: format_start_error(program, &error),
        output: Vec::new(),
    })?;
    drop(command);

    let mut reader = tokio::fs::File::from_std(std::fs::File::from(read_end));
    let reader_task = tokio::spawn(async move {
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.map(|_| output)
    });
    let status = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            let _direct_child_kill = child.start_kill();
            child.wait().await
        }
        status = child.wait() => status,
    };
    let output = match reader_task.await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return Err(CombinedCommandError {
                error: error.to_string(),
                output: Vec::new(),
            });
        }
        Err(error) => {
            return Err(CombinedCommandError {
                error: error.to_string(),
                output: Vec::new(),
            });
        }
    };
    match status {
        Ok(status) if status.success() => Ok(output),
        Ok(status) => Err(CombinedCommandError {
            error: format_exit_status(status),
            output,
        }),
        Err(error) => Err(CombinedCommandError {
            error: error.to_string(),
            output,
        }),
    }
}

/// 把 Rust spawn 错误收敛为 Go `os/exec` 常见启动文本。
///
/// Rust 对 PATH 中缺失的命令只返回 `os error 2`，而 Go `LookPath`
/// 会附带命令名和 `$PATH` 语义。npm 不存在是 setup 的公开错误
/// 路径，必须保留该文本；其他 errno 继续保留系统描述。
///
/// # 参数
/// - `program`：传给 `Command` 的原始名称或路径。
/// - `error`：spawn 返回的 Linux I/O 错误。
///
/// # 返回
/// PATH 命令缺失时返回 Go `exec: "name": ...` 文本；其他情况
/// 返回底层 I/O 文本。
fn format_start_error(program: &OsStr, error: &std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::NotFound && !program.as_bytes().contains(&b'/') {
        return format!("exec: {program:?}: executable file not found in $PATH");
    }
    error.to_string()
}

/// 把 Linux wait status 格式化为 Go `exec.ExitError.Error()` 文本。
///
/// npm 通常以数字状态退出；上层取消会产生 SIGKILL。额外映射
/// 常见的 shell 信号，避免 Rust 的 `signal: N` 泄露到兼容错误中。
///
/// # 参数
/// - `status`：已回收 direct child 的 Linux wait 状态。
///
/// # 返回
/// 普通退出返回 `exit status N`；信号退出返回 `signal: name`。
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

/// 按 `exec.LookPath` 语义寻找 executable；包含路径分隔符时只检查该路径。
fn find_executable(binary: impl AsRef<OsStr>) -> Option<PathBuf> {
    let binary = binary.as_ref();
    let direct = Path::new(binary);
    // Go LookPath 按原始字节是否包含 `/` 决定直接检查；`foo/`
    // 的 Path components 只有一个，不能用 components().count() 代替。
    if binary.as_bytes().contains(&b'/') {
        return is_executable(direct).then(|| direct.to_owned());
    }
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .map(|directory| directory.join(binary))
        .find(|candidate| is_executable(candidate))
}

/// 判断路径是普通文件，并在 Unix 上至少有一个 executable bit。
fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    return metadata.permissions().mode() & 0o111 != 0;
    #[cfg(not(unix))]
    true
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt as _};

    use tempfile::TempDir;

    use super::*;

    /// 验证 `current` 不触碰 npm 或 PATH 即成功返回。
    #[tokio::test]
    async fn current_version_skips_installation() {
        install_sandbox_runtime(&CancellationToken::new(), "current")
            .await
            .unwrap();
    }

    /// 验证 stdout/stderr 使用共享 pipe，不人为插入换行。
    #[tokio::test]
    async fn combined_output_preserves_kernel_write_order() {
        let temp = TempDir::new().unwrap();
        let script = temp.path().join("combined-output.sh");
        fs::write(
            &script,
            "#!/bin/sh\nprintf stdout\nprintf stderr >&2\nexit 7\n",
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
        let error = run_combined_command(&CancellationToken::new(), script.as_os_str(), &[])
            .await
            .unwrap_err();
        assert_eq!(error.error, "exit status 7");
        assert_eq!(error.output, b"stdoutstderr");
    }

    /// 验证预取消在 exec 前返回 Go context 文本。
    #[tokio::test]
    async fn pre_cancelled_combined_output_does_not_spawn() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error =
            run_combined_command(&cancellation, OsStr::new("/definitely/not/executable"), &[])
                .await
                .unwrap_err();
        assert_eq!(error.error, "context canceled");
        assert!(error.output.is_empty());
    }

    /// 验证 PATH 命令缺失时使用 Go `exec.LookPath` 公开文本。
    #[tokio::test]
    async fn missing_path_command_uses_go_error_text() {
        let name = OsStr::new("environment-manager-definitely-missing-command");
        let error = run_combined_command(&CancellationToken::new(), name, &[])
            .await
            .unwrap_err();
        assert_eq!(
            error.error,
            "exec: \"environment-manager-definitely-missing-command\": executable file not found in $PATH"
        );
        assert!(error.output.is_empty());
    }
}
