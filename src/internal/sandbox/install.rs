//! 通过 npm 安装或升级 `@anthropic-ai/sandbox-runtime`。
//!
//! 如果系统已有可执行 `srt`，npm 升级失败只记录 warning 并继续使用旧版本；
//! 若不存在可回退版本，则返回错误。取消时子进程通过 `kill_on_drop` 终止。

use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::{ffi::OsStrExt as _, fs::PermissionsExt};

use thiserror::Error;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::internal::process::{CaptureMode, CommandFailure, CommandOutput, run_command};

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
    #[error("srt binary not found in PATH after installation: {binary}")]
    BinaryNotFound {
        /// 寻找的可执行文件名称或路径。
        binary: String,
    },
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
/// npm 捕获输出或安装后的可执行文件检查错误。
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
    let (combined_output, failure) =
        match run_npm_command(cancellation, OsStr::new("npm"), &install_arguments).await {
            Ok(output) if output.status.success() => (output.output.expect_combined(), None),
            Ok(output) => (
                Vec::new(),
                Some((output.status.to_string(), output.output.expect_combined())),
            ),
            Err(error) => {
                let message = error.to_string();
                (Vec::new(), Some((message, error.output.expect_combined())))
            }
        };
    if let Some((error, output)) = failure {
        if existing.is_some() {
            tracing::warn!("error" = ?(error), "output" = ?(String::from_utf8_lossy(&output)), "{}", "sandbox-runtime upgrade failed, continuing with existing installation");
            return Ok(());
        }
        return Err(SandboxInstallError::NpmFailed {
            error,
            output: String::from_utf8_lossy(&output).into_owned(),
        });
    }
    tracing::debug!("output" = ?(String::from_utf8_lossy(&combined_output)), "{}", "npm install completed");

    match find_executable(&srt_binary) {
        Some(path) => {
            tracing::info!("version" = ?(target_version), "path" = ?(path.to_string_lossy()), "{}", "sandbox-runtime installation completed");
            Ok(())
        }
        None if existing.is_some() => {
            tracing::warn!(error = %binary_not_found_error(&srt_binary), "srt not found after install, using existing installation");
            Ok(())
        }
        None => Err(SandboxInstallError::BinaryNotFound { binary: srt_binary }),
    }
}

/// 为安装后的第二次 PATH 查询生成直接、稳定的诊断文本。
fn binary_not_found_error(binary: &str) -> String {
    format!("executable not found in PATH: {binary}")
}

/// 执行 npm 并捕获 stdout/stderr 的真实交错顺序。
///
/// stdout 与 stderr 共享同一个 close-on-exec 管道写端，因此按内核接收顺序捕获，
/// 不在 Rust 端拼接或补换行。取消只终止并回收直接子进程，不扩大为进程组。
///
/// # 参数
/// - `cancellation`：上层 context 的取消信号；预取消时不启动进程。
/// - `program`：可执行文件名称或路径。
/// - `arguments`：原样传给 child 的 argv，不经 shell 解析。
///
/// # 返回
/// 退出码为零时返回合并原始字节；启动、取消、wait、读 pipe
/// 或非零退出时返回错误文本和失败前输出。
async fn run_npm_command(
    cancellation: &CancellationToken,
    program: &OsStr,
    arguments: &[&OsStr],
) -> Result<CommandOutput, CommandFailure> {
    let mut command = Command::new(program);
    command.args(arguments);
    run_command(command, cancellation, None, None, CaptureMode::Combined).await
}

/// 在显式路径或当前 PATH 中寻找可执行文件。
fn find_executable(binary: impl AsRef<OsStr>) -> Option<PathBuf> {
    let binary = binary.as_ref();
    let direct = Path::new(binary);
    // 只要输入包含 `/` 就视为显式路径；`foo/` 的 Path components 只有一个，
    // 不能用 components().count() 代替。
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
        let error = run_npm_command(&CancellationToken::new(), script.as_os_str(), &[])
            .await
            .unwrap();
        assert_eq!(error.status.code(), Some(7));
        assert_eq!(error.output.expect_combined(), b"stdoutstderr");
    }

    /// 验证预取消在 exec 前返回类型化取消错误的文本视图。
    #[tokio::test]
    async fn pre_cancelled_combined_output_does_not_spawn() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = run_npm_command(&cancellation, OsStr::new("/definitely/not/executable"), &[])
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "command was cancelled");
        assert!(error.output.expect_combined().is_empty());
    }

    /// 验证 PATH 命令缺失时保留 Rust 原生启动错误。
    #[tokio::test]
    async fn missing_path_command_uses_native_error_text() {
        let name = OsStr::new("environment-manager-definitely-missing-command");
        let error = run_npm_command(&CancellationToken::new(), name, &[])
            .await
            .unwrap_err();
        assert!(error.to_string().starts_with("failed to spawn command:"));
        assert!(error.output.expect_combined().is_empty());
    }
}
