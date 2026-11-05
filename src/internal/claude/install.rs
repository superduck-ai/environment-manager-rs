//! Claude Code 版本探测、npm 安装与内置 CLI 升级。
//!
//! 版本识别使用正则 `[0-9]+\.[0-9]+\.[0-9]+` 的“任意位置首个匹配”语义，
//! 不引入额外 regex 依赖。所有命令使用 Tokio，取消只 kill direct child；升级失败
//! 且已有版本时继续使用旧安装，这是调用方可观察的非致命回退。

use std::{collections::BTreeMap, ffi::OsString, time::Duration};

use crate::internal::{
    api::{LogCategory, LogLevel},
    process::{CommandFailure, CommandOutput},
    session::ActivityRecorder,
};
use tokio_util::sync::CancellationToken;

use super::types::{ClaudeError, ClaudeObservability, run_combined_command, run_stdout_command};

const CLAUDE_PACKAGE: &str = "@anthropic-ai/claude-code";
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// 判断目标是否为具体语义版本；保留字和不含三段数字的文本返回 false。
pub(super) fn is_specific_version(version: &str) -> bool {
    if matches!(version, "" | "skip" | "latest" | "stable" | "current") {
        return false;
    }
    find_version(version).is_some()
}

/// 找到 ASCII 文本中第一个匹配 `[0-9]+\.[0-9]+\.[0-9]+` 的子串。
fn find_version(value: &str) -> Option<&str> {
    let bytes = value.as_bytes();
    let mut start = 0;
    while start < bytes.len() {
        if !bytes[start].is_ascii_digit() {
            start += 1;
            continue;
        }

        let mut cursor = start;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if bytes.get(cursor) != Some(&b'.') {
            start += 1;
            continue;
        }
        cursor += 1;
        let minor_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == minor_start || bytes.get(cursor) != Some(&b'.') {
            start += 1;
            continue;
        }
        cursor += 1;
        let patch_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor > patch_start {
            return value.get(start..cursor);
        }
        start += 1;
    }
    None
}

/// 在十秒窗口内执行 `claude --version`，任何失败都按“未安装”处理。
pub(super) async fn get_installed_version(
    cancellation: &CancellationToken,
    claude_path: &str,
) -> String {
    let args = [OsString::from("--version")];
    let output = match run_stdout_command(
        cancellation,
        claude_path.as_ref(),
        &args,
        Some(VERSION_PROBE_TIMEOUT),
    )
    .await
    {
        Ok(output) => {
            let (stdout, _) = output.output.expect_separate();
            stdout
        }
        // 忽略版本探测错误，但仍扫描非零退出前产生的 stdout。
        Err(error) => {
            let (stdout, _) = error.output.expect_separate();
            stdout
        }
    };
    find_version(&String::from_utf8_lossy(&output))
        .map(str::to_owned)
        .unwrap_or_default()
}

/// 通过 `npm install -g` 安装 Claude；具体版本会附加到 package 名称。
async fn install_via_npm(
    cancellation: &CancellationToken,
    version: &str,
) -> Result<(), ClaudeError> {
    let install_target = if is_specific_version(version) {
        format!("{CLAUDE_PACKAGE}@{version}")
    } else {
        CLAUDE_PACKAGE.to_owned()
    };
    tracing::info!("package" = ?(&install_target), "{}", "Installing Claude Code via npm");
    let args = [
        OsString::from("install"),
        OsString::from("-g"),
        OsString::from(&install_target),
    ];
    match successful_combined(
        "npm install failed",
        run_combined_command(cancellation, "npm".as_ref(), &args, None, None).await,
    ) {
        Ok(output) => {
            tracing::debug!("output" = ?(String::from_utf8_lossy(&output)), "{}", "npm install completed");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// 通过现有 Claude 二进制的 `install <version>` 子命令执行升级。
async fn upgrade_via_cli(
    cancellation: &CancellationToken,
    claude_path: &str,
    version: &str,
) -> Result<(), ClaudeError> {
    tracing::info!("version" = ?(version), "{}", "Upgrading Claude Code via CLI");
    let args = [OsString::from("install"), OsString::from(version)];
    match successful_combined(
        "claude install failed",
        run_combined_command(cancellation, claude_path.as_ref(), &args, None, None).await,
    ) {
        Ok(output) => {
            tracing::debug!("output" = ?(String::from_utf8_lossy(&output)), "{}", "claude install completed");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// 安装或更新 Claude Code；未提供 o11y adapter 时只执行真实安装和活动记录。
pub async fn install_or_update_claude_code(
    cancellation: &CancellationToken,
    claude_path: &str,
    target_version: &str,
    activity_recorder: &dyn ActivityRecorder,
) -> Result<String, ClaudeError> {
    install_or_update_claude_code_with_observability(
        cancellation,
        claude_path,
        target_version,
        activity_recorder,
        None,
    )
    .await
}

/// 安装或更新 Claude Code，并把 o11y 调用交给显式 adapter。
pub async fn install_or_update_claude_code_with_observability(
    cancellation: &CancellationToken,
    claude_path: &str,
    target_version: &str,
    activity_recorder: &dyn ActivityRecorder,
    observability: Option<&dyn ClaudeObservability>,
) -> Result<String, ClaudeError> {
    let target_version = if target_version.is_empty() {
        "latest"
    } else {
        target_version
    };
    if target_version == "skip" {
        tracing::debug!("{}", "Skipping Claude Code installation (version=skip)");
        return Ok(String::new());
    }

    let existing_version = get_installed_version(cancellation, claude_path).await;
    if target_version == "current" {
        if existing_version.is_empty() {
            tracing::error!("{}", "Claude Code required but not found (version=current)");
            activity_recorder.record_activity(
                LogLevel::Error,
                LogCategory::Init,
                "Claude Code required but not found (version=current)".to_owned(),
                Default::default(),
            );
            return Err(ClaudeError::message(
                "claude code required but not found (version=current)",
            ));
        }

        tracing::debug!("cc_version" = ?(&existing_version), "{}", "Using existing Claude Code installation (version=current)");
        activity_recorder.record_activity(
            LogLevel::Debug,
            LogCategory::Init,
            format!("Using existing Claude Code installation (version {existing_version})"),
            BTreeMap::from([("cc_version".to_owned(), serde_json::json!(existing_version))]),
        );
        return Ok(existing_version);
    }

    activity_recorder.record_activity(
        LogLevel::Debug,
        LogCategory::Init,
        format!("Installing/updating Claude Code to version {target_version}"),
        BTreeMap::from([(
            "target_version".to_owned(),
            serde_json::json!(target_version),
        )]),
    );

    if existing_version.is_empty() {
        tracing::info!("target_version" = ?(target_version), "{}", "Installing Claude Code via npm");
        if let Err(error) = install_via_npm(cancellation, target_version).await {
            return handle_install_error(
                activity_recorder,
                observability,
                error,
                "",
                "npm install",
            );
        }
    } else {
        if is_specific_version(target_version) && existing_version == target_version {
            tracing::debug!("cc_version" = ?(&existing_version), "{}", "Claude Code already at desired version");
            activity_recorder.record_activity(
                LogLevel::Debug,
                LogCategory::Init,
                format!("Claude Code already at desired version {existing_version}"),
                BTreeMap::from([("cc_version".to_owned(), serde_json::json!(existing_version))]),
            );
            return Ok(existing_version);
        }

        tracing::info!("currentVersion" = ?(&existing_version), "target_version" = ?(target_version), "{}", "Upgrading Claude Code via CLI");
        if let Err(error) = upgrade_via_cli(cancellation, claude_path, target_version).await {
            return handle_install_error(
                activity_recorder,
                observability,
                error,
                &existing_version,
                "claude install",
            );
        }
    }

    let installed_version = get_installed_version(cancellation, claude_path).await;
    if installed_version.is_empty() {
        return handle_install_error(
            activity_recorder,
            observability,
            ClaudeError::message("claude not found after installation"),
            &existing_version,
            "post-install verification",
        );
    }
    if is_specific_version(target_version) && installed_version != target_version {
        return handle_install_error(
            activity_recorder,
            observability,
            ClaudeError::message(format!(
                "version mismatch after installation: expected {target_version}, got {installed_version}"
            )),
            &existing_version,
            "post-install verification",
        );
    }

    tracing::info!("cc_version" = ?(&installed_version), "{}", "Claude Code installed successfully");
    activity_recorder.record_activity(
        LogLevel::Info,
        LogCategory::Init,
        format!("Claude Code installed successfully (version {installed_version})"),
        BTreeMap::from([(
            "cc_version".to_owned(),
            serde_json::json!(installed_version),
        )]),
    );
    Ok(installed_version)
}

/// 按是否存在旧版本决定降级为 warning 还是返回致命安装错误。
fn handle_install_error(
    activity_recorder: &dyn ActivityRecorder,
    observability: Option<&dyn ClaudeObservability>,
    error: ClaudeError,
    existing_version: &str,
    operation: &str,
) -> Result<String, ClaudeError> {
    if !existing_version.is_empty() {
        if let Some(observability) = observability {
            observability.log_claude_update_failed_nonfatal(operation);
        }
        tracing::warn!("operation" = ?(operation), "error" = ?(error.to_string()), "{}", "Claude Code update failed, continuing with existing installation");
        activity_recorder.record_activity(
            LogLevel::Warning,
            LogCategory::Init,
            format!("Claude Code update failed during {operation} (continuing with existing)"),
            BTreeMap::from([("error".to_owned(), serde_json::json!(error.to_string()))]),
        );
        return Ok(existing_version.to_owned());
    }

    tracing::error!("operation" = ?(operation), "error" = ?(error.to_string()), "{}", "Claude Code installation failed with no fallback");
    activity_recorder.record_activity(
        LogLevel::Error,
        LogCategory::Init,
        format!("Claude Code installation failed during {operation}"),
        BTreeMap::from([("error".to_owned(), serde_json::json!(error.to_string()))]),
    );
    let mut message = format!("claude code installation failed during {operation}: ").into_bytes();
    // 安装命令的捕获输出可能不是 UTF-8；外层只追加前缀，不应把
    // 已由 command_error 保存的原始字节提前替换为 U+FFFD。
    message.extend_from_slice(error.as_bytes());
    Err(ClaudeError::message_bytes(message))
}

/// 把完成状态与生命周期错误收敛为安装步骤的成功输出或 Claude 错误。
fn successful_combined(
    context: &str,
    result: Result<CommandOutput, CommandFailure>,
) -> Result<Vec<u8>, ClaudeError> {
    match result {
        Ok(output) if output.status.success() => Ok(output.output.expect_combined()),
        Ok(output) => {
            let error = output.status.to_string();
            Err(command_error(
                context,
                error.as_bytes(),
                &output.output.expect_combined(),
            ))
        }
        Err(error) => {
            let message = error.to_string();
            Err(command_error(
                context,
                message.as_bytes(),
                &error.output.expect_combined(),
            ))
        }
    }
}

/// 把命令错误和原始输出包装为 `error, output` 文案。
fn command_error(context: &str, error: &[u8], output: &[u8]) -> ClaudeError {
    let mut message = context.as_bytes().to_vec();
    message.extend_from_slice(b": ");
    // 命令错误和输出都以字节保存，避免在 CLI 边界之前替换非法 UTF-8。
    message.extend_from_slice(error);
    message.extend_from_slice(b", output: ");
    // 这里直接拼接捕获输出，直到 CLI 最外层
    // 才决定原样写 stderr 或为 JSON 日志生成 UTF-8 替换视图。
    message.extend_from_slice(output);
    ClaudeError::message_bytes(message)
}
