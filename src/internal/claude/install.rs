//! Claude Code 版本探测、npm 安装与内置 CLI 升级。
//!
//! 版本识别复刻 Go 正则 `[0-9]+\.[0-9]+\.[0-9]+` 的“任意位置首个匹配”语义，
//! 不引入额外 regex 依赖。所有命令使用 Tokio，取消只 kill direct child；升级失败
//! 且已有版本时继续使用旧安装，这是调用方可观察的非致命回退。

use std::{ffi::OsString, time::Duration};

use crate::internal::{
    api::{LogCategory, LogLevel},
    logger::Logger,
    session::ActivityRecorder,
};
use tokio_util::sync::CancellationToken;

use super::zz_recovered_types::{
    CapturedCommandError, ClaudeError, ClaudeObservability, claude_attrs, run_combined_command,
    run_stdout_command,
};

const CLAUDE_PACKAGE: &str = "@anthropic-ai/claude-code";
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// 判断目标是否为具体语义版本；保留字和不含三段数字的文本返回 false。
pub(super) fn is_specific_version(version: &str) -> bool {
    if matches!(version, "" | "skip" | "latest" | "stable" | "current") {
        return false;
    }
    find_version(version).is_some()
}

/// 找到 ASCII 文本中第一个 `数字.数字.数字` 子串，行为等价于 Go regexp。
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
        Ok(output) => output,
        // Go 忽略 `Cmd.Output` 的 error 但仍扫描非零退出前产生的 stdout。
        Err(error) => error.output,
    };
    find_version(&String::from_utf8_lossy(&output))
        .map(str::to_owned)
        .unwrap_or_default()
}

/// 通过 `npm install -g` 安装 Claude；具体版本会附加到 package 名称。
async fn install_via_npm(
    cancellation: &CancellationToken,
    logger: &Logger,
    version: &str,
) -> Result<(), ClaudeError> {
    let install_target = if is_specific_version(version) {
        format!("{CLAUDE_PACKAGE}@{version}")
    } else {
        CLAUDE_PACKAGE.to_owned()
    };
    logger.info(
        "Installing Claude Code via npm",
        claude_attrs!("package" => &install_target),
    );
    let args = [
        OsString::from("install"),
        OsString::from("-g"),
        OsString::from(&install_target),
    ];
    match run_combined_command(cancellation, "npm".as_ref(), &args, None, None).await {
        Ok(output) => {
            logger.debug(
                "npm install completed",
                claude_attrs!("output" => String::from_utf8_lossy(&output)),
            );
            Ok(())
        }
        Err(error) => Err(command_error("npm install failed", error)),
    }
}

/// 通过现有 Claude 二进制的 `install <version>` 子命令执行升级。
async fn upgrade_via_cli(
    cancellation: &CancellationToken,
    logger: &Logger,
    claude_path: &str,
    version: &str,
) -> Result<(), ClaudeError> {
    logger.info(
        "Upgrading Claude Code via CLI",
        claude_attrs!("version" => version),
    );
    let args = [OsString::from("install"), OsString::from(version)];
    match run_combined_command(cancellation, claude_path.as_ref(), &args, None, None).await {
        Ok(output) => {
            logger.debug(
                "claude install completed",
                claude_attrs!("output" => String::from_utf8_lossy(&output)),
            );
            Ok(())
        }
        Err(error) => Err(command_error("claude install failed", error)),
    }
}

/// 安装或更新 Claude Code；未提供 o11y adapter 时只执行真实安装和活动记录。
pub async fn install_or_update_claude_code(
    cancellation: &CancellationToken,
    logger: &Logger,
    claude_path: &str,
    target_version: &str,
    activity_recorder: &dyn ActivityRecorder,
) -> Result<String, ClaudeError> {
    install_or_update_claude_code_with_observability(
        cancellation,
        logger,
        claude_path,
        target_version,
        activity_recorder,
        None,
    )
    .await
}

/// 安装或更新 Claude Code，并把 Go o11y 调用交给显式 adapter。
pub async fn install_or_update_claude_code_with_observability(
    cancellation: &CancellationToken,
    logger: &Logger,
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
        logger.debug(
            "Skipping Claude Code installation (version=skip)",
            Default::default(),
        );
        return Ok(String::new());
    }

    let existing_version = get_installed_version(cancellation, claude_path).await;
    if target_version == "current" {
        if existing_version.is_empty() {
            logger.error(
                "Claude Code required but not found (version=current)",
                Default::default(),
            );
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

        logger.debug(
            "Using existing Claude Code installation (version=current)",
            claude_attrs!("cc_version" => &existing_version),
        );
        activity_recorder.record_activity(
            LogLevel::Debug,
            LogCategory::Init,
            format!("Using existing Claude Code installation (version {existing_version})"),
            claude_attrs!("cc_version" => &existing_version),
        );
        return Ok(existing_version);
    }

    activity_recorder.record_activity(
        LogLevel::Debug,
        LogCategory::Init,
        format!("Installing/updating Claude Code to version {target_version}"),
        claude_attrs!("target_version" => target_version),
    );

    if existing_version.is_empty() {
        logger.info(
            "Installing Claude Code via npm",
            claude_attrs!("target_version" => target_version),
        );
        if let Err(error) = install_via_npm(cancellation, logger, target_version).await {
            return handle_install_error(
                logger,
                activity_recorder,
                observability,
                error,
                "",
                "npm install",
            );
        }
    } else {
        if is_specific_version(target_version) && existing_version == target_version {
            logger.debug(
                "Claude Code already at desired version",
                claude_attrs!("cc_version" => &existing_version),
            );
            activity_recorder.record_activity(
                LogLevel::Debug,
                LogCategory::Init,
                format!("Claude Code already at desired version {existing_version}"),
                claude_attrs!("cc_version" => &existing_version),
            );
            return Ok(existing_version);
        }

        logger.info(
            "Upgrading Claude Code via CLI",
            claude_attrs!(
                "currentVersion" => &existing_version,
                "target_version" => target_version,
            ),
        );
        if let Err(error) = upgrade_via_cli(cancellation, logger, claude_path, target_version).await
        {
            return handle_install_error(
                logger,
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
            logger,
            activity_recorder,
            observability,
            ClaudeError::message("claude not found after installation"),
            &existing_version,
            "post-install verification",
        );
    }
    if is_specific_version(target_version) && installed_version != target_version {
        return handle_install_error(
            logger,
            activity_recorder,
            observability,
            ClaudeError::message(format!(
                "version mismatch after installation: expected {target_version}, got {installed_version}"
            )),
            &existing_version,
            "post-install verification",
        );
    }

    logger.info(
        "Claude Code installed successfully",
        claude_attrs!("cc_version" => &installed_version),
    );
    activity_recorder.record_activity(
        LogLevel::Info,
        LogCategory::Init,
        format!("Claude Code installed successfully (version {installed_version})"),
        claude_attrs!("cc_version" => &installed_version),
    );
    Ok(installed_version)
}

/// 按是否存在旧版本决定降级为 warning 还是返回致命安装错误。
fn handle_install_error(
    logger: &Logger,
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
        logger.warn(
            "Claude Code update failed, continuing with existing installation",
            claude_attrs!("operation" => operation, "error" => error.to_string()),
        );
        activity_recorder.record_activity(
            LogLevel::Warning,
            LogCategory::Init,
            format!("Claude Code update failed during {operation} (continuing with existing)"),
            claude_attrs!("error" => error.to_string()),
        );
        return Ok(existing_version.to_owned());
    }

    logger.error(
        "Claude Code installation failed with no fallback",
        claude_attrs!("operation" => operation, "error" => error.to_string()),
    );
    activity_recorder.record_activity(
        LogLevel::Error,
        LogCategory::Init,
        format!("Claude Code installation failed during {operation}"),
        claude_attrs!("error" => error.to_string()),
    );
    let mut message = format!("claude code installation failed during {operation}: ").into_bytes();
    // 安装命令的 CombinedOutput 可能不是 UTF-8；外层 `%w` 只追加前缀，不应把
    // 已由 command_error 保存的原始字节提前替换为 U+FFFD。
    message.extend_from_slice(error.as_bytes());
    Err(ClaudeError::message_bytes(message))
}

/// 把捕获命令错误包装为 Go `CombinedOutput` 使用的 `error, output` 文案。
fn command_error(context: &str, error: CapturedCommandError) -> ClaudeError {
    let mut message = context.as_bytes().to_vec();
    message.extend_from_slice(b": ");
    // 启动路径本身可能含非法 UTF-8；Go `%w` 会把底层 PathError 字节原样拼入，
    // 因此不能通过 `format!`/Display 追加这部分。
    message.extend_from_slice(&error.error);
    message.extend_from_slice(b", output: ");
    // Go 的 string 可以承载任意字节；这里直接拼接 CombinedOutput，直到 CLI 最外层
    // 才决定原样写 stderr 或为 JSON 日志生成 UTF-8 替换视图。
    message.extend_from_slice(&error.output);
    ClaudeError::message_bytes(message)
}
