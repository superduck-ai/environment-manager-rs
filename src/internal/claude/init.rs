//! `claude --init-only` session-start hook 执行。
//!
//! 子进程继承过滤后的宿主环境，再应用 Claude config 环境覆盖。stdout/stderr 指向
//! 同一匿名 pipe 以保留交错顺序；取消只终止并回收 direct child。

use std::{ffi::OsString, path::Path, time::Instant};

use crate::internal::config::ClaudeConfig;
use tokio_util::sync::CancellationToken;

use super::{
    claude_code_executor::filter_init_only_from_environ,
    config_helpers::get_claude_path,
    types::{ClaudeError, ClaudeObservability, lossy_utf8_error_view, run_combined_command},
};

/// 执行 init hook；未提供 o11y adapter 时不伪造诊断事件。
pub async fn run_init(
    cancellation: &CancellationToken,
    cfg: Option<&ClaudeConfig>,
    working_dir: impl AsRef<Path>,
) -> Result<(), ClaudeError> {
    run_init_with_observability(cancellation, cfg, working_dir, None).await
}

/// 执行 init hook，并将成功或失败诊断交给显式 o11y adapter。
pub async fn run_init_with_observability(
    cancellation: &CancellationToken,
    cfg: Option<&ClaudeConfig>,
    working_dir: impl AsRef<Path>,
    observability: Option<&dyn ClaudeObservability>,
) -> Result<(), ClaudeError> {
    let working_dir = working_dir.as_ref();
    let claude_path = get_claude_path(cfg);
    tracing::info!("claude_path" = ?(&claude_path), "working_dir" = ?(working_dir.display().to_string()), "{}", "Running claude --init-only for session start hooks");

    let started = Instant::now();
    let mut environment = filter_init_only_from_environ(std::env::vars_os());
    if let Some(cfg) = cfg {
        environment.extend(
            cfg.environment
                .iter()
                .map(|(key, value)| (OsString::from(key), OsString::from(value))),
        );
    }
    let args = [OsString::from("--init-only")];
    let result = run_combined_command(
        cancellation,
        claude_path.as_ref(),
        &args,
        Some(working_dir),
        Some(&environment),
    )
    .await;
    let duration_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);

    let (exit_code, error, output) = match result {
        Ok(output) if output.status.success() => {
            tracing::info!("duration_ms" = ?(duration_ms), "{}", "claude --init-only completed successfully");
            if let Some(observability) = observability {
                observability.log_claude_init(true, None, duration_ms);
            }
            return Ok(());
        }
        Ok(output) => (
            output.status.code().unwrap_or(-1),
            output.status.to_string().into_bytes(),
            output.output.expect_combined(),
        ),
        Err(error) => {
            let message = error.to_string().into_bytes();
            (-1, message, error.output.expect_combined())
        }
    };
    let error_view = lossy_utf8_error_view(&error);
    tracing::error!("error" = ?(&error_view), exit_code, "duration_ms" = ?(duration_ms), "output" = ?(String::from_utf8_lossy(&output)), "{}", "claude --init-only failed");
    if let Some(observability) = observability {
        observability.log_claude_init(false, Some(exit_code), duration_ms);
    }
    let mut message =
        format!("claude --init-only failed with exit code {exit_code}: ").into_bytes();
    // 错误以字节形式保存，仅日志属性使用上方的 UTF-8 替换视图。
    message.extend_from_slice(&error);
    Err(ClaudeError::message_bytes(message))
}
