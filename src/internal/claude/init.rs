//! `claude --init-only` session-start hook 执行。
//!
//! 子进程继承过滤后的宿主环境，再应用 Claude config 环境覆盖。stdout/stderr 指向
//! 同一匿名 pipe 以复刻 `CombinedOutput`；取消只终止 direct child，失败文案保留
//! Go 的退出码和底层 wait/start 错误。

use std::{ffi::OsString, path::Path, time::Instant};

use crate::internal::config::ClaudeConfig;
use tokio_util::sync::CancellationToken;

use super::{
    claude_code_executor::filter_init_only_from_environ,
    config_helpers::get_claude_path,
    types::{ClaudeError, ClaudeObservability, go_utf8_error_view, run_combined_command},
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

    match result {
        Ok(_) => {
            tracing::info!("duration_ms" = ?(duration_ms), "{}", "claude --init-only completed successfully");
            if let Some(observability) = observability {
                observability.log_claude_init(true, None, duration_ms);
            }
            Ok(())
        }
        Err(error) => {
            let error_view = go_utf8_error_view(&error.error);
            tracing::error!("error" = ?(&error_view), "exit_code" = ?(error.exit_code), "duration_ms" = ?(duration_ms), "output" = ?(String::from_utf8_lossy(&error.output)), "{}", "claude --init-only failed");
            if let Some(observability) = observability {
                observability.log_claude_init(false, Some(error.exit_code), duration_ms);
            }
            let mut message = format!(
                "claude --init-only failed with exit code {}: ",
                error.exit_code
            )
            .into_bytes();
            // spawn PathError 可能包含非 UTF-8 executable path；外部错误沿用 Go
            // string 任意字节语义，仅日志属性使用上面的 UTF-8 替换视图。
            message.extend_from_slice(&error.error);
            Err(ClaudeError::message_bytes(message))
        }
    }
}
