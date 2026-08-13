//! MCP server 注册表与 Claude CLI 注册流程。
//!
//! 注册表保留 插入顺序和 `None` 跳过规则。工厂第一次看到非空工具列表时缓存
//! `mcp__<server>__<tool>` 名称；启动和 `claude mcp add` 则改为可取消的 Tokio
//! 子进程/网络流程，取消仍只终止直接子进程。

use std::{
    ffi::OsStr,
    path::PathBuf,
    process::ExitStatus,
    sync::{Arc, OnceLock},
    time::Instant,
};

use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    auth::AuthContext,
    config::{ClaudeConfig, StartupContext},
    process::{CaptureMode, run_command},
    session::ActivityRecorder,
};

use super::types::{
    AllowedTools, BaseServer, MCPServerConfigurator, MCPServerRegistration, McpError, McpResult,
    RegistrationStore, ServerConfig, ShouldRegister,
};

static REGISTRY: OnceLock<RegistrationStore> = OnceLock::new();

/// 一次 `claude` 子进程的退出状态和按到达顺序合并的 stdout/stderr。
struct ClaudeOutput {
    /// direct child 的原始 Linux wait status。
    status: ExitStatus,
    /// stdout/stderr 共用 pipe 后读取到的原始字节。
    combined: Vec<u8>,
}

/// 返回进程级 registration 存储。
fn registry() -> &'static RegistrationStore {
    REGISTRY.get_or_init(|| RegistrationStore::new(Vec::new()))
}

/// 按调用顺序追加一个 registration；`None` 表示空 registration。
pub fn register_server(registration: Option<Arc<MCPServerRegistration>>) {
    registry()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(registration);
}

/// 创建并立即加入注册表的 MCP registration。
///
/// configurator 使用 `Option` 显式表达 `None` callback；factory 会保留原始的错误
/// 包装层级。`allowed_tools` 只有在非空工具列表产生后才真正缓存，零工具 server 的
/// 后续 factory 调用仍会重新计算。
pub fn new_registration(
    name: impl Into<String>,
    description: impl Into<String>,
    configurator: Option<MCPServerConfigurator>,
    should_register: ShouldRegister,
    get_additional_system_prompt: Arc<dyn Fn() -> String + Send + Sync>,
) -> Arc<MCPServerRegistration> {
    let name = name.into();
    let description = description.into();
    let allowed_tools: AllowedTools = Arc::new(std::sync::RwLock::new(Vec::new()));

    let factory_name = name.clone();
    let factory_allowed_tools = Arc::clone(&allowed_tools);
    let factory = Arc::new(
        move |cancellation: CancellationToken,
              auth_context: Option<Arc<AuthContext>>,
              working_dir: PathBuf,
              startup_context: Option<Arc<StartupContext>>,
              activity_recorder: Arc<dyn ActivityRecorder>| {
            let name = factory_name.clone();
            let configurator = configurator.clone();
            let allowed_tools = Arc::clone(&factory_allowed_tools);
            Box::pin(async move {
                let Some(configurator) = configurator else {
                    return Err(McpError::message(format!(
                        "failed to configure {name} server: configurator is missing"
                    )));
                };

                let base_server = Arc::new(BaseServer::new(name.clone(), "0.0.1"));
                let server = configurator(
                    base_server,
                    auth_context,
                    startup_context,
                    working_dir,
                    activity_recorder,
                )
                .await
                .map_err(|error| {
                    McpError::message(format!("failed to configure {name} server: {error}"))
                })?;

                let tools = Arc::clone(&server).get_tools();
                {
                    let mut cached = allowed_tools
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if cached.is_empty() && !tools.is_empty() {
                        *cached = tools
                            .iter()
                            .map(|tool| format!("mcp__{name}__{}", tool.tool.name))
                            .collect();
                    }
                }

                let server_config = server.start(tools).await.map_err(|error| {
                    McpError::message(format!("failed to start {name} server: {error}"))
                })?;
                if server.should_register_with_claude() {
                    register_with_claude(&cancellation, &server.get_name(), Some(&server_config))
                        .await
                        .map_err(|error| {
                            McpError::message(format!(
                                "failed to register {name} server with Claude: {error}"
                            ))
                        })?;
                    tracing::info!("server_name" = ?(&name), "{}", "MCP server started and registered");
                } else {
                    tracing::info!("server_name" = ?(&name), "{}", "MCP server started");
                }
                Ok(server)
            }) as super::types::FactoryFuture
        },
    );

    let getter_cache = Arc::clone(&allowed_tools);
    let registration = Arc::new(MCPServerRegistration {
        name,
        factory,
        description,
        should_register,
        get_additional_system_prompt,
        get_tools: Arc::new(move || {
            getter_cache
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }),
    });
    register_server(Some(Arc::clone(&registration)));
    registration
}

/// 按注册顺序返回当前启动上下文应启用的 server。
pub fn get_mcp_registrations(
    startup_context: Option<&StartupContext>,
    claude_config: Option<&ClaudeConfig>,
) -> Vec<Arc<MCPServerRegistration>> {
    registry()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .filter_map(|registration| registration.as_ref())
        .filter(|registration| (registration.should_register)(startup_context, claude_config))
        .cloned()
        .collect()
}

/// 使用 Claude CLI 删除同名旧条目并添加 HTTP MCP server。
///
/// `mcp list` 和 `mcp remove` 失败都只降级为继续 add；最终 add 失败才返回错误。
/// URL 固定为 loopback `/mcp`，header 使用精确 Bearer 文本。
pub async fn register_with_claude(
    cancellation: &CancellationToken,
    server_name: &str,
    server_config: Option<&ServerConfig>,
) -> McpResult<()> {
    let started = Instant::now();
    let Some(server_config) = server_config else {
        return Err(McpError::message(
            "claude mcp add failed: server config is missing",
        ));
    };
    let url = format!("http://127.0.0.1:{}/mcp", server_config.port);
    let auth_header = format!("Authorization: Bearer {}", server_config.bearer_token);

    // 把启动失败和非零退出归一化，再从唯一日志调用点输出。
    let (listed, list_error) = match run_claude(cancellation, ["mcp", "list"]).await {
        Ok(output) if output.status.success() => (Some(output), None),
        Ok(output) => (None, Some(output.status.to_string())),
        Err(error) => (None, Some(error.to_string())),
    };
    if let Some(error) = list_error {
        tracing::warn!("error" = ?(error), "server_name" = ?(server_name), "{}", "Failed to list MCP servers, proceeding with add");
    }
    if listed.as_ref().is_some_and(|output| {
        String::from_utf8_lossy(&output.combined).contains(&format!("{server_name}:"))
    }) {
        tracing::info!("server_name" = ?(server_name), "{}", "MCP server already registered, removing and re-adding");

        // remove 失败只记录诊断，随后仍然执行 add。失败结果先统一成
        // `(error, output)`，以便所有失败共享唯一日志源位置。
        let remove_failure = match run_claude(cancellation, ["mcp", "remove", server_name]).await {
            Ok(output) if output.status.success() => {
                tracing::info!("server_name" = ?(server_name), "{}", "Removed existing MCP server");
                None
            }
            Ok(output) => Some((
                output.status.to_string(),
                String::from_utf8_lossy(&output.combined).into_owned(),
            )),
            Err(error) => Some((error.to_string(), String::new())),
        };
        if let Some((error, output)) = remove_failure {
            tracing::warn!("error" = ?(error), "output" = ?(output), "server_name" = ?(server_name), "{}", "Failed to remove existing MCP server");
        }
    }

    let args = [
        "mcp",
        "add",
        "--transport",
        "http",
        server_name,
        url.as_str(),
        "--header",
        auth_header.as_str(),
        "--scope",
        "user",
    ];
    // 只计算一次耗时，并把启动失败和非零退出归一到同一日志分支。
    let add_result = run_claude(cancellation, args).await;
    let duration_ms = started.elapsed().as_millis();
    let add_failure = match add_result {
        Ok(output) if output.status.success() => None,
        Ok(output) => Some((
            output.status.to_string(),
            String::from_utf8_lossy(&output.combined).into_owned(),
        )),
        Err(error) => Some((error.to_string(), String::new())),
    };
    if let Some((error, output)) = add_failure {
        tracing::warn!("error" = ?(&error), "output" = ?(output), "url" = ?(&url), "server_name" = ?(server_name), "duration_ms" = ?(duration_ms), "{}", "Failed to register MCP server with claude");
        return Err(McpError::message(format!("claude mcp add failed: {error}")));
    }

    tracing::info!("url" = ?(url), "server_name" = ?(server_name), "duration_ms" = ?(duration_ms), "{}", "Successfully registered MCP server with claude");
    Ok(())
}

/// 执行一个可取消的 `claude` 子命令并捕获 stdout/stderr 的交错顺序。
///
/// stdout 和 stderr 指向同一个 close-on-exec 管道写端，因而保留内核观察到的到达
/// 顺序，而不是事后把两个独立缓冲区拼接。取消只向 direct child 发送 SIGKILL 并
/// wait，不扩展到进程组。
///
/// # 参数
/// - `cancellation`：上层 context；预取消时不启动进程，运行中取消时终止 direct child。
/// - `args`：原样传给 `claude` 的 argv，不经过 shell。
///
/// # 返回
/// 成功启动并完成 wait 时返回状态和合并输出，非零状态也由调用方判定；pipe、spawn、
/// wait 或 reader task 失败时返回 MCP 错误。
async fn run_claude<I, S>(cancellation: &CancellationToken, args: I) -> McpResult<ClaudeOutput>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new("claude");
    command.args(args);
    let output = run_command(command, cancellation, None, None, CaptureMode::Combined)
        .await
        .map_err(|error| McpError::message(error.to_string()))?;
    Ok(ClaudeOutput {
        status: output.status,
        combined: output.output.expect_combined(),
    })
}

/// 测试间清空进程级 registration，避免并行 case 污染插入顺序。
#[cfg(test)]
pub(crate) fn clear_registry_for_test() {
    registry()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}
