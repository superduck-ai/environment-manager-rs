//! MCP server 注册表与 Claude CLI 注册流程。
//!
//! 注册表保留 Go 的插入顺序和 nil 跳过规则。工厂第一次看到非空工具列表时缓存
//! `mcp__<server>__<tool>` 名称；启动和 `claude mcp add` 则改为可取消的 Tokio
//! 子进程/网络流程，取消仍只终止直接子进程。

use std::{
    ffi::OsStr,
    os::unix::process::ExitStatusExt as _,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::{Arc, OnceLock},
    time::Instant,
};

use nix::{fcntl::OFlag, unistd::pipe2};
use serde_json::json;
use tokio::{io::AsyncReadExt as _, process::Command};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    auth::AuthContext,
    config::{ClaudeConfig, StartupContext},
    logger::Logger,
    process::{go_spawn_error, resolve_go_command},
    session::ActivityRecorder,
};

use super::types::{
    AllowedTools, BaseServer, MCPServerConfigurator, MCPServerRegistration, McpError, McpResult,
    RegistrationStore, ServerConfig, ShouldRegister, log_attributes,
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

/// 按调用顺序追加一个 registration；`None` 对应 Go 可写入的 nil 指针。
pub fn register_server(registration: Option<Arc<MCPServerRegistration>>) {
    registry()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(registration);
}

/// 创建并立即加入注册表的 MCP registration。
///
/// configurator 使用 `Option` 显式表达 Go nil function；factory 会保留原始的错误
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
              logger: Logger,
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
                        "failed to configure {name} server: configurator is nil"
                    )));
                };

                let base_server = Arc::new(BaseServer::new(logger.clone(), name.clone(), "0.0.1"));
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
                    register_with_claude(
                        &cancellation,
                        &logger,
                        &server.get_name(),
                        Some(&server_config),
                    )
                    .await
                    .map_err(|error| {
                        McpError::message(format!(
                            "failed to register {name} server with Claude: {error}"
                        ))
                    })?;
                    logger.info(
                        "MCP server started and registered",
                        log_attributes([("server_name", json!(&name))]),
                    );
                } else {
                    logger.info(
                        "MCP server started",
                        log_attributes([("server_name", json!(&name))]),
                    );
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
/// argv 与 Go 完全一致，URL 固定为 loopback `/mcp`，header 使用精确 Bearer 文本。
pub async fn register_with_claude(
    cancellation: &CancellationToken,
    logger: &Logger,
    server_name: &str,
    server_config: Option<&ServerConfig>,
) -> McpResult<()> {
    let started = Instant::now();
    let Some(server_config) = server_config else {
        return Err(McpError::message(
            "claude mcp add failed: server config is nil",
        ));
    };
    let url = format!("http://127.0.0.1:{}/mcp", server_config.port);
    let auth_header = format!("Authorization: Bearer {}", server_config.bearer_token);

    // Go 的 `CombinedOutput` 会把“启动失败”和“非零退出”汇聚到同一个 `err`
    // 分支。这里先把 Tokio 的两种结果归一化，再从唯一日志调用点输出；这样既保留
    // 完全相同的事件语义，也避免一个 Go 日志源被多个 Rust 调用点重复消费。
    let (listed, list_error) = match run_claude(cancellation, ["mcp", "list"]).await {
        Ok(output) if output.status.success() => (Some(output), None),
        Ok(output) => (None, Some(exit_status_error(&output.status))),
        Err(error) => (None, Some(error.to_string())),
    };
    if let Some(error) = list_error {
        logger.warn(
            "Failed to list MCP servers, proceeding with add",
            log_attributes([("error", json!(error)), ("server_name", json!(server_name))]),
        );
    }
    if listed.as_ref().is_some_and(|output| {
        String::from_utf8_lossy(&output.combined).contains(&format!("{server_name}:"))
    }) {
        logger.info(
            "MCP server already registered, removing and re-adding",
            log_attributes([("server_name", json!(server_name))]),
        );

        // remove 失败与 Go 一致只记录诊断，随后仍然执行 add。失败结果先统一成
        // `(error, output)`，以便所有失败共享唯一日志源位置。
        let remove_failure = match run_claude(cancellation, ["mcp", "remove", server_name]).await {
            Ok(output) if output.status.success() => {
                logger.info(
                    "Removed existing MCP server",
                    log_attributes([("server_name", json!(server_name))]),
                );
                None
            }
            Ok(output) => Some((
                exit_status_error(&output.status),
                String::from_utf8_lossy(&output.combined).into_owned(),
            )),
            Err(error) => Some((error.to_string(), String::new())),
        };
        if let Some((error, output)) = remove_failure {
            logger.warn(
                "Failed to remove existing MCP server",
                log_attributes([
                    ("error", json!(error)),
                    ("output", json!(output)),
                    ("server_name", json!(server_name)),
                ]),
            );
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
    // Go 在 `CombinedOutput` 返回后只计算一次耗时，并让启动失败和非零退出进入同一
    // 日志分支。Rust 的 wait status 与 I/O 错误是不同通道，因此在这里显式归一化。
    let add_result = run_claude(cancellation, args).await;
    let duration_ms = started.elapsed().as_millis();
    let add_failure = match add_result {
        Ok(output) if output.status.success() => None,
        Ok(output) => Some((
            exit_status_error(&output.status),
            String::from_utf8_lossy(&output.combined).into_owned(),
        )),
        Err(error) => Some((error.to_string(), String::new())),
    };
    if let Some((error, output)) = add_failure {
        logger.warn(
            "Failed to register MCP server with claude",
            log_attributes([
                ("error", json!(&error)),
                ("output", json!(output)),
                ("url", json!(&url)),
                ("server_name", json!(server_name)),
                ("duration_ms", json!(duration_ms)),
            ]),
        );
        return Err(McpError::message(format!("claude mcp add failed: {error}")));
    }

    logger.info(
        "Successfully registered MCP server with claude",
        log_attributes([
            ("url", json!(url)),
            ("server_name", json!(server_name)),
            ("duration_ms", json!(duration_ms)),
        ]),
    );
    Ok(())
}

/// 执行一个可取消的 `claude` 子命令并按 Go `CombinedOutput` 语义捕获输出。
///
/// stdout 和 stderr 指向同一个 `pipe2(O_CLOEXEC)` 写端，因而保留内核观察到的到达
/// 顺序，而不是事后把两个独立缓冲区拼接。取消只向 direct child 发送 SIGKILL 并
/// wait，不扩展到进程组；这与 Go `exec.CommandContext` 的当前副作用一致。
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
    let resolved_program = resolve_go_command(OsStr::new("claude"))
        .map_err(|error| McpError::message(error.to_string()))?;
    if cancellation.is_cancelled() {
        return Err(McpError::message("context canceled"));
    }
    // CLOEXEC 必须在 pipe 创建时原子设置；若先 pipe 再 fcntl，并发 spawn 可能短暂
    // 继承父进程写端，让 reader 在 `claude` 已退出后仍无法观察 EOF。
    let (read_end, write_end) =
        pipe2(OFlag::O_CLOEXEC).map_err(|error| McpError::message(error.to_string()))?;
    let stderr_end = write_end
        .try_clone()
        .map_err(|error| McpError::message(error.to_string()))?;
    let mut command = Command::new(&resolved_program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(write_end))
        .stderr(Stdio::from(stderr_end))
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| {
        McpError::message(go_spawn_error(&resolved_program, &error).to_string())
    })?;
    // `Command` 已把 owning FDs 转交给 child；立即 drop 父侧状态，确保最后一个 child
    // 写端关闭后共享 reader 能收到 EOF。
    drop(command);

    let mut reader = tokio::fs::File::from_std(std::fs::File::from(read_end));
    let reader_task = tokio::spawn(async move {
        let mut combined = Vec::new();
        reader.read_to_end(&mut combined).await.map(|_| combined)
    });
    let status = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            // Go CommandContext 的默认 Cancel 只杀 direct child；wait 仍负责回收僵尸。
            let _direct_child_kill = child.start_kill();
            child.wait().await
        },
        result = child.wait() => result,
    }
    .map_err(|error| McpError::message(error.to_string()))?;
    let combined = reader_task
        .await
        .map_err(|error| McpError::message(error.to_string()))?
        .map_err(|error| McpError::message(error.to_string()))?;
    Ok(ClaudeOutput { status, combined })
}

/// 把非零 exit status 转成接近 Go `exec.ExitError` 的稳定文本。
fn exit_status_error(status: &ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    let signal = status.signal().unwrap_or_default();
    // 使用 libc 针对当前 Linux 目标定义的信号编号，避免假定所有 CPU
    // 架构都沿用 ARM64 编号，同时保持 Go syscall.Signal.String 的名称。
    let name = match signal {
        libc::SIGHUP => "hangup",
        libc::SIGINT => "interrupt",
        libc::SIGQUIT => "quit",
        libc::SIGILL => "illegal instruction",
        libc::SIGTRAP => "trace/breakpoint trap",
        libc::SIGABRT => "aborted",
        libc::SIGBUS => "bus error",
        libc::SIGFPE => "floating point exception",
        libc::SIGKILL => "killed",
        libc::SIGUSR1 => "user defined signal 1",
        libc::SIGSEGV => "segmentation fault",
        libc::SIGUSR2 => "user defined signal 2",
        libc::SIGPIPE => "broken pipe",
        libc::SIGALRM => "alarm clock",
        libc::SIGTERM => "terminated",
        // MIPS 和 SPARC 的 Linux signal ABI 没有 SIGSTKFLT；其他架构按 libc
        // 定义保留 Go 对 "stack fault" 的命名。
        #[cfg(not(any(
            target_arch = "mips",
            target_arch = "mips64",
            target_arch = "sparc",
            target_arch = "sparc64"
        )))]
        libc::SIGSTKFLT => "stack fault",
        libc::SIGCHLD => "child exited",
        libc::SIGCONT => "continued",
        libc::SIGSTOP => "stopped (signal)",
        libc::SIGTSTP => "stopped",
        libc::SIGTTIN => "stopped (tty input)",
        libc::SIGTTOU => "stopped (tty output)",
        libc::SIGURG => "urgent I/O condition",
        libc::SIGXCPU => "CPU time limit exceeded",
        libc::SIGXFSZ => "file size limit exceeded",
        libc::SIGVTALRM => "virtual timer expired",
        libc::SIGPROF => "profiling timer expired",
        libc::SIGWINCH => "window changed",
        libc::SIGIO => "I/O possible",
        libc::SIGPWR => "power failure",
        libc::SIGSYS => "bad system call",
        _ => return format!("signal: signal {signal}"),
    };
    format!("signal: {name}")
}

/// 测试间清空进程级 registration，避免并行 case 污染插入顺序。
#[cfg(test)]
pub(crate) fn clear_registry_for_test() {
    registry()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}
