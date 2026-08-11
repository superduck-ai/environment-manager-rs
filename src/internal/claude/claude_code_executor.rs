//! Claude Code 主子进程的参数、环境、FD、stdio 与生命周期实现。
//!
//! 对应 Go：`internal/claude/claude_code_executor.go`。本模块由 `manager` 创建，向下
//! 调用配置、认证、会话活动记录与 tailer；结构上把 Go 的 `Cmd.Run` 拆为 Tokio
//! child、两个有界生命周期 stream task 和显式清理阶段，但保持外部顺序与副作用。
//!
//! stdout/stderr 以原始字节同时写入内存和 `/tmp/claude-code.log`，因此 stream-json
//! 与 NDJSON 不会经历解析或重编码。敏感 token 先写入匿名 pipe，再在 `pre_exec`
//! 中安全映射到从 3 开始的固定 FD。所有取消路径仅 SIGKILL direct child，绝不
//! 创建或终止 process group，以保留 Go `exec.CommandContext` 的孙进程语义。

use std::{
    ffi::{OsStr, OsString},
    fs::OpenOptions as StdOpenOptions,
    io::{self, Write as _},
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd},
        unix::{
            fs::{DirBuilderExt as _, OpenOptionsExt as _},
            process::CommandExt as _,
        },
    },
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    unistd::{close, pipe2},
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Mutex,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{ClaudeCodeExecutionError, LogCategory, LogLevel},
    auth::AuthContext,
    config::{ClaudeConfig, Source, StartupContext},
    logger::{LogHandler, LogLevel as LoggerLevel, LogWriter, Logger},
    process::{go_spawn_error, resolve_go_command},
    session::ActivityRecorder,
    util::{Tailer, version},
};

use super::{
    Outcomes,
    config_helpers::get_claude_path,
    session_urls::build_session_urls,
    zz_recovered_types::{
        ClaudeCodeExecutor, ClaudeError, ClaudeObservability, Executor, McpServerRegistration,
        claude_attrs, format_exit_status,
    },
};

const CLAUDE_CODE_LOG_PATH: &str = "/tmp/claude-code.log";
const DEFAULT_API_BASE_URL: &str = "api.anthropic.com";
const DEFAULT_MCP_CONFIG_PATH: &str = "/tmp/mcp-config.json";
const SESSION_INGRESS_TOKEN_PATH: &str = "/home/claude/.claude/remote/.session_ingress_token";
const CCR_UPSTREAM_PROXY_ENABLED_ENVIRONMENT_KEY: &str = "CCR_UPSTREAM_PROXY_ENABLED";
const CLAUDE_CODE_REMOTE_ENVIRONMENT_KEY: &str = "CLAUDE_CODE_REMOTE";
const STREAM_BUFFER_SIZE: usize = 32 * 1024;
const SAFE_TOKEN_FD_FLOOR: i32 = 64;

/// 父进程持有的 token pipe 读端及其 child 目标 FD。
pub(super) struct TokenDescriptor {
    pub(super) source: TokenSourceFd,
    pub(super) target: i32,
}

/// 父进程持有的 token pipe 读端，并在释放时检查 `close(2)` 的返回值。
///
/// `OwnedFd` 的标准析构会安全关闭 FD，却按 Rust 约定忽略关闭错误；Go 基线则在
/// `Execute` 的 `filesToClose` defer 中记录该错误。这个薄包装只补回可观察日志，
/// 不改变 FD 的所有权、CLOEXEC 标志或 child 的目标编号。
pub(super) struct TokenSourceFd {
    fd: Option<OwnedFd>,
    logger: Logger,
}

impl TokenSourceFd {
    /// 接管一个高位 CLOEXEC source FD；此后仅由本包装负责关闭。
    fn new(fd: OwnedFd, logger: Logger) -> Self {
        Self {
            fd: Some(fd),
            logger,
        }
    }

    /// 返回仍由本包装拥有的 FD；只在析构前的安装与测试阶段调用。
    fn fd(&self) -> &OwnedFd {
        self.fd
            .as_ref()
            .expect("token source FD is available until drop")
    }
}

impl AsFd for TokenSourceFd {
    /// 借用 source FD，供 `fcntl` 与测试检查 CLOEXEC，不转移所有权。
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd().as_fd()
    }
}

impl AsRawFd for TokenSourceFd {
    /// 返回 source FD 数字，供 child `pre_exec` 建立 3/4/5 映射。
    fn as_raw_fd(&self) -> i32 {
        self.fd().as_raw_fd()
    }
}

impl Drop for TokenSourceFd {
    /// 对应 Go：`ClaudeCodeExecutor.Execute` 中关闭 `filesToClose` 的 defer。
    ///
    /// 先用 `into_raw_fd` 取消 `OwnedFd` 的隐式析构，再显式检查一次 `close(2)`；
    /// 这样既不会 double-close，也能保留 Go 的 DEBUG 错误日志。
    fn drop(&mut self) {
        let Some(fd) = self.fd.take() else {
            return;
        };
        if let Err(error) = close_owned_fd(fd.into_raw_fd()) {
            self.logger.debug(
                "Failed to close token file descriptor",
                claude_attrs!("error" => error.to_string()),
            );
        }
    }
}

/// 一次 Claude child wait 与 stream 汇合的结果。
struct ExecutionRun {
    /// Go error string 的原始字节；显式 executable 路径可能不是 UTF-8。
    error: Option<Vec<u8>>,
    exit_code: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// 正在运行的 CC log tailer 及其结构化日志转发任务。
struct ActiveTailer {
    tailer: Arc<Tailer>,
    consumer: JoinHandle<()>,
}

/// 构造 Claude Code 执行器，并保留尚未消费的 MCP/lease 上下文。
#[allow(clippy::too_many_arguments)]
pub fn new_claude_code_executor(
    logger: Logger,
    cfg: Option<ClaudeConfig>,
    session_id: impl Into<String>,
    startup_context: StartupContext,
    auth_context: Option<AuthContext>,
    mcp_registrations: Vec<Arc<dyn McpServerRegistration>>,
    outcomes: Option<Outcomes>,
    activity_recorder: Arc<dyn ActivityRecorder>,
    otel_log_handler: Arc<dyn LogHandler>,
    lease_manager: Option<Arc<dyn crate::internal::envtype::LeaseManagerHandle>>,
    _target_version: &str,
) -> Result<ClaudeCodeExecutor, ClaudeError> {
    let session_id = session_id.into();
    if session_id.is_empty() {
        return Err(ClaudeError::message("sessionID cannot be empty"));
    }

    let api_base_url = if startup_context.api_base_url.is_empty() {
        DEFAULT_API_BASE_URL.to_owned()
    } else {
        logger.info(
            "Using APIBaseURL from startup context",
            claude_attrs!("url" => &startup_context.api_base_url),
        );
        startup_context.api_base_url.clone()
    };
    let claude_path = get_claude_path(&logger, cfg.as_ref());
    if let Some(outcomes) = outcomes.as_ref().filter(|outcomes| !outcomes.is_empty()) {
        logger.info(
            "ClaudeCodeExecutor initialized with repository outcomes",
            claude_attrs!("repo_count" => outcomes.len()),
        );
    }

    Ok(ClaudeCodeExecutor {
        logger,
        cfg,
        session_id,
        claude_path,
        startup_context,
        auth_context,
        api_base_url,
        _outcomes: outcomes,
        _mcp_registrations: mcp_registrations,
        activity_recorder,
        otel_log_handler,
        _lease_manager: lease_manager,
        observability: None,
    })
}

/// 从宿主环境中删除只允许 init script 使用的变量。
///
/// 泛型 pair 输入既可直接接 `std::env::vars_os`，也便于测试保留非 UTF-8 值。
pub(super) fn filter_init_only_from_environ<I, K, V>(environment: I) -> Vec<(OsString, OsString)>
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<OsString>,
    V: Into<OsString>,
{
    environment
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .filter(|(key, _)| key != OsStr::new("CCR_PLUGINS_JSON"))
        .collect()
}

/// 解析部署系统常见的 truthy 环境值。
fn is_truthy_environment_value(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// 旧 token 文件交接已停用；测试入口固定返回错误以防行为被意外恢复。
#[cfg(test)]
pub(super) fn write_ccr_upstream_proxy_token(
    _path: &Path,
    _token: &str,
) -> Result<(), ClaudeError> {
    Err(ClaudeError::message(
        "CCR upstream proxy token handoff is no longer supported",
    ))
}

/// 创建 token pipe、写入一行 token，并安排为 child 的下一个 ExtraFiles FD。
pub(super) fn add_token_via_file_descriptor(
    logger: &Logger,
    token: &str,
    token_name: &str,
    descriptors: &mut Vec<TokenDescriptor>,
) -> Result<i32, ClaudeError> {
    // Go 在 Linux 上的 os.Pipe 原子调用 pipe2(O_CLOEXEC)。这里不能先 pipe 再
    // fcntl，否则并发线程恰好 spawn 时可能把敏感 token FD 泄漏给无关子进程。
    let (read_end, write_end) = pipe2(OFlag::O_CLOEXEC).map_err(|error| {
        ClaudeError::message(format!("failed to create pipe for {token_name}: {error}"))
    })?;
    let mut writer = std::fs::File::from(write_end);
    // Go 无论 Write 是否成功都会先关闭写端，并且关闭错误不会覆盖真正的写错误。
    // 因此这里必须把两次结果分开保存，不能用 `?` 提前离开并依赖静默 Drop。
    let write_error = writer.write_all(format!("{token}\n").as_bytes()).err();
    if let Err(error) = close_owned_fd(writer.into_raw_fd()) {
        logger.error(
            format!("Failed to close write end of {token_name} pipe"),
            claude_attrs!("error" => error.to_string()),
        );
    }
    if let Some(error) = write_error {
        // Go 只在写入失败时立即关闭读端；成功路径由 Execute 的 defer 在整个
        // child 生命周期结束后关闭，所以不能把这个清理提前到普通路径。
        if let Err(close_error) = close_owned_fd(read_end.into_raw_fd()) {
            logger.debug(
                format!("Failed to close read end of {token_name} pipe"),
                claude_attrs!("error" => close_error.to_string()),
            );
        }
        return Err(ClaudeError::message(format!(
            "failed to write {token_name} to pipe: {error}"
        )));
    }

    // 所有 source FD 先复制到 64 以上，避免 pre_exec 依次 dup2 到 3/4/5 时
    // 覆盖尚未处理的另一个 pipe source；这比依赖父进程当前 FD 分配更稳健。
    let duplicated =
        fcntl(&read_end, FcntlArg::F_DUPFD_CLOEXEC(SAFE_TOKEN_FD_FLOOR)).map_err(|error| {
            ClaudeError::message(format!("failed to prepare pipe for {token_name}: {error}"))
        })?;
    // SAFETY: `fcntl(F_DUPFD_CLOEXEC)` 成功返回一个由当前进程独占的新 FD；
    // 从此由 OwnedFd 唯一负责关闭，不再以其他 owning handle 使用该 raw fd。
    let source = unsafe { OwnedFd::from_raw_fd(duplicated) };
    let target = i32::try_from(descriptors.len()).unwrap_or(i32::MAX) + 3;
    descriptors.push(TokenDescriptor {
        source: TokenSourceFd::new(source, logger.clone()),
        target,
    });
    logger.info(
        format!("Passed {token_name} via file descriptor"),
        claude_attrs!("fd" => target),
    );
    Ok(target)
}

impl ClaudeCodeExecutor {
    /// 注入真实 o11y adapter；未调用时所有 o11y 事件被明确省略而非伪造。
    pub fn set_observability(&mut self, observability: Arc<dyn ClaudeObservability>) {
        self.observability = Some(observability);
    }

    /// 覆盖 Claude 可执行路径，供安装后的真实路径或测试脚本使用。
    pub fn set_claude_path(&mut self, path: impl Into<String>) {
        self.claude_path = path.into();
    }

    /// 按“宿主 < ClaudeConfig < StartupContext”读取有效环境值。
    fn effective_environment_value(&self, key: &str) -> String {
        let mut value = std::env::var(key).unwrap_or_default();
        if let Some(configured) = self.cfg.as_ref().and_then(|cfg| cfg.environment.get(key)) {
            value.clone_from(configured);
        }
        if let Some(configured) = self.startup_context.environment_variables.get(key) {
            value.clone_from(configured);
        }
        value
    }

    /// 判断远程 code session 是否配置了 CCRv2 relay。
    pub(super) fn should_prepare_ccr_upstream_proxy_token(&self) -> bool {
        self.startup_context.use_code_sessions
            && is_truthy_environment_value(
                &self.effective_environment_value(CLAUDE_CODE_REMOTE_ENVIRONMENT_KEY),
            )
            && is_truthy_environment_value(
                &self.effective_environment_value(CCR_UPSTREAM_PROXY_ENABLED_ENVIRONMENT_KEY),
            )
    }

    /// 判断当前会话是否由 environment-manager 启动 CCRv2 relay。
    pub(super) fn should_start_agent_proxy(&self, local_testing_mode: bool) -> bool {
        !local_testing_mode && self.should_prepare_ccr_upstream_proxy_token()
    }

    /// 合并 sandbox gateway 参数、本地追加 prompt，并按需写出 gateway MCP 文件。
    pub(super) async fn build_args_from_gateway_config(
        &self,
        mut args: Vec<String>,
    ) -> Result<Vec<String>, ClaudeError> {
        let gateway_args = &self.startup_context.claude_code_args;
        self.logger.info(
            "Building command args from sandbox-gateway config",
            claude_attrs!(
                "gateway_args_count" => gateway_args.len(),
                "has_mcp_config_file" => self.startup_context.mcp_config_file.is_some(),
            ),
        );
        if self.startup_context.mcp_config_file.is_some() {
            self.write_mcp_config_file_from_gateway()
                .await
                .map_err(|error| {
                    ClaudeError::message(format!(
                        "failed to write MCP config from gateway: {error}"
                    ))
                })?;
        }

        for (key, value) in gateway_args {
            if value.is_empty() {
                continue;
            }
            let flag = format!("--{key}");
            args.extend([flag.clone(), value.clone()]);
            self.logger.info(
                "Added gateway arg to command",
                claude_attrs!("flag" => flag, "value" => value),
            );
        }

        if let Some(local_prompt) = self
            .cfg
            .as_ref()
            .map(|cfg| cfg.local_append_system_prompt.as_str())
            .filter(|prompt| !prompt.is_empty())
        {
            if let Some(gateway_prompt) = gateway_args
                .get("append-system-prompt")
                .filter(|prompt| !prompt.is_empty())
            {
                let merged = format!("{gateway_prompt}\n\n{local_prompt}");
                let mut index = 0;
                while index + 1 < args.len() {
                    if args[index] == "--append-system-prompt" {
                        args[index + 1].clone_from(&merged);
                    }
                    index += 1;
                }
            } else {
                args.extend(["--append-system-prompt".to_owned(), local_prompt.to_owned()]);
            }
            self.logger.info(
                "Merged local append system prompt with gateway config",
                claude_attrs!("local_prompt_length" => local_prompt.len()),
            );
        }
        Ok(args)
    }

    /// Base64 解码并按 gateway 指定 path/mode 写出 MCP JSON 文件。
    pub(super) async fn write_mcp_config_file_from_gateway(&self) -> Result<String, ClaudeError> {
        let Some(configuration) = self.startup_context.mcp_config_file.as_ref() else {
            return Ok(String::new());
        };
        let decoded = BASE64_STANDARD
            .decode(&configuration.content)
            .map_err(|error| {
                ClaudeError::message(format!("failed to decode MCP config content: {error}"))
            })?;
        let path = if configuration.path.is_empty() {
            DEFAULT_MCP_CONFIG_PATH
        } else {
            &configuration.path
        };
        let mode = if (1..=0o777).contains(&configuration.mode) {
            u32::try_from(configuration.mode).unwrap_or(0o600)
        } else {
            0o600
        };
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true).mode(mode);
        let mut file = options.open(path).await.map_err(|error| {
            ClaudeError::message(format!("failed to write MCP config to {path}: {error}"))
        })?;
        file.write_all(&decoded).await.map_err(|error| {
            ClaudeError::message(format!("failed to write MCP config to {path}: {error}"))
        })?;
        file.shutdown().await.map_err(|error| {
            ClaudeError::message(format!("failed to write MCP config to {path}: {error}"))
        })?;
        self.logger.info(
            "Wrote MCP config file from gateway",
            claude_attrs!("path" => path, "size_bytes" => decoded.len()),
        );
        Ok(path.to_owned())
    }

    /// 执行完整 Claude child 生命周期，并保持 Go 参数、环境和副作用。
    async fn execute_inner(&self, cancellation: &CancellationToken) -> Result<(), ClaudeError> {
        self.logger.info(
            "Starting ClaudeCodeExecutor",
            claude_attrs!("session_id" => &self.session_id),
        );
        let local_testing_mode = self.cfg.as_ref().is_some_and(|cfg| cfg.local_testing_mode);
        let mut args = vec![
            "--output-format=stream-json".to_owned(),
            "--verbose".to_owned(),
        ];
        if !local_testing_mode {
            args.extend([
                "--replay-user-messages".to_owned(),
                "--input-format=stream-json".to_owned(),
                "--debug-to-stderr".to_owned(),
            ]);
        }
        self.logger.info(
            "Using sandbox-gateway config for Claude Code args",
            Default::default(),
        );
        args = self
            .build_args_from_gateway_config(args)
            .await
            .map_err(|error| {
                ClaudeError::message(format!("failed to build args from gateway config: {error}"))
            })?;

        let configured_working_dir = self.cfg.as_ref().map_or("", |cfg| cfg.working_dir.as_str());
        if !self.startup_context.sources.is_empty() {
            let base_dir = if configured_working_dir.is_empty() {
                Path::new("/workspace")
            } else {
                Path::new(configured_working_dir)
            };
            for source in &self.startup_context.sources {
                if !source.is_repository() {
                    continue;
                }
                let source_dir = source.get_directory(base_dir);
                if source_dir.as_os_str().is_empty() {
                    continue;
                }
                self.logger.info(
                    "Adding repository directory to Claude multi-root",
                    claude_attrs!(
                        "directory" => source_dir.display().to_string(),
                        "source_type" => source.get_type(),
                    ),
                );
                args.extend([
                    "--add-dir".to_owned(),
                    source_dir.to_string_lossy().into_owned(),
                ]);
            }
        }

        let api_version = if matches!(std::env::var("CCR_USE_V2_ENDPOINT").as_deref(), Ok("true")) {
            "v2"
        } else {
            "v1"
        };
        self.logger.info(
            "Endpoint selection based on CCR_USE_V2_ENDPOINT",
            claude_attrs!(
                "use_v2_endpoint" => api_version == "v2",
                "api_version" => api_version,
            ),
        );
        if !local_testing_mode {
            let (sdk_url, resume_url) = build_session_urls(
                &self.api_base_url,
                &self.session_id,
                api_version,
                self.startup_context.use_code_sessions,
            );
            args.extend([
                "--sdk-url".to_owned(),
                sdk_url.clone(),
                format!("--resume={resume_url}"),
            ]);
            self.logger.info(
                "Using SDK URL",
                claude_attrs!(
                    "sdk_url" => sdk_url,
                    "resume_url" => &resume_url,
                    "api_version" => api_version,
                    "api_base_url" => &self.api_base_url,
                    "session_id" => &self.session_id,
                ),
            );
            self.logger.info(
                "Using cloud session persistence with resume URL",
                claude_attrs!("resume_url" => resume_url),
            );
        } else {
            self.logger.info(
                "WebSocket disabled, running Claude Code in standalone mode",
                Default::default(),
            );
        }
        if std::env::var_os("CLAUDE_CODE_DEBUG").is_some_and(|value| !value.is_empty()) {
            args.push("--debug".to_owned());
            self.logger.info(
                "CLAUDE_CODE_DEBUG is set, adding --debug flag",
                Default::default(),
            );
        }

        let working_dir = self.resolve_working_directory(configured_working_dir);
        if local_testing_mode {
            // Go 在构造 `cmd.Stdin` 时立即记录此事件；实际字节仍由 Tokio 在 spawn
            // 后写入匿名 stdin pipe，但日志顺序必须保持在环境和文件准备之前。
            self.logger.info(
                "Passing user message via stdin (WebSocket disabled)",
                Default::default(),
            );
        }
        let mut environment = self.build_child_environment();
        let mut descriptors = Vec::new();
        self.prepare_authentication(local_testing_mode, &mut environment, &mut descriptors)?;
        let mut agent_proxy = self
            .start_agent_proxy(local_testing_mode, &environment)
            .await?;
        if let Some(proxy) = agent_proxy.as_ref() {
            environment.extend(proxy.environment().iter().cloned());
        }

        // Go `exec.CommandContext` 在构造 Cmd 时立即执行 LookPath，但把错误保存到
        // `Cmd.Err`，直到后续 `Run` 才返回。这里同样先解析并继续完成日志/tailer
        // 准备，避免 missing executable 改变已有文件副作用的先后顺序。
        let (resolved_program, command_lookup_error) =
            match resolve_go_command(OsStr::new(&self.claude_path)) {
                Ok(path) => (path, None),
                Err(error) => (OsString::from(&self.claude_path), Some(error.into_bytes())),
            };
        let mut command = Command::new(&resolved_program);
        command
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear()
            .envs(environment);
        // `os.Getwd` 失败时 Go 保持 `Cmd.Dir == ""`，让 exec 继承当前目录；给 Tokio
        // 传空 Path 会变成一次失败的 chdir，所以 None 分支必须完全不调用 current_dir。
        if let Some(directory) = working_dir.as_deref() {
            command.current_dir(directory);
        }
        install_token_descriptors(&mut command, &descriptors)?;

        let log_file = open_claude_log().await?;
        let log_file = Arc::new(Mutex::new(log_file));
        write_log_header(
            &self.logger,
            &log_file,
            &self.claude_path,
            &args,
            &self.session_id,
            working_dir.as_deref(),
        )
        .await;
        write_command_file(&self.logger, &self.claude_path, &args).await;
        let active_tailer = start_cc_log_tailer(&self.logger, &self.session_id).await;

        self.logger.info(
            "Starting Claude Code execution",
            claude_attrs!("has_cancel" => true, "args_count" => args.len()),
        );
        if let Some(observability) = self.observability.as_deref() {
            observability.increment_claude_code_start();
            observability.log_claude_code_start();
        }
        let started = Instant::now();
        let run = self
            .run_child(
                cancellation,
                command,
                resolved_program,
                command_lookup_error,
                local_testing_mode,
                Arc::clone(&log_file),
                agent_proxy.as_mut(),
            )
            .await;
        if let Some(proxy) = agent_proxy.take() {
            proxy.shutdown().await;
        }
        stop_cc_log_tailer(active_tailer).await;
        let duration_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
        if let Some(observability) = self.observability.as_deref() {
            observability.log_claude_code_end(run.error.is_some(), run.exit_code, duration_ms);
            observability.increment_claude_code_end(
                run.error.is_some(),
                run.error
                    .as_ref()
                    .and_then(|_| (run.exit_code >= 0).then_some(run.exit_code)),
            );
        }

        let result = self
            .finish_execution(cancellation, &log_file, run, duration_ms)
            .await;

        // 日志与 token read FD 的清理只告警，不覆盖 child 的主返回结果。
        close_claude_log(&self.logger, log_file).await;
        drop(descriptors);
        result
    }

    /// 对应 Go：`ClaudeCodeExecutor.Execute` 中选择 `cmd.Dir` 的三分支逻辑。
    ///
    /// 配置目录存在时返回实际路径；未配置时尝试 `getcwd`。Linux 上当前目录已被
    /// 删除会使 `getcwd` 失败，此时返回 `None` 表示完全不设置 Tokio `current_dir`，
    /// 并保留 Go 的 warning + `working_dir=unknown` 两条日志，而不是中止执行。
    fn resolve_working_directory(&self, configured: &str) -> Option<PathBuf> {
        if !configured.is_empty() {
            let directory = if self.startup_context.sources.len() == 1 {
                self.startup_context.sources[0].get_directory(configured)
            } else {
                PathBuf::from(configured)
            };
            self.logger.info(
                "Claude Code will run in directory",
                claude_attrs!("working_dir" => directory.display().to_string()),
            );
            return Some(directory);
        }

        match std::env::current_dir() {
            Err(error) => {
                self.logger.warn(
                    "Failed to get current working directory",
                    claude_attrs!("error" => error.to_string()),
                );
                self.logger.info(
                    "Claude Code will run in current directory (no working_dir configured)",
                    claude_attrs!("working_dir" => "unknown"),
                );
                None
            }
            // 分支书写顺序有意与 Go 源码的两个同文案 slog 点一致：错误分支在前、
            // 成功分支在后，使静态 occurrence 映射与实际 runtime source 同时正确。
            Ok(directory) => {
                self.logger.info(
                    "Claude Code will run in current directory (no working_dir configured)",
                    claude_attrs!("working_dir" => directory.display().to_string()),
                );
                Some(directory)
            }
        }
    }

    /// 按 Go append 顺序构造 child 环境，后层键覆盖前层键。
    pub(super) fn build_child_environment(&self) -> Vec<(OsString, OsString)> {
        let mut environment = filter_init_only_from_environ(std::env::vars_os());
        let mut has_anthropic_base_url = false;
        if let Some(cfg) = self.cfg.as_ref() {
            for (key, value) in &cfg.environment {
                has_anthropic_base_url |= key == "ANTHROPIC_BASE_URL";
                environment.push((OsString::from(key), OsString::from(value)));
            }
        }
        if !has_anthropic_base_url && !self.startup_context.api_base_url.is_empty() {
            environment.push((
                OsString::from("ANTHROPIC_BASE_URL"),
                OsString::from(&self.startup_context.api_base_url),
            ));
            self.logger.info(
                "Set ANTHROPIC_BASE_URL from startup context (fallback)",
                claude_attrs!("api_base_url" => &self.startup_context.api_base_url),
            );
        }
        environment.extend([
            (
                OsString::from("CLAUDE_CODE_SESSION_ID"),
                OsString::from(&self.session_id),
            ),
            (
                OsString::from("CLAUDE_CODE_REMOTE_SESSION_ID"),
                OsString::from(&self.session_id),
            ),
            (
                OsString::from("CLAUDE_CODE_ENVIRONMENT_RUNNER_VERSION"),
                OsString::from(version()),
            ),
        ]);
        if let Some(path) = self
            .observability
            .as_deref()
            .and_then(|observability| observability.claude_code_diagnostics_file_path())
            .filter(|path| !path.is_empty())
        {
            environment.push((
                OsString::from("CLAUDE_CODE_DIAGNOSTICS_FILE"),
                OsString::from(path),
            ));
        }
        for source in &self.startup_context.sources {
            let Source::GitRepository(source) = source;
            if let Some(reference) = source
                .git_info
                .git_ref
                .as_deref()
                .filter(|reference| !reference.is_empty())
            {
                environment.push((
                    OsString::from("CLAUDE_CODE_BASE_REF"),
                    OsString::from(reference.strip_prefix("refs/heads/").unwrap_or(reference)),
                ));
            }
        }
        for (key, value) in &self.startup_context.environment_variables {
            environment.push((OsString::from(key), OsString::from(value)));
            if key == "ANTHROPIC_BASE_URL" {
                self.logger.info(
                    "Using ANTHROPIC_BASE_URL from environment config",
                    claude_attrs!("api_base_url" => value),
                );
            }
        }
        environment
    }

    /// 准备 session/OAuth/API token FD 与 Bash 兼容 token 文件。
    fn prepare_authentication(
        &self,
        local_testing_mode: bool,
        environment: &mut Vec<(OsString, OsString)>,
        descriptors: &mut Vec<TokenDescriptor>,
    ) -> Result<(), ClaudeError> {
        let Some(auth) = self.auth_context.as_ref() else {
            return Ok(());
        };
        let session_token = auth.get_session_ingress_token();
        if !session_token.is_empty() && !local_testing_mode {
            let fd = add_token_via_file_descriptor(
                &self.logger,
                session_token,
                "session ingress token",
                descriptors,
            )?;
            environment.push((
                OsString::from("CLAUDE_CODE_WEBSOCKET_AUTH_FILE_DESCRIPTOR"),
                OsString::from(fd.to_string()),
            ));
            match create_remote_session_directory() {
                Err(error) => self.logger.warn(
                    "Failed to create remote session directory",
                    claude_attrs!(
                        "error" => error.to_string(),
                        "path" => "/home/claude/.claude/remote",
                    ),
                ),
                Ok(()) => {
                    if let Err(error) = write_session_ingress_token_file(session_token) {
                        self.logger.warn(
                            "Failed to write session ingress token file",
                            claude_attrs!("error" => error.to_string(), "path" => SESSION_INGRESS_TOKEN_PATH),
                        );
                    } else {
                        environment.push((
                            OsString::from("CLAUDE_SESSION_INGRESS_TOKEN_FILE"),
                            OsString::from(SESSION_INGRESS_TOKEN_PATH),
                        ));
                        self.logger.info(
                            "Wrote session ingress token to file for bash access",
                            claude_attrs!("path" => SESSION_INGRESS_TOKEN_PATH),
                        );
                    }
                }
            }
        }

        if !auth.get_anthropic_oauth_token().is_empty() {
            let fd = add_token_via_file_descriptor(
                &self.logger,
                auth.get_anthropic_oauth_token(),
                "OAuth token",
                descriptors,
            )?;
            environment.push((
                OsString::from("CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR"),
                OsString::from(fd.to_string()),
            ));
        } else if !auth.get_anthropic_api_token().is_empty() {
            let fd = add_token_via_file_descriptor(
                &self.logger,
                auth.get_anthropic_api_token(),
                "API key",
                descriptors,
            )?;
            environment.push((
                OsString::from("CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR"),
                OsString::from(fd.to_string()),
            ));
        }
        Ok(())
    }

    /// 使用最终 child 环境中的 API base 和 session ingress token 启动本地 relay。
    async fn start_agent_proxy(
        &self,
        local_testing_mode: bool,
        environment: &[(OsString, OsString)],
    ) -> Result<Option<super::agent_proxy::AgentProxyHandle>, ClaudeError> {
        if !self.should_start_agent_proxy(local_testing_mode) {
            return Ok(None);
        }
        self.remove_stale_agent_proxy_token(Path::new(super::agent_proxy::STALE_RELAY_TOKEN_PATH))?;
        let token = self
            .auth_context
            .as_ref()
            .map(AuthContext::get_session_ingress_token)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| {
                ClaudeError::message(
                    "CCR upstream proxy requires a non-empty session ingress token",
                )
            })?;
        let api_base_url = environment
            .iter()
            .rev()
            .find(|(key, _)| key == OsStr::new("ANTHROPIC_BASE_URL"))
            .and_then(|(_, value)| value.to_str())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ClaudeError::message("CCR upstream proxy requires ANTHROPIC_BASE_URL")
            })?;
        let proxy = super::agent_proxy::AgentProxy::start(super::agent_proxy::AgentProxyConfig {
            api_base_url: api_base_url.to_owned(),
            session_id: self.session_id.clone(),
            token: token.to_owned(),
        })
        .await?;
        self.logger.info(
            "Prepared session ingress token for CCR upstream proxy relay",
            claude_attrs!("path" => "environment-manager memory"),
        );
        Ok(Some(proxy))
    }

    /// 删除旧 Claude relay 遗留的固定 token；非 NotFound 错误按 fail-closed 返回。
    pub(super) fn remove_stale_agent_proxy_token(&self, path: &Path) -> Result<(), ClaudeError> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                self.logger.warn(
                    "Failed to remove CCR upstream proxy token file",
                    claude_attrs!("error" => error.to_string(), "path" => path.display().to_string()),
                );
                Err(ClaudeError::message(format!(
                    "failed to remove stale CCR upstream proxy token: {error}"
                )))
            }
        }
    }

    /// spawn child、写 stdin、并发复制两个 stream，并只等待 direct child。
    #[allow(clippy::too_many_arguments)]
    async fn run_child(
        &self,
        cancellation: &CancellationToken,
        mut command: Command,
        resolved_program: OsString,
        command_lookup_error: Option<Vec<u8>>,
        local_testing_mode: bool,
        log_file: Arc<Mutex<File>>,
        agent_proxy: Option<&mut super::agent_proxy::AgentProxyHandle>,
    ) -> ExecutionRun {
        if let Some(error) = command_lookup_error {
            return ExecutionRun {
                error: Some(error),
                exit_code: -1,
                stdout: Vec::new(),
                stderr: Vec::new(),
            };
        }
        if cancellation.is_cancelled() {
            return ExecutionRun {
                error: Some(b"context canceled".to_vec()),
                exit_code: -1,
                stdout: Vec::new(),
                stderr: Vec::new(),
            };
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                return ExecutionRun {
                    error: Some(go_spawn_error(&resolved_program, &error).into_bytes()),
                    exit_code: -1,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                };
            }
        };
        drop(command);
        let mut stdin = child.stdin.take().expect("stdin configured as piped");
        if local_testing_mode {
            // Go 把 `bytes.Buffer` 交给 `os/exec`，child 提前退出造成的 EPIPE 会由
            // `skipStdinCopyError` 忽略，也不会产生应用日志；Rust 同样只尽力写入。
            let _ = stdin.write_all(b"update README.md with hello world").await;
        }
        drop(stdin);

        let stdout = child.stdout.take().expect("stdout configured as piped");
        let stderr = child.stderr.take().expect("stderr configured as piped");
        let verbose = self.cfg.as_ref().is_some_and(|cfg| cfg.verbose_logging);
        let stdout_task = tokio::spawn(copy_claude_stream(
            stdout,
            Arc::clone(&log_file),
            StreamDestination::Stdout,
            verbose,
            None,
        ));
        let log_writer = LogWriter::new(
            Logger::new(Arc::clone(&self.otel_log_handler)),
            LoggerLevel::Info,
            "[claude] ",
            claude_attrs!("process" => "claude"),
        );
        let stderr_task = tokio::spawn(copy_claude_stream(
            stderr,
            log_file,
            StreamDestination::Stderr,
            verbose,
            Some(log_writer),
        ));
        let proxy_failure = super::agent_proxy::wait_for_agent_proxy_failure(agent_proxy);
        tokio::pin!(proxy_failure);
        let mut forced_error = None;
        let status = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                // 有意只 kill direct child；孙进程持有 pipe 时 reader 可能继续等待，
                // 这正是 Go CommandContext 的兼容行为，不能擅自切换 process group。
                let _direct_child_kill = child.start_kill();
                child.wait().await
            }
            error = &mut proxy_failure => {
                forced_error = Some(error.into_bytes());
                let _direct_child_kill = child.start_kill();
                child.wait().await
            }
            result = child.wait() => result,
        };
        let stdout = join_stream(stdout_task).await;
        let stderr = join_stream(stderr_task).await;
        let mut stream_error = None;
        let stdout = match stdout {
            Ok(output) => output,
            Err(error) => {
                stream_error = Some(error);
                Vec::new()
            }
        };
        let stderr = match stderr {
            Ok(output) => output,
            Err(error) => {
                stream_error.get_or_insert(error);
                Vec::new()
            }
        };
        let process_exit_code = exit_code(&status);
        if let Some(error) = forced_error {
            return ExecutionRun {
                error: Some(error),
                exit_code: process_exit_code,
                stdout,
                stderr,
            };
        }
        match status {
            Ok(status) if status.success() && stream_error.is_none() => ExecutionRun {
                error: None,
                exit_code: process_exit_code,
                stdout,
                stderr,
            },
            Ok(status) => ExecutionRun {
                error: stream_error.or_else(|| Some(format_exit_status(status).into_bytes())),
                exit_code: process_exit_code,
                stdout,
                stderr,
            },
            Err(error) => ExecutionRun {
                error: Some(error.to_string().into_bytes()),
                exit_code: process_exit_code,
                stdout,
                stderr,
            },
        }
    }

    /// 写完成标记、记录活动/失败结果，并生成最终兼容错误。
    async fn finish_execution(
        &self,
        cancellation: &CancellationToken,
        log_file: &Arc<Mutex<File>>,
        run: ExecutionRun,
        duration_ms: i64,
    ) -> Result<(), ClaudeError> {
        if let Some(error) = run.error.as_deref() {
            if cancellation.is_cancelled() {
                append_log(
                    &self.logger,
                    log_file,
                    format!(
                        "\n[{}] Command stopped gracefully after {duration_ms}ms (context cancelled)\n",
                        timestamp()
                    )
                    .as_bytes(),
                )
                .await;
                self.logger.info(
                    "Claude Code stopped due to context cancellation",
                    claude_attrs!("duration_ms" => duration_ms),
                );
                if self.cfg.as_ref().is_some_and(|cfg| cfg.print_code_logs) {
                    // 三个终止分支在 Go 中是三个独立 slog source。这里有意不抽成
                    // helper，确保取消分支仍解析到 Go Execute 的第一处 sync 日志。
                    if let Err(error) = log_file.lock().await.sync_all().await {
                        self.logger.warn(
                            "Failed to sync log file before printing",
                            claude_attrs!("error" => error.to_string()),
                        );
                    }
                    self.print_code_logs().await;
                }
                return Err(ClaudeError::message(
                    "claude code stopped: context canceled",
                ));
            }

            // 日志文件和 JSON 属性必须是合法 UTF-8；只在这些观测旁路生成替换
            // 视图，最终返回的 ClaudeError 仍拼接原始 PathError 字节。
            let error_view = go_json_valid_utf8(error);
            append_log(
                &self.logger,
                log_file,
                format!(
                    "\n[{}] Command FAILED after {duration_ms}ms: {error_view}\n",
                    timestamp()
                )
                .as_bytes(),
            )
            .await;
            let stderr_tail = if run.stderr.is_empty() {
                "<nil>".to_owned()
            } else {
                tail_string(&run.stderr, 100 * 1024)
            };
            self.logger.error(
                "Claude code command failed",
                claude_attrs!(
                    "error" => &error_view,
                    "exit_code" => run.exit_code,
                    "duration_ms" => duration_ms,
                    "stderr_tail" => stderr_tail,
                ),
            );
            self.activity_recorder.record_activity(
                LogLevel::Error,
                LogCategory::Execute,
                "Claude Code execution failed".to_owned(),
                claude_attrs!(
                    "error" => &error_view,
                    "exit_code" => run.exit_code,
                    "duration_ms" => duration_ms,
                ),
            );
            if let Err(record_error) = self
                .activity_recorder
                .record_failure_result(&ClaudeCodeExecutionError { fatal: true })
                .await
            {
                self.logger.error(
                    "Failed to record Claude Code failure result",
                    claude_attrs!("error" => record_error.to_string()),
                );
            }
            if self.cfg.as_ref().is_some_and(|cfg| cfg.print_code_logs) {
                // 对应 Go 的普通失败分支；独立调用点也是 source 兼容协议的一部分。
                if let Err(error) = log_file.lock().await.sync_all().await {
                    self.logger.warn(
                        "Failed to sync log file before printing",
                        claude_attrs!("error" => error.to_string()),
                    );
                }
                self.print_code_logs().await;
            }
            let mut message = b"claude code command failed: ".to_vec();
            message.extend_from_slice(error);
            return Err(ClaudeError::message_bytes(message));
        }

        append_log(
            &self.logger,
            log_file,
            format!(
                "\n[{}] Command completed successfully after {duration_ms}ms\n",
                timestamp()
            )
            .as_bytes(),
        )
        .await;
        self.logger.info(
            "Claude Code execution completed",
            claude_attrs!("duration_ms" => duration_ms, "output_bytes" => run.stdout.len()),
        );
        if self.cfg.as_ref().is_some_and(|cfg| cfg.print_code_logs) {
            // 对应 Go 的成功分支；保留第三个独立 source，不能与上面两处合并。
            if let Err(error) = log_file.lock().await.sync_all().await {
                self.logger.warn(
                    "Failed to sync log file before printing",
                    claude_attrs!("error" => error.to_string()),
                );
            }
            self.print_code_logs().await;
        }
        Ok(())
    }

    /// 打印 `/tmp/claude-code.log`，保持 Go 的标题、分隔线和末尾换行。
    async fn print_code_logs(&self) {
        let data = match tokio::fs::read(CLAUDE_CODE_LOG_PATH).await {
            Ok(data) => data,
            Err(error) => {
                self.logger.warn(
                    "Failed to read Claude Code logs",
                    claude_attrs!("path" => CLAUDE_CODE_LOG_PATH, "error" => error.to_string()),
                );
                return;
            }
        };
        if data.is_empty() {
            self.logger.info(
                "Claude Code log file is empty",
                claude_attrs!("path" => CLAUDE_CODE_LOG_PATH),
            );
            return;
        }
        let separator = format!("{}\n", "=".repeat(80));
        let mut stdout = tokio::io::stdout();
        let _ = stdout.write_all(format!("\n{separator}").as_bytes()).await;
        let _ = stdout
            .write_all(b"CLAUDE CODE LOGS (/tmp/claude-code.log)\n")
            .await;
        let _ = stdout.write_all(separator.as_bytes()).await;
        let _ = stdout.write_all(&data).await;
        if data.last() != Some(&b'\n') {
            let _ = stdout.write_all(b"\n").await;
        }
        let _ = stdout.write_all(format!("{separator}\n").as_bytes()).await;
    }

    /// 幂等删除 Bash 使用的 session ingress token。
    async fn destroy_inner(&self) -> Result<(), ClaudeError> {
        self.logger.info(
            "Destroying ClaudeCodeExecutor",
            claude_attrs!("session_id" => &self.session_id),
        );
        remove_token_file(&self.logger, Path::new(SESSION_INGRESS_TOKEN_PATH), false);
        let local_testing_mode = self.cfg.as_ref().is_some_and(|cfg| cfg.local_testing_mode);
        if self.should_start_agent_proxy(local_testing_mode) {
            remove_token_file(
                &self.logger,
                Path::new(super::agent_proxy::STALE_RELAY_TOKEN_PATH),
                true,
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Executor for ClaudeCodeExecutor {
    /// 执行 Claude Code 并保留 direct-child 取消语义。
    async fn execute(&self, cancellation: &CancellationToken) -> Result<(), ClaudeError> {
        self.execute_inner(cancellation).await
    }

    /// 清理 token 文件；取消参数不影响幂等安全清理。
    async fn destroy(&self, _cancellation: &CancellationToken) -> Result<(), ClaudeError> {
        self.destroy_inner().await
    }
}

/// 把高位 token source FD 安装为 child 的 3/4/5，并依赖 exec 关闭高位 CLOEXEC FD。
pub(super) fn install_token_descriptors(
    command: &mut Command,
    descriptors: &[TokenDescriptor],
) -> Result<(), ClaudeError> {
    if descriptors.is_empty() {
        return Ok(());
    }
    let mappings = descriptors
        .iter()
        .map(|descriptor| (descriptor.source.as_raw_fd(), descriptor.target))
        .collect::<Vec<_>>();
    // nix 0.30 的任意目标 `dup2` API 要求构造 `OwnedFd`，但 3/4/5 在此可能尚未打开，
    // 伪造所有权反而会破坏 IO safety，因此仅这里保留原始 `dup2`。
    // SAFETY: closure 只调用 async-signal-safe 的 `dup2` 与 errno 读取；source FD
    // 在 `spawn` 返回前由 `descriptors` 保持存活，且都复制到 64 以上，不会被目标
    // 3/4/5 的顺序映射覆盖。dup2 会清除目标 CLOEXEC，使其按约定穿过 exec。
    unsafe {
        command.as_std_mut().pre_exec(move || {
            for (source, target) in &mappings {
                if libc::dup2(*source, *target) == -1 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    Ok(())
}

/// 对应 Go：`ClaudeCodeExecutor.Execute` 中的 `os.MkdirAll`。
///
/// 只负责以 0700 创建缺失目录，使调用方可以把目录创建失败与 token 写入失败记录
/// 为两个不同的兼容事件；对既有目录不主动 chmod，与 Go `MkdirAll` 保持一致。
fn create_remote_session_directory() -> io::Result<()> {
    let directory = Path::new(SESSION_INGRESS_TOKEN_PATH)
        .parent()
        .expect("session token path has parent");
    let mut directory_builder = std::fs::DirBuilder::new();
    directory_builder.recursive(true).mode(0o700);
    directory_builder.create(directory)
}

/// 以 0600 创建或覆盖 session ingress token 文件。
///
/// 目录必须已由 `create_remote_session_directory` 成功准备；拆分这两个阶段是为了
/// 保留 Go 在 mkdir 和 WriteFile 失败时不同的消息、属性与分支顺序。
fn write_session_ingress_token_file(token: &str) -> io::Result<()> {
    let path = Path::new(SESSION_INGRESS_TOKEN_PATH);
    let mut options = StdOpenOptions::new();
    options.create(true).truncate(true).write(true).mode(0o600);
    let mut file = options.open(path)?;
    file.write_all(token.as_bytes())
}

/// 打开 append-only Claude 日志，新文件 mode 为 0600。
async fn open_claude_log() -> Result<File, ClaudeError> {
    let mut options = OpenOptions::new();
    options.create(true).write(true).append(true).mode(0o600);
    options
        .open(CLAUDE_CODE_LOG_PATH)
        .await
        .map_err(|error| ClaudeError::message(format!("failed to create claude-code.log: {error}")))
}

/// 对应 Go：`ClaudeCodeExecutor.Execute` 写入并同步执行 header 的步骤。
///
/// 写入或 sync 失败只记录 warning，不阻止 Claude 启动；两次 I/O 必须按顺序都尝试，
/// 因为 Go 即使 header Write 失败也继续调用 `Sync`。
async fn write_log_header(
    logger: &Logger,
    log_file: &Arc<Mutex<File>>,
    command: &str,
    args: &[String],
    session_id: &str,
    working_dir: Option<&Path>,
) {
    let working_dir = working_dir
        .map(|directory| directory.display().to_string())
        .unwrap_or_else(|| "(current directory)".to_owned());
    let header = format!(
        "\n========================================\n[{}] Executing Claude Code\nCommand: {command}\nArgs: [{}]\nSession: {session_id}\nWorking Dir: {}\n========================================\n",
        timestamp(),
        args.join(" "),
        working_dir,
    );
    let mut file = log_file.lock().await;
    if let Err(error) = file.write_all(header.as_bytes()).await {
        logger.warn(
            "Failed to write header to log file",
            claude_attrs!("error" => error.to_string()),
        );
    }
    if let Err(error) = file.sync_all().await {
        logger.warn(
            "Failed to sync log file",
            claude_attrs!("error" => error.to_string()),
        );
    }
}

/// 对应 Go：`ClaudeCodeExecutor.Execute` 中关闭 `/tmp/claude-code.log` 的 defer。
///
/// 所有 stream task 与完成日志都结束后，Arc 应只剩调用方这一份。消费 Tokio File
/// 并显式调用 `close(2)`，使罕见关闭错误也能按 Go 的 WARN 文案被观察到。
async fn close_claude_log(logger: &Logger, log_file: Arc<Mutex<File>>) {
    let mutex = match Arc::try_unwrap(log_file) {
        Ok(mutex) => mutex,
        Err(still_shared) => {
            // run_child 已 join 两个 stream task；这一分支仅防未来重构遗留共享引用。
            // 无法取得唯一所有权时让最后一个 Arc 走安全析构，避免强行关闭活跃 FD。
            drop(still_shared);
            return;
        }
    };
    let file = mutex.into_inner();
    let raw_fd = file.into_std().await.into_raw_fd();
    if let Err(error) = close_owned_fd(raw_fd) {
        logger.warn(
            "Failed to close claude-code.log",
            claude_attrs!("error" => error.to_string()),
        );
    }
}

/// 写 `/tmp/claude-command` 的 shell 可复制形式；失败只记录 warning。
async fn write_command_file(logger: &Logger, command: &str, args: &[String]) {
    let path = Path::new("/tmp/claude-command");
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true).mode(0o644);
    let result = match options.open(path).await {
        Ok(mut file) => {
            file.write_all(shell_command(command, args).as_bytes())
                .await
        }
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        // Go 的 `os.WriteFile` 只有一个错误返回点；open/write 两阶段必须汇合到同一
        // Logger callsite，否则同一个 Go source 会被两个 Rust consumer 消费。
        logger.warn(
            "Failed to write claude command to file",
            claude_attrs!("error" => error.to_string()),
        );
    } else {
        logger.info(
            "Claude command written to /tmp/claude-command",
            Default::default(),
        );
    }
}

/// 在启用 `ENV_MANAGER_STREAM_CC_LOGS` 时启动从文件末尾跟随的日志转发。
async fn start_cc_log_tailer(logger: &Logger, session_id: &str) -> Option<ActiveTailer> {
    std::env::var_os("ENV_MANAGER_STREAM_CC_LOGS")?;
    let initialized =
        Tailer::new(CLAUDE_CODE_LOG_PATH, Duration::from_millis(100)).and_then(|tailer| {
            let lines = tailer.take_lines()?;
            Ok((Arc::new(tailer), lines))
        });
    let (tailer, mut lines) = match initialized {
        Ok(initialized) => initialized,
        Err(error) => {
            // Rust 的 receiver 所有权检查是 Go channel 不需要的结构步骤；无论失败
            // 来自 metadata 还是 receiver 初始化，都汇入 Go `NewTailer` 唯一日志点。
            logger.warn(
                "Failed to start CC log tailer, logs will not stream",
                claude_attrs!("error" => error.to_string(), "path" => CLAUDE_CODE_LOG_PATH),
            );
            return None;
        }
    };
    if let Err(error) = tailer.start(CancellationToken::new()).await {
        logger.warn(
            "Failed to start CC log tailer goroutine",
            claude_attrs!("error" => error.to_string(), "path" => CLAUDE_CODE_LOG_PATH),
        );
        return None;
    }
    let logger = logger.clone();
    let session_id = session_id.to_owned();
    let hostname = std::env::var("HOSTNAME").unwrap_or_default();
    let consumer = tokio::spawn(async move {
        while let Some(line) = lines.recv().await {
            if let Some(error) = line.error {
                logger.error(
                    "CC log tailer error",
                    claude_attrs!("error" => error, "session_id" => &session_id, "hostname" => &hostname),
                );
                return;
            }
            logger.info(
                "CC log",
                claude_attrs!("line" => line.text, "session_id" => &session_id, "hostname" => &hostname),
            );
        }
    });
    Some(ActiveTailer { tailer, consumer })
}

/// 请求 tailer 停止并等待接收任务消费完 channel 关闭。
async fn stop_cc_log_tailer(active: Option<ActiveTailer>) {
    let Some(active) = active else {
        return;
    };
    active.tailer.stop().await;
    let _ = active.consumer.await;
}

/// stream 输出目标，用于可选镜像到父 stdout/stderr。
#[derive(Clone, Copy)]
enum StreamDestination {
    Stdout,
    Stderr,
}

/// 原样复制一个 child pipe 到内存、共享日志、可选父 stdio 与结构化行 writer。
async fn copy_claude_stream<R>(
    mut reader: R,
    log_file: Arc<Mutex<File>>,
    destination: StreamDestination,
    verbose: bool,
    mut structured_writer: Option<LogWriter>,
) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut captured = Vec::new();
    let mut buffer = vec![0_u8; STREAM_BUFFER_SIZE];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(captured);
        }
        let bytes = &buffer[..read];
        captured.extend_from_slice(bytes);
        log_file.lock().await.write_all(bytes).await?;
        if verbose {
            match destination {
                StreamDestination::Stdout => tokio::io::stdout().write_all(bytes).await?,
                StreamDestination::Stderr => tokio::io::stderr().write_all(bytes).await?,
            }
        }
        if let Some(writer) = structured_writer.as_mut() {
            writer.write_all(bytes)?;
        }
    }
}

/// 汇合 stream task，将 panic/cancel 与 I/O 错误压成单层原始字节。
///
/// 当前 Tokio join/I/O 文案都是 UTF-8，但统一使用字节类型可避免它与 spawn 的
/// 非 UTF-8 `PathError` 在 `ExecutionRun` 汇合时被迫提前转换。
async fn join_stream(task: JoinHandle<io::Result<Vec<u8>>>) -> Result<Vec<u8>, Vec<u8>> {
    task.await
        .map_err(|error| error.to_string().into_bytes())?
        .map_err(|error| error.to_string().into_bytes())
}

/// 对应 Go：`Execute` 内部 `writeCompletion` closure。
///
/// 三种 child 结局共用同一 closure/source；写入失败沿用历史上略显宽泛的
/// `Failed to write header` 文案，并且绝不覆盖取消或 child 失败的主结果。
async fn append_log(logger: &Logger, log_file: &Arc<Mutex<File>>, bytes: &[u8]) {
    if let Err(error) = log_file.lock().await.write_all(bytes).await {
        logger.warn(
            "Failed to write header to log file",
            claude_attrs!("error" => error.to_string()),
        );
    }
}

/// 关闭一个已经转移出 Rust RAII 包装的 owned FD，并返回 Linux errno。
///
/// 调用方必须先通过 `into_raw_fd` 取得唯一所有权；函数无重试，因为 Linux 上
/// close 返回 EINTR 后 FD 状态不确定，重试可能误关已被其他线程复用的描述符。
fn close_owned_fd(raw_fd: i32) -> io::Result<()> {
    close(raw_fd).map_err(io::Error::from)
}

/// 生成日志 header 使用的本地毫秒时间戳。
fn timestamp() -> String {
    chrono::Local::now()
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

/// 把 command 与参数转换为 Go `shellCommand` 的单行表示。
fn shell_command(command: &str, args: &[String]) -> String {
    std::iter::once(command)
        .chain(args.iter().map(String::as_str))
        .map(shell_quote)
        .collect::<Vec<_>>()
        .join(" ")
}

/// 只在 Go 实现检查的空白或引号字符出现时使用 POSIX 单引号转义。
fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_owned();
    }
    if !value
        .bytes()
        .any(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\"' | b'\''))
    {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// 复刻 Go `tailString` 对任意 stderr 字节的截断与 UTF-8 处理。
///
/// 未截断时 Go 把原始 string 交给 slog/JSON，后者逐个无效字节写 U+FFFD；发生截断
/// 时则调用 `strings.ToValidUTF8(..., "")`，删除尾部切片中的所有无效字节。
pub(super) fn tail_string(value: &[u8], max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return go_json_valid_utf8(value);
    }
    delete_invalid_utf8(&value[value.len() - max_bytes..])
}

/// 把每个无效 UTF-8 字节替换为一个 U+FFFD，匹配 Go JSON 字符串编码。
fn go_json_valid_utf8(value: &[u8]) -> String {
    let mut output = String::new();
    let mut remaining = value;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                output.push_str(valid);
                break;
            }
            Err(error) => {
                let valid = &remaining[..error.valid_up_to()];
                // `valid_up_to` 保证该前缀位于 UTF-8 字符边界。
                output.push_str(std::str::from_utf8(valid).unwrap_or_default());
                output.push('\u{fffd}');
                remaining = &remaining[error.valid_up_to() + 1..];
            }
        }
    }
    output
}

/// 删除所有无效 UTF-8 字节，保留有效 U+FFFD 字符和其他有效片段。
fn delete_invalid_utf8(value: &[u8]) -> String {
    let mut output = String::new();
    let mut remaining = value;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                output.push_str(valid);
                break;
            }
            Err(error) => {
                let valid = &remaining[..error.valid_up_to()];
                output.push_str(std::str::from_utf8(valid).unwrap_or_default());
                remaining = &remaining[error.valid_up_to() + 1..];
            }
        }
    }
    output
}

/// 把 Tokio 子进程等待结果归一化为 Go `exitCode` 的三态返回值。
///
/// 正常退出返回系统 exit code（成功即 0）；被信号终止或等待本身失败返回 -1。
/// 该函数只解释状态，不吞掉调用方需要记录的原始 I/O 错误。
fn exit_code(status: &std::io::Result<ExitStatus>) -> i32 {
    status
        .as_ref()
        .ok()
        .and_then(ExitStatus::code)
        .unwrap_or(-1)
}

/// 对应 Go：`ClaudeCodeExecutor.Destroy` 清理 session/CCR token 的两个分支。
///
/// `ccr_token` 只选择兼容文案，不改变删除语义。两个文件共用一个 Rust 调用点，
/// 但条件表达式保留四个静态消息，使 source resolver 能分别映射 Go 的四个位置；
/// `NotFound` 与 Go `os.IsNotExist` 一样保持静默。
fn remove_token_file(logger: &Logger, path: &Path, ccr_token: bool) {
    match std::fs::remove_file(path) {
        Ok(()) => logger.info(
            if ccr_token {
                "Removed CCR upstream proxy token file"
            } else {
                "Removed session ingress token file"
            },
            claude_attrs!("path" => path.display().to_string()),
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => logger.warn(
            if ccr_token {
                "Failed to remove CCR upstream proxy token file"
            } else {
                "Failed to remove session ingress token file"
            },
            claude_attrs!("error" => error.to_string(), "path" => path.display().to_string()),
        ),
    }
}
