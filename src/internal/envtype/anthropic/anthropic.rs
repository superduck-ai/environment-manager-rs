//! Anthropic 环境的初始化编排与外部副作用实现。
//!
//! 负责 sources 模式选择、Baku 模板、hook、语言脚本、用户 init script、Git 命令顺序
//! 和非 fatal 边界。

use std::{
    collections::BTreeMap,
    env,
    fs::{self, DirBuilder, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde_json::Value;
use tokio::{net::TcpStream, process::Command};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use crate::internal::{
    api::{InitScriptError, LogCategory, LogLevel as ApiLogLevel, SessionError},
    auth::AuthContext,
    config::{SessionMode, StartupContext},
    envtype::{EnvironmentType, Registration, shared},
    process::{CaptureMode, execute_script, run_command},
    session::{ActivityRecorder, ActivityRecorderError, NoopActivityRecorder},
    sources::{ProcessMode, new_source_handler_manager},
    util::{BoxError, Stopper, format_go_duration, output_streamer},
};

use super::{
    AnthropicEnvironmentType, AnthropicError, Config, Language, RunScriptError, decode_config,
    install_scripts,
    types::{HookConfig, ScopedActivityRecorder, SharedScopedRecorder, boxed},
};

/// Baku 镜像内置 Vite 模板目录。
const BAKU_TEMPLATE_DIR: &str = "/opt/baku-templates/vite-template";
/// Baku 工作项目固定目录。
const BAKU_PROJECT_DIR: &str = "/home/claude/project";
/// 脚本日志和失败结果最多保留的尾部字节数。
const OUTPUT_TAIL_BYTES: usize = 4096;

/// 保存 init script 的长任务 stopper，并实现的覆盖范围。
///
/// 正常初始化结束时 [`Self::stop`] 会异步等待后台 callback；若整个 initialize future
/// 被 abort 或因 panic 展开，Rust `Drop` 不能等待，但仍会同步传播取消，避免周期记录器
/// 在已经失去所属初始化任务后继续运行。
pub(super) struct InitActivityStopGuard {
    stopper: Option<Arc<dyn Stopper>>,
}

impl InitActivityStopGuard {
    /// 创建尚未持有 stopper 的 guard；init script 真正开始时才通过 [`Self::set`] 安装。
    pub(super) fn empty() -> Self {
        Self { stopper: None }
    }

    /// 保存已启动的长任务 stopper，使后续任意退出路径都至少发出同步取消。
    pub(super) fn set(&mut self, stopper: Arc<dyn Stopper>) {
        // 同一段 Initialize 只会启动一次 init script。若未来控制流变化导致重复设置，
        // 先取消旧 stopper，避免静默遗留后台任务，同时让新 stopper 接管 guard。
        if let Some(previous) = self.stopper.replace(stopper) {
            previous.request_stop();
        }
    }

    /// 正常控制流完成时取消并等待在途 callback，保持 Stop 的完成边界。
    pub(super) async fn stop(mut self) {
        if let Some(stopper) = self.stopper.take() {
            stopper.stop().await;
        }
    }
}

impl Drop for InitActivityStopGuard {
    /// initialize future 被丢弃时同步请求停止；无法在 `Drop` 中阻塞 Tokio worker。
    fn drop(&mut self) {
        if let Some(stopper) = self.stopper.take() {
            stopper.request_stop();
        }
    }
}

/// Anthropic 环境在注册表中的静态元数据。
pub static REGISTRATION: LazyLock<Registration> = LazyLock::new(|| Registration {
    type_name: "anthropic".to_owned(),
    factory: Some(Arc::new(|data| {
        new(data)
            .map(|environment| Box::new(environment) as Box<dyn EnvironmentType>)
            .map_err(boxed)
    })),
    description: "Anthropic environment with init scripts and environment variables".to_owned(),
});

///
/// 从后端 JSON 创建 Anthropic 环境。
///
/// # 参数
/// - `data`：Anthropic 环境原始 JSON；函数只在解码期间借用。
///
/// # 返回
/// 配置有效时返回环境；JSON 解码失败时返回带稳定文本的 `AnthropicError`。
pub fn new(data: &[u8]) -> Result<AnthropicEnvironmentType, AnthropicError> {
    Ok(AnthropicEnvironmentType {
        config: Some(decode_config(data)?),
        startup_context: None,
        auth_context: None,
        session_mode: SessionMode::New,
    })
}

#[async_trait]
impl ActivityRecorder for ScopedActivityRecorder<'_> {
    /// 同步转发当前 initialize 作用域内的活动日志。
    fn record_activity(
        &self,
        level: ApiLogLevel,
        category: LogCategory,
        message: String,
        extra: BTreeMap<String, Value>,
    ) {
        self.inner.record_activity(level, category, message, extra);
    }

    /// 同步创建并转发长任务 stopper。
    fn record_long_running_activity(
        &self,
        category: LogCategory,
        message: String,
    ) -> Arc<dyn Stopper> {
        self.inner.record_long_running_activity(category, message)
    }

    /// 在借用仍有效的同一 async 作用域内转发失败结果。
    async fn record_failure_result(
        &self,
        session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        self.inner.record_failure_result(session_error).await
    }
}

impl AnthropicEnvironmentType {
    /// 更新启动上下文。
    pub fn set_startup_context(&mut self, context: Option<StartupContext>) {
        self.startup_context = context;
    }

    /// 更新认证上下文。
    pub fn set_auth_context(&mut self, context: Option<AuthContext>) {
        self.auth_context = context;
    }

    /// 更新 session 模式。
    pub fn set_session_mode(&mut self, mode: SessionMode) {
        self.session_mode = mode;
    }

    /// 判断启动上下文是否声明 Baku 子类型。
    #[must_use]
    pub fn is_baku_environment(&self) -> bool {
        self.startup_context
            .as_ref()
            .is_some_and(|context| context.environment_sub_type == "baku")
    }

    /// 返回配置的工作目录。
    ///
    /// 手工构造 `None` 配置后调用该 accessor 会 panic；正常工厂
    /// 和成功解码路径始终存在配置。
    #[must_use]
    pub fn get_cwd(&self) -> &str {
        &self
            .config
            .as_ref()
            .expect("anthropic environment config is required")
            .cwd
    }

    /// 返回 Claude 环境变量的独立副本。
    #[must_use]
    pub fn get_claude_environment_variables(&self) -> BTreeMap<String, String> {
        self.config
            .as_ref()
            .expect("anthropic environment config is required")
            .environment
            .clone()
    }

    /// 完成 Anthropic 环境初始化。
    ///
    /// sources 和语言/init 脚本错误是 fatal；Git proxy、Claude hook 和最终 Baku
    /// dev server 是 best effort。文件系统副作用不因取消回滚，外部命令只杀直接 child。
    pub async fn initialize(
        &mut self,
        cancellation: &CancellationToken,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), AnthropicError> {
        let noop = NoopActivityRecorder;
        let activity_recorder = activity_recorder.unwrap_or(&noop);
        if self.config.is_none() {
            return Err(AnthropicError::message(
                "environment configuration is required",
            ));
        }

        let has_init_script = self
            .config
            .as_ref()
            .is_some_and(|config| !config.init_script.is_empty());
        let has_sources = self
            .startup_context
            .as_ref()
            .is_some_and(|context| !context.sources.is_empty());
        let has_cwd = self
            .config
            .as_ref()
            .is_some_and(|config| !config.cwd.is_empty());
        let is_baku = self.is_baku_environment();
        let should_run_setup =
            matches!(self.session_mode, SessionMode::New | SessionMode::SetupOnly);

        tracing::info!(
            session_mode = %self.session_mode.as_str(),
            has_sources,
            language_count = self
                .config
                .as_ref()
                .map_or(0, |config| config.languages.len()),
            has_init_script,
            has_cwd,
            is_baku,
            "Initializing Anthropic environment"
        );

        if has_sources {
            let mode = if should_run_setup {
                Some(ProcessMode::from("fresh"))
            } else if self.session_mode == SessionMode::ResumeCached {
                tracing::info!("Fast resume: Using existing repository clones");
                Some(ProcessMode::from("allow-prefetched"))
            } else {
                tracing::info!("Fast resume: Environment already configured");
                None
            };
            if let Some(mode) = mode {
                self.process_sources(
                    cancellation,
                    activity_recorder,
                    mode,
                    self.session_mode == SessionMode::ResumeCached,
                )
                .await?;
            }
        }

        if is_baku && !has_sources {
            if should_run_setup {
                let project_dir =
                    self.initialize_baku_project(cancellation)
                        .await
                        .map_err(|error| {
                            AnthropicError::with_source("failed to initialize Baku project", error)
                        })?;
                if !project_dir.is_empty() {
                    self.config_mut().cwd.clone_from(&project_dir);
                    tracing::info!(project_dir = %project_dir, "Initial project from Baku templates");
                }
            } else {
                match self.find_existing_baku_project() {
                    Ok(project_dir) if !project_dir.is_empty() => {
                        self.config_mut().cwd.clone_from(&project_dir);
                        tracing::info!(project_dir = %project_dir, "Baku environment detected");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(error = %error, "Baku environment detected")
                    }
                }
            }

            if self
                .auth_context
                .as_ref()
                .is_some_and(AuthContext::has_supabase)
            {
                let cwd = self.config_ref().cwd.clone();
                if let Err(error) = self.write_supabase_env_files(cancellation, &cwd).await {
                    tracing::warn!(error = %error, "Failed to write .env.local file");
                }
            }
        }

        let cwd = self.config_ref().cwd.clone();
        if !cwd.is_empty() {
            tracing::info!(cwd = %cwd, "Setting working directory");
            create_dir_all_with_mode(&cwd, 0o755).map_err(|error| {
                AnthropicError::formatted_with_source(
                    format!("failed to create directory {cwd}: {error}"),
                    error,
                )
            })?;
            env::set_current_dir(&cwd).map_err(|error| {
                AnthropicError::formatted_with_source(
                    format!("failed to change directory to {cwd}: {error}"),
                    error,
                )
            })?;
            tracing::info!(cwd = %cwd, "Working directory set successfully");
        }

        let hooks = if is_baku {
            HookConfig {
                settings_data: shared::BAKU_SETTINGS_JSON.to_vec(),
                hook_name: "stop-hook-baku.sh".to_owned(),
                hook_data: shared::BAKU_STOP_HOOK_SCRIPT.to_vec(),
            }
        } else {
            HookConfig {
                settings_data: shared::DEFAULT_SETTINGS_JSON.to_vec(),
                hook_name: "stop-hook-git-check.sh".to_owned(),
                hook_data: shared::STOP_HOOK_SCRIPT.to_vec(),
            }
        };
        if let Err(error) = self.bootstrap_hooks_in_all_dirs(cancellation, hooks).await {
            if is_baku {
                tracing::warn!(error = %error, "Failed to bootstrap Baku settings");
            } else {
                tracing::warn!(error = %error, "Failed to bootstrap Claude settings");
            }
        }

        if !self.config_ref().languages.is_empty() {
            if should_run_setup {
                // 初始化和语言安装保留独立事件，便于定位所处阶段。
                tracing::info!(
                    language_count = self.config_ref().languages.len(),
                    "Installing languages"
                );
                self.install_languages(cancellation)
                    .await
                    .map_err(|error| {
                        AnthropicError::with_source("failed to install languages", error)
                    })?;
            } else {
                tracing::info!(
                    language_count = self.config_ref().languages.len(),
                    "Fast resume: Skipping language installation"
                );
            }
        }

        // stopper 必须存活到整个 initialize 返回，而不只到脚本结束。先在内部 future
        // 中保留所有提前返回，再统一异步 stop，确保所有路径都等待 invoker 完成。
        let mut init_activity_guard = InitActivityStopGuard::empty();
        let initialization_result = async {
            if has_init_script {
                if should_run_setup {
                    tracing::info!("Running init script");
                    init_activity_guard.set(activity_recorder.record_long_running_activity(
                        LogCategory::Init,
                        "Running task setup script".to_owned(),
                    ));
                    let script = self.config_ref().init_script.clone();
                    if let Err(error) = self.run_init_script(cancellation, &script).await {
                        activity_recorder.record_activity(
                            ApiLogLevel::Error,
                            LogCategory::Init,
                            format!("Init script failed: {error}"),
                            BTreeMap::new(),
                        );
                        let session_error = InitScriptError {
                            exit_code: error.exit_code.unwrap_or(1),
                            output: error.output.clone(),
                        };
                        let _ = activity_recorder
                            .record_failure_result(&session_error)
                            .await;
                        return Err(AnthropicError::with_source(
                            "failed to run init script",
                            error,
                        ));
                    }
                } else {
                    tracing::info!("Fast resume: Skipping task setup script");
                }
            }

            if is_baku && !has_sources {
                let cwd = self.config_ref().cwd.clone();
                if let Err(error) = self.start_dev_server(cancellation, &cwd).await {
                    tracing::warn!(error = %error, "Failed to start dev server");
                }
            }

            tracing::info!("Anthropic environment initialization completed");
            Ok(())
        }
        .await;
        init_activity_guard.stop().await;
        initialization_result
    }

    /// 创建并在局部作用域内使用 sources manager。
    ///
    /// recorder 生命周期仅为本函数；`SourceHandlerManager<'_>` 和默认 Git handler
    /// 通过显式生命周期参数保留该借用，无法被返回或 spawn。源码处理错误 fatal，
    /// Git proxy 设置错误只记录 warning，与 初始化边界一致。
    async fn process_sources(
        &self,
        cancellation: &CancellationToken,
        activity_recorder: &(dyn ActivityRecorder + Send + Sync),
        process_mode: ProcessMode,
        should_validate: bool,
    ) -> Result<(), AnthropicError> {
        let context = self
            .startup_context
            .as_ref()
            .expect("has_sources guarantees startup context");
        let scoped: SharedScopedRecorder<'_> = Arc::new(ScopedActivityRecorder {
            inner: activity_recorder,
        });
        let manager = new_source_handler_manager(
            self.config_ref().cwd.clone(),
            self.auth_context.clone().map(Arc::new),
            context.api_base_url.clone(),
            None,
            Some(scoped),
            process_mode,
            should_validate,
        )
        .map_err(|error| {
            AnthropicError::with_source("failed to create source handler manager", error)
        })?;

        if let Err(error) = manager
            .process_sources(cancellation, &context.sources)
            .await
        {
            // 明确结束 recorder 借用后再构造上层错误；即使 future 被取消，Rust 的
            // 局部变量销毁顺序也会先释放 manager 中的 handler 与 recorder wrapper。
            drop(manager);
            return Err(AnthropicError::with_source(
                "failed to process sources",
                error,
            ));
        }
        tracing::info!(
            source_count = context.sources.len(),
            "Sources processed successfully"
        );

        if self.session_mode != SessionMode::SetupOnly {
            if let Err(error) = manager
                .setup_git_proxy_after_sources_processed(cancellation, &context.sources)
                .await
            {
                tracing::warn!(error = %error, "Failed to setup git proxy");
            } else {
                tracing::info!(
                    source_count = context.sources.len(),
                    "Git proxy setup completed"
                );
            }
        }
        // 与 局部 manager 的生命周期一致；类型系统同时保证 recorder wrapper
        // 不可能越过当前 initialize 调用。
        drop(manager);
        Ok(())
    }

    /// 执行用户初始化脚本并保留 stdout/stderr 合并尾部。
    pub async fn run_init_script(
        &self,
        cancellation: &CancellationToken,
        script_content: &str,
    ) -> Result<String, RunScriptError> {
        tracing::info!(
            script_length = script_content.len(),
            "Running initialization script"
        );
        let output = Arc::new(Mutex::new(Vec::<u8>::new()));
        let captured = Arc::clone(&output);
        let streamer = output_streamer(move |stream, data| {
            tracing::debug!(stream = %stream, data = %String::from_utf8_lossy(data), "Init script output");
            append_tail(&captured, data);
            Ok::<(), io::Error>(())
        });

        let result = match execute_script(
            cancellation,
            script_content,
            "init-script-*.sh",
            streamer,
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                return Err(RunScriptError {
                    output: tail_to_string(&output),
                    error: AnthropicError::with_source("failed to execute init script", error),
                    exit_code: None,
                });
            }
        };
        if let Some(error) = result.error {
            let message = format!(
                "init script failed (exit code {}): {}",
                result.exit_code, error
            );
            return Err(RunScriptError {
                output: tail_to_string(&output),
                error: AnthropicError::formatted_with_source(message, error),
                exit_code: Some(result.exit_code),
            });
        }

        tracing::info!("Successfully executed init script");
        Ok(tail_to_string(&output))
    }

    /// 并发安装配置中的全部语言，并按完成顺序拼接错误。
    pub async fn install_languages(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), AnthropicError> {
        let languages = self.config_ref().languages.clone();
        // 该日志与 Initialize 外层日志同文案、同属性，保留第二个独立
        // runtime caller 以区分 installLanguages 阶段。
        tracing::info!(language_count = languages.len(), "Installing languages");
        let started = Instant::now();
        let mut futures = FuturesUnordered::new();
        for language in languages.clone() {
            futures.push(async move {
                let language_started = Instant::now();
                tracing::info!(language = %language.name, version = %language.version, "Installing language in parallel");
                let result = self.install_language(cancellation, &language).await;
                (language, language_started.elapsed(), result)
            });
        }

        let mut errors = Vec::new();
        while let Some((language, duration, result)) = futures.next().await {
            if let Err(error) = result {
                tracing::error!(
                    language = %language.name,
                    version = %language.version,
                    duration = %format_go_duration(duration),
                    error = %error,
                    "Failed to install language"
                );
                errors.push(format!(
                    "failed to install {} {}: {error}",
                    language.name, language.version
                ));
            } else {
                // installLanguage 自身成功时还会记录同文案日志；此处保留
                // installLanguages 后台任务 完成点的独立 runtime source。
                tracing::info!(
                    language = %language.name,
                    version = %language.version,
                    duration = %format_go_duration(duration),
                    "Successfully installed language"
                );
            }
        }
        if !errors.is_empty() {
            tracing::error!(
                error_count = errors.len(),
                total_count = languages.len(),
                duration = %format_go_duration(started.elapsed()),
                errors = %errors.join(", "),
                "Failed to install some languages"
            );
            return Err(AnthropicError::message(format!(
                "failed to install languages: {}",
                errors.join(", ")
            )));
        }
        tracing::info!(
            language_count = languages.len(),
            duration = %format_go_duration(started.elapsed()),
            "Successfully installed all languages in parallel"
        );
        Ok(())
    }

    /// 安装一个受支持语言；未知名称只记录 warning 并成功跳过。
    pub async fn install_language(
        &self,
        cancellation: &CancellationToken,
        language: &Language,
    ) -> Result<(), AnthropicError> {
        tracing::info!(language = %language.name, version = %language.version, "Installing language");
        let script = match language.name.as_str() {
            "python" => install_scripts::python(&language.version),
            "node" | "nodejs" => install_scripts::node(&language.version),
            "go" | "golang" => install_scripts::go(&language.version),
            _ => {
                tracing::warn!(language = %language.name, version = %language.version, "Unsupported language, skipping installation");
                return Ok(());
            }
        };

        let output = Arc::new(Mutex::new(Vec::<u8>::new()));
        let captured = Arc::clone(&output);
        let name = language.name.clone();
        let version = language.version.clone();
        let streamer = output_streamer(move |stream, data| {
            tracing::debug!(
                language = %name,
                version = %version,
                stream = %stream,
                data = %String::from_utf8_lossy(data),
                "Installation script output"
            );
            append_tail(&captured, data);
            Ok::<(), io::Error>(())
        });
        let pattern = format!("install-{}-{}-*.sh", language.name, language.version);
        let result = match execute_script(cancellation, &script, &pattern, streamer).await {
            Ok(result) => result,
            Err(error) => {
                // 在给错误追加上下文前记录当前 4 KiB 尾部，不能用 trim 改变
                // `output` 字段。
                let captured = tail_to_string(&output);
                tracing::error!(
                    language = %language.name,
                    version = %language.version,
                    error = %error,
                    output = %captured,
                    "Failed to execute installation script"
                );
                return Err(AnthropicError::with_source(
                    "failed to execute installation script",
                    error,
                ));
            }
        };

        let success = result.error.is_none();
        tracing::info!(
            language = %language.name,
            version = %language.version,
            exit_code = result.exit_code,
            duration = %format_go_duration(result.duration),
            success,
            "Installation script completed"
        );

        if let Some(error) = result.error {
            let output = tail_to_string(&output);
            let captured = if output.trim().is_empty() {
                error.to_string()
            } else {
                output.trim().to_owned()
            };
            tracing::error!(
                language = %language.name,
                version = %language.version,
                exit_code = result.exit_code,
                duration = %format_go_duration(result.duration),
                error = %error,
                output = %captured,
                "Installation script failed"
            );
            return Err(AnthropicError::message(format!(
                "failed to install {} {}: {} (exit code: {})",
                language.name, language.version, captured, result.exit_code
            )));
        }

        for line in tail_to_string(&output).lines() {
            let line = line.trim();
            if should_log_install_output_line(line, language) {
                tracing::info!(language = %language.name, version = %language.version, line = %line, "Installation output");
            }
        }
        // 这是 installLanguage 内部的成功点，与并发调度层的同文案日志是
        // 两个 callsite，不能合并到通用 wrapper caller。
        tracing::info!(
            language = %language.name,
            version = %language.version,
            duration = %format_go_duration(result.duration),
            "Successfully installed language"
        );
        Ok(())
    }

    /// 在用户 HOME 和 `/home/claude` 依次引导 Claude hook。
    pub(crate) async fn bootstrap_hooks_in_all_dirs(
        &self,
        cancellation: &CancellationToken,
        config: HookConfig,
    ) -> Result<(), AnthropicError> {
        let home = user_home_dir()?;
        self.bootstrap_hooks_under_dir(cancellation, &home, &config)
            .await
            .map_err(|error| {
                AnthropicError::with_source(
                    format!("failed to bootstrap hooks in {}", home.display()),
                    error,
                )
            })?;
        self.bootstrap_hooks_under_dir(cancellation, Path::new("/home/claude"), &config)
            .await
            .map_err(|error| {
                AnthropicError::with_source("failed to bootstrap hooks in /home/claude", error)
            })
    }

    /// 在单个目录下创建 `.claude/settings.json` 与 stop hook。
    ///
    /// 已存在文件保持原字节和 mode；任意 `metadata` 错误均进入 写入分支，
    /// 新文件分别使用 0600 和 0755。
    pub(crate) async fn bootstrap_hooks_under_dir(
        &self,
        _cancellation: &CancellationToken,
        directory: &Path,
        config: &HookConfig,
    ) -> Result<(), AnthropicError> {
        let claude_dir = directory.join(".claude");
        let settings_path = claude_dir.join("settings.json");
        let stop_hook_path = claude_dir.join(&config.hook_name);
        create_dir_all_with_mode(&claude_dir, 0o755).map_err(|error| {
            AnthropicError::with_source("failed to create .claude directory", error)
        })?;

        if fs::metadata(&settings_path).is_ok() {
            tracing::info!(path = %settings_path.display(), "Claude settings file already exists");
        } else {
            write_file_with_mode(&settings_path, &config.settings_data, 0o600).map_err(
                |error| AnthropicError::with_source("failed to write settings file", error),
            )?;
            tracing::info!(path = %settings_path.display(), "Successfully created settings file");
        }

        if fs::metadata(&stop_hook_path).is_ok() {
            tracing::info!(path = %stop_hook_path.display(), "Stop hook script already exists");
        } else {
            write_file_with_mode(&stop_hook_path, &config.hook_data, 0o755).map_err(|error| {
                AnthropicError::with_source("failed to write stop hook script", error)
            })?;
            tracing::info!(path = %stop_hook_path.display(), "Successfully created stop hook script");
        }
        Ok(())
    }

    /// 从固定模板创建 Baku 项目、写凭据、初始化 Git 并 best-effort 启动服务。
    pub async fn initialize_baku_project(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<String, AnthropicError> {
        tracing::info!(
            source_dir = BAKU_TEMPLATE_DIR,
            project_dir = BAKU_PROJECT_DIR,
            "Initializing Baku project"
        );
        copy_dir(Path::new(BAKU_TEMPLATE_DIR), Path::new(BAKU_PROJECT_DIR))
            .map_err(|error| AnthropicError::with_source("failed to copy Baku template", error))?;
        if self
            .auth_context
            .as_ref()
            .is_some_and(AuthContext::has_supabase)
        {
            self.write_supabase_env_files(cancellation, Path::new(BAKU_PROJECT_DIR))
                .await
                .map_err(|error| {
                    AnthropicError::with_source("failed to write Supabase env files", error)
                })?;
        }
        self.init_git_repo(cancellation, Path::new(BAKU_PROJECT_DIR))
            .await
            .map_err(|error| AnthropicError::with_source("failed to initialize git repo", error))?;
        if let Err(error) = self
            .start_dev_server(cancellation, Path::new(BAKU_PROJECT_DIR))
            .await
        {
            tracing::warn!(error = %error, "Failed to start dev server (non-fatal)");
        }
        Ok(BAKU_PROJECT_DIR.to_owned())
    }

    /// 将 Supabase 凭据按恢复格式写入 `.env` 与 `.env.local`。
    ///
    /// 日志只包含 project reference；anon key、数据库密码和 PAT 均不会进入日志。
    pub async fn write_supabase_env_files(
        &self,
        _cancellation: &CancellationToken,
        project_dir: impl AsRef<Path>,
    ) -> Result<(), AnthropicError> {
        let Some(auth) = self
            .auth_context
            .as_ref()
            .filter(|auth| auth.has_supabase())
        else {
            return Ok(());
        };
        let project_dir = project_dir.as_ref();
        let env_content = format!(
            "VITE_SUPABASE_URL=https://{}.supabase.co\nVITE_SUPABASE_ANON_KEY={}\n",
            auth.get_supabase_project_ref(),
            auth.get_supabase_anon_key()
        );
        write_file_with_mode(&project_dir.join(".env"), env_content.as_bytes(), 0o600)
            .map_err(|error| AnthropicError::with_source("write .env", error))?;
        let local_content = format!(
            "DATABASE_URL=postgresql://postgres:{}@db.{}.supabase.co:5432/postgres\n",
            auth.get_supabase_db_pass(),
            auth.get_supabase_project_ref()
        );
        write_file_with_mode(
            &project_dir.join(".env.local"),
            local_content.as_bytes(),
            0o600,
        )
        .map_err(|error| AnthropicError::with_source("write .env.local", error))?;
        tracing::info!(project_ref = %auth.get_supabase_project_ref(), "supabase_env_files_written");
        Ok(())
    }

    /// 查找恢复 session 使用的固定 Baku 项目目录。
    pub fn find_existing_baku_project(&self) -> Result<String, AnthropicError> {
        if fs::metadata(BAKU_PROJECT_DIR).is_ok_and(|metadata| metadata.is_dir()) {
            return Ok(BAKU_PROJECT_DIR.to_owned());
        }
        Err(AnthropicError::message("Baku project directory not found"))
    }

    /// 按固定顺序运行七条 Git 命令并创建初始模板提交。
    pub async fn init_git_repo(
        &self,
        cancellation: &CancellationToken,
        directory: &Path,
    ) -> Result<(), AnthropicError> {
        const COMMANDS: &[&[&str]] = &[
            &["git", "init"],
            &["git", "config", "user.email", "claude@anthropic.com"],
            &["git", "config", "user.name", "Claude"],
            &["git", "config", "gc.auto", "0"],
            &["git", "config", "commit.gpgsign", "false"],
            &["git", "add", "."],
            &["git", "commit", "-m", "Initial project from Baku template"],
        ];
        for command in COMMANDS {
            let (output, result) =
                run_command_combined(cancellation, directory, command[0], &command[1..]).await;
            if let Err(error) = result {
                return Err(AnthropicError::formatted_with_source(
                    format!(
                        "git command [{}] failed: {}\nOutput: {}",
                        command.join(" "),
                        error,
                        String::from_utf8_lossy(&output)
                    ),
                    error,
                ));
            }
        }
        tracing::info!(project_dir = %directory.display(), "Git repository initialized");
        Ok(())
    }

    /// 若 9199 端口未监听，则脱离取消 token 启动 supervisord 并后台 wait。
    pub async fn start_dev_server(
        &self,
        _cancellation: &CancellationToken,
        directory: impl AsRef<Path>,
    ) -> Result<(), AnthropicError> {
        if self.is_supervisord_running().await {
            tracing::info!("supervisord already running, skipping start");
            return Ok(());
        }
        let mut command = Command::new("supervisord");
        command
            .args(["-c", "/etc/supervisord.conf"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|error| AnthropicError::with_source("failed to start supervisord", error))?;
        let pid = child.id().unwrap_or_default();
        let directory = directory.as_ref().to_path_buf();
        tracing::info!(project_dir = %directory.display(), pid, "supervisord started for dev server");
        tokio::spawn(async move {
            let error = match child.wait().await {
                Ok(status) if status.success() => None,
                Ok(status) => Some(status.to_string()),
                Err(error) => Some(error.to_string()),
            };
            if let Some(error) = error {
                tracing::warn!(project_dir = %directory.display(), error = %error, "supervisord exited with error");
            }
        });
        Ok(())
    }

    /// 在一秒窗口内探测本地 supervisord/Vite 端口。
    pub async fn is_supervisord_running(&self) -> bool {
        matches!(
            tokio::time::timeout(Duration::from_secs(1), TcpStream::connect("127.0.0.1:9199"))
                .await,
            Ok(Ok(_))
        )
    }

    /// 返回必定存在的配置借用；仅在 initialize 的 `None` 检查之后使用。
    fn config_ref(&self) -> &Config {
        self.config
            .as_ref()
            .expect("initialize checked anthropic config")
    }

    /// 返回必定存在的配置可变借用；仅在 initialize 的 `None` 检查之后使用。
    fn config_mut(&mut self) -> &mut Config {
        self.config
            .as_mut()
            .expect("initialize checked anthropic config")
    }
}

#[async_trait]
impl EnvironmentType for AnthropicEnvironmentType {
    /// 转发 session 模式更新。
    fn set_session_mode(&mut self, mode: SessionMode) {
        AnthropicEnvironmentType::set_session_mode(self, mode);
    }

    /// 转发启动上下文更新。
    fn set_startup_context(&mut self, context: Option<StartupContext>) {
        AnthropicEnvironmentType::set_startup_context(self, context);
    }

    /// 转发认证上下文更新。
    fn set_auth_context(&mut self, context: Option<AuthContext>) {
        AnthropicEnvironmentType::set_auth_context(self, context);
    }

    /// 转发异步初始化并转换为公共动态错误。
    async fn initialize(
        &mut self,
        cancellation: &CancellationToken,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), BoxError> {
        AnthropicEnvironmentType::initialize(self, cancellation, activity_recorder)
            .await
            .map_err(boxed)
    }

    /// 转发工作目录 getter。
    fn get_cwd(&self) -> &str {
        AnthropicEnvironmentType::get_cwd(self)
    }

    /// 转发 Claude 环境变量副本 getter。
    fn get_claude_environment_variables(&self) -> BTreeMap<String, String> {
        AnthropicEnvironmentType::get_claude_environment_variables(self)
    }
}

/// 只记录含安装状态关键词的输出行。
fn should_log_install_output_line(line: &str, language: &Language) -> bool {
    if line.is_empty() {
        return false;
    }
    let line = line.to_lowercase();
    let name = language.name.to_lowercase();
    [
        "warning",
        "error",
        name.as_str(),
        "installed",
        "created",
        "using",
        "found",
        "version",
    ]
    .iter()
    .any(|needle| line.contains(needle))
}

/// 在共享缓冲中追加新块并仅保留最后 4096 字节。
fn append_tail(output: &Mutex<Vec<u8>>, data: &[u8]) {
    let mut output = output
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    output.extend_from_slice(data);
    if output.len() > OUTPUT_TAIL_BYTES {
        let remove = output.len() - OUTPUT_TAIL_BYTES;
        output.drain(..remove);
    }
}

/// 把共享尾部缓冲转换为可发送的 UTF-8 文本。
fn tail_to_string(output: &Mutex<Vec<u8>>) -> String {
    let output = output
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    String::from_utf8_lossy(&output).into_owned()
}

/// 返回 Linux 上的非空 `$HOME`。
fn user_home_dir() -> Result<PathBuf, AnthropicError> {
    env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            AnthropicError::message("failed to get home directory: $HOME is not defined")
        })
}

/// 用指定 Unix mode 递归创建目录；已有目录的 mode 不会被修改。
fn create_dir_all_with_mode(path: impl AsRef<Path>, mode: u32) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(true).mode(mode);
    builder.create(path)
}

/// 以 create/truncate 语义和指定创建 mode 写文件。
fn write_file_with_mode(path: &Path, data: &[u8], mode: u32) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true).mode(mode);
    let mut file = options.open(path)?;
    file.write_all(data)
}

/// 运行带参数命令、并发读取两个 pipe，并在取消时只杀直接 child。
async fn run_command_combined(
    cancellation: &CancellationToken,
    directory: &Path,
    program: &str,
    arguments: &[&str],
) -> (Vec<u8>, Result<(), AnthropicError>) {
    let mut command = Command::new(program);
    command.args(arguments).current_dir(directory);
    match run_command(command, cancellation, None, None, CaptureMode::Combined).await {
        Ok(output) => {
            let status = output.status;
            let output = output.output.expect_combined();
            if status.success() {
                (output, Ok(()))
            } else {
                (output, Err(AnthropicError::message(status.to_string())))
            }
        }
        Err(error) => {
            let message = error.to_string();
            let output = error.output.expect_combined();
            (output, Err(AnthropicError::message(message)))
        }
    }
}

/// 递归复制目录、文件 mode 与 symlink 目标，不跟随 symlink。
pub(crate) fn copy_dir(source: &Path, destination: &Path) -> Result<(), AnthropicError> {
    copy_dir_inner(source, destination).map_err(|error| {
        AnthropicError::with_source(
            format!(
                "failed to copy directory {} to {}",
                source.display(),
                destination.display()
            ),
            error,
        )
    })
}

/// 执行 `copy_dir` 的逐项复制，并生成既定的细粒度错误上下文。
fn copy_dir_inner(source: &Path, destination: &Path) -> Result<(), AnthropicError> {
    for entry in WalkDir::new(source).sort_by_file_name().follow_links(false) {
        let entry = entry.map_err(|error| {
            let path = error.path().unwrap_or(source);
            AnthropicError::formatted_with_source(
                format!("walk error at {}: {error}", path.display()),
                error,
            )
        })?;
        let path = entry.path();
        let relative = path.strip_prefix(source).map_err(|error| {
            AnthropicError::formatted_with_source(
                format!(
                    "failed to get relative path for {}: {error}",
                    path.display()
                ),
                error,
            )
        })?;
        let target = destination.join(relative);
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            AnthropicError::formatted_with_source(
                format!("failed to stat file {}: {error}", path.display()),
                error,
            )
        })?;

        if metadata.file_type().is_symlink() {
            let link_target = fs::read_link(path).map_err(|error| {
                AnthropicError::formatted_with_source(
                    format!("failed to read symlink {}: {error}", path.display()),
                    error,
                )
            })?;
            symlink(&link_target, &target).map_err(|error| {
                AnthropicError::formatted_with_source(
                    format!(
                        "failed to create symlink {} -> {}: {error}",
                        target.display(),
                        link_target.display()
                    ),
                    error,
                )
            })?;
            continue;
        }
        if metadata.is_dir() {
            create_dir_all_with_mode(&target, metadata.permissions().mode()).map_err(|error| {
                AnthropicError::formatted_with_source(
                    format!("failed to create directory {}: {error}", target.display()),
                    error,
                )
            })?;
            continue;
        }

        let mut source_file = File::open(path).map_err(|error| {
            AnthropicError::formatted_with_source(
                format!("failed to open source file {}: {error}", path.display()),
                error,
            )
        })?;
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create(true)
            .truncate(true)
            .mode(metadata.permissions().mode());
        let mut target_file = options.open(&target).map_err(|error| {
            AnthropicError::formatted_with_source(
                format!("error creating file {}: {error}", target.display()),
                error,
            )
        })?;
        io::copy(&mut source_file, &mut target_file).map_err(|error| {
            AnthropicError::formatted_with_source(
                format!(
                    "failed to copy {} to {}: {error}",
                    path.display(),
                    target.display()
                ),
                error,
            )
        })?;
        target_file.flush().map_err(|error| {
            AnthropicError::formatted_with_source(
                format!("failed to close file {}: {error}", target.display()),
                error,
            )
        })?;
    }
    Ok(())
}
