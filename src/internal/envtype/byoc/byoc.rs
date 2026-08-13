//! BYOC 环境的配置校验、初始化和外部适配逻辑。
//!
//! 本模块依次完成工作目录、
//! Claude 设置、Git 全局配置、setup 脚本、缓存分支和 Git proxy 初始化。
//! Rust 额外保留可注入的 processor/factory trait 以隔离测试；生产构造始终安装
//! 真实适配器，不改变 日志、网络和子进程副作用边界。

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use crate::internal::api::{HttpClient, LogCategory, LogLevel as ApiLogLevel};
use crate::internal::auth::AuthContext;
use crate::internal::config::{SessionMode, StartupContext};
use crate::internal::envtype::{
    EnvironmentType, LeaseManagerHandle, LeaseManagerProvider, Registration,
};
use crate::internal::podmonitor::{self, get_default_health_file_path};
use crate::internal::process::{
    CaptureMode, CommandFailure, CommandOutput, execute_script, run_command,
};
use crate::internal::session::ActivityRecorder;
use crate::internal::util::{BoxError, output_streamer};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use super::{
    ByocConfig, ByocEnvironmentType, ByocError, ContainProvideAuthTransport, LeaseManagerFactory,
    LeaseManagerRequest, SourceProcessMode, SourceProcessRequest, SourceProcessor,
    SourceProcessorError, source_processor::ProductionSourceProcessor,
};

/// setup 脚本的最大 UTF-8 字节数，与 1 MiB 上限一致。
pub const MAX_TASK_SETUP_SCRIPT_BYTES: usize = 1024 * 1024;

/// BYOC 在环境类型注册表中的静态元数据。
pub static REGISTRATION: LazyLock<Registration> = LazyLock::new(|| Registration {
    type_name: "byoc".to_owned(),
    factory: Some(Arc::new(|data| {
        new(data)
            .map(|environment| Box::new(environment) as Box<dyn EnvironmentType>)
            .map_err(|error| Box::new(error) as BoxError)
    })),
    description: "BYOC (Bring Your Own Cloud) environment with customer-managed infrastructure"
        .to_owned(),
});

/// 把 BYOC 创建参数接到真实 podmonitor 状态机的默认工厂。
///
/// 在 `CreateLeaseManager` 中直接调用 `podmonitor.NewLeaseManager`。Rust 保留
/// 可注入 factory 仅用于隔离测试，但生产默认值始终使用此适配器，避免调用方额外接线。
#[derive(Debug, Default)]
struct PodmonitorLeaseManagerFactory;

impl LeaseManagerFactory for PodmonitorLeaseManagerFactory {
    /// 构造尾段。
    ///
    /// 复用带认证 default headers 的 Reqwest client 创建真实租约管理器。
    ///
    /// `ContainProvideAuthTransport` 已固定 30 秒请求超时，并写入 Bearer、organization、
    /// runner 版本和 beta header；转换过程中不得重建连接池，否则这些 wire 行为会丢失。
    ///
    /// # 参数
    /// - `request`：包含已认证 transport、租约标识、取消令牌和时间参数的完整请求。
    ///
    /// # 返回
    /// 成功时返回可异步启动/停止的 podmonitor 句柄；当前生产适配过程本身不失败，
    /// 仍使用公共 `Result` 以允许测试 factory 注入构造错误。
    ///
    /// # 副作用
    /// 本函数只组装共享连接池和状态机，不立即发起 heartbeat；网络请求在 `start`
    /// 后发生。
    fn create(
        &self,
        request: LeaseManagerRequest,
    ) -> Result<Arc<dyn LeaseManagerHandle>, BoxError> {
        let api_client = HttpClient::from_reqwest_client(
            &request.api_base_url,
            request.transport.client(),
            None,
        );
        Ok(Arc::new(podmonitor::new_lease_manager(
            request.cancellation,
            request.session_id,
            request.environment_id,
            request.organization_uuid,
            api_client,
            request.heartbeat_interval,
            request.safety_window,
            get_default_health_file_path(),
        )))
    }
}

#[async_trait]
impl LeaseManagerHandle for podmonitor::LeaseManager {
    /// 启动真实 podmonitor，并把具体错误擦除为环境类型公共边界的错误。
    async fn start(&self) -> Result<(), BoxError> {
        podmonitor::LeaseManager::start(self)
            .await
            .map_err(|error| Box::new(error) as BoxError)
    }

    /// 保留等待语义，包括初始 heartbeat 未成功时持续等待。
    async fn stop(&self) {
        podmonitor::LeaseManager::stop(self).await;
    }

    /// 暴露 podmonitor 构造时从 manager 父 token 派生的真实子 token。
    fn cancellation_token(&self) -> CancellationToken {
        podmonitor::LeaseManager::cancellation_token(self)
    }
}

///
/// 从原始 JSON 创建 BYOC 环境，并在产生任何副作用前完成配置校验。
///
/// CWD 除了必须是绝对路径，还必须与词法归一化结果逐字节相同。
/// 因而 `..`、`.`、重复分隔符和非根目录的尾随分隔符都会被拒绝，
/// 不依赖目标是否已存在，也不解析 symlink。
///
/// # 参数
/// - `data`：后端返回的 BYOC JSON，函数只在反序列化期间借用。
///
/// # 返回
/// 配置合法时返回已接好生产 adapter 的环境；JSON、类型、CWD 或脚本大小不合法时
/// 返回与 文本一致的 `ByocError`。本函数不访问文件、网络或子进程。
pub fn new(data: &[u8]) -> Result<ByocEnvironmentType, ByocError> {
    let config: ByocConfig = serde_json::from_slice(data)?;
    if config.environment_type != "byoc" {
        return Err(ByocError::InvalidEnvironmentType {
            actual: config.environment_type,
        });
    }
    validate_cwd(&config.cwd)?;
    let script_size = config.task_setup_script.len();
    if script_size > MAX_TASK_SETUP_SCRIPT_BYTES {
        return Err(ByocError::SetupScriptTooLarge {
            actual: script_size,
            maximum: MAX_TASK_SETUP_SCRIPT_BYTES,
        });
    }

    Ok(ByocEnvironmentType {
        config,
        session_mode: SessionMode::New,
        startup_context: None,
        auth_context: None,
        // 构造后的 BYOC 会直接调用 internal/sources；Rust 默认接入同一生产实现，
        // 同时保留 setter，供测试替换 Git/网络副作用。
        source_processor: Arc::new(ProductionSourceProcessor),
        // 生产构造后立即具备真实 podmonitor 能力；测试仍可通过 setter
        // 覆盖为捕获工厂，但不会出现生产路径忘记注入而 fail closed 的新行为。
        lease_manager_factory: Some(Arc::new(PodmonitorLeaseManagerFactory)),
    })
}

/// 按 Unix 词法路径归一化 语义构造词法归一化路径并与原字节比较。
fn validate_cwd(cwd: &str) -> Result<(), ByocError> {
    if cwd.is_empty() {
        return Ok(());
    }
    let path = Path::new(cwd);
    if !path.is_absolute() {
        return Err(ByocError::CwdNotAbsolute {
            cwd: cwd.to_owned(),
        });
    }

    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => clean.push(prefix.as_os_str()),
            Component::RootDir => clean.push(Path::new("/")),
            Component::CurDir => {}
            Component::ParentDir => {
                clean.pop();
            }
            Component::Normal(component) => clean.push(component),
        }
    }
    if clean.as_os_str() != OsStr::new(cwd) {
        return Err(ByocError::CwdNotClean {
            cwd: cwd.to_owned(),
        });
    }
    Ok(())
}

impl ByocEnvironmentType {
    /// 更新 session 模式，用于决定 setup 脚本和缓存 branch 处理。
    pub fn set_session_mode(&mut self, mode: SessionMode) {
        self.session_mode = mode;
    }

    /// 更新已解析的启动上下文。
    pub fn set_startup_context(&mut self, context: Option<StartupContext>) {
        self.startup_context = context;
    }

    /// 更新 source 处理可用的认证上下文。
    pub fn set_auth_context(&mut self, context: Option<AuthContext>) {
        self.auth_context = context;
    }

    /// 注入 sources 子系统适配器。
    pub fn set_source_processor(&mut self, processor: Arc<dyn SourceProcessor>) {
        self.source_processor = processor;
    }

    /// 注入 podmonitor 子系统的 lease manager factory。
    pub fn set_lease_manager_factory(&mut self, factory: Arc<dyn LeaseManagerFactory>) {
        self.lease_manager_factory = Some(factory);
    }

    /// 返回配置的工作目录。
    #[must_use]
    pub fn get_cwd(&self) -> &str {
        &self.config.cwd
    }

    /// 完成 BYOC 初始化。
    ///
    /// 取消保证是“不再启动新副作用”，而不是事务回滚：已创建的目录/
    /// settings、已执行的 Git config 以及脚本在被终止前写入的数据都会保留。
    /// 进程 CWD 是全局状态；切换成功后不会在错误或取消时自动改回，
    /// 否则会与并发任务竞争。
    pub async fn initialize(
        &mut self,
        cancellation: &CancellationToken,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), ByocError> {
        ensure_not_cancelled(cancellation)?;
        tracing::info!(
            cwd = %self.config.cwd,
            environment_type = %self.config.environment_type,
            has_task_setup_script = !self.config.task_setup_script.is_empty(),
            session_mode = %self.session_mode.as_str(),
            "Initializing BYOC environment"
        );

        if !self.config.cwd.is_empty() {
            tracing::info!(cwd = %self.config.cwd, "Setting working directory");
            ensure_not_cancelled(cancellation)?;
            create_dir_all_with_mode(PathBuf::from(&self.config.cwd), 0o755)
                .await
                .map_err(|source| ByocError::CreateWorkingDirectory {
                    cwd: self.config.cwd.clone(),
                    source,
                })?;
            // mkdir 已经可见；在取消后不再触碰进程全局 CWD。
            ensure_not_cancelled(cancellation)?;
            env::set_current_dir(&self.config.cwd).map_err(|source| {
                ByocError::ChangeWorkingDirectory {
                    cwd: self.config.cwd.clone(),
                    source,
                }
            })?;
            tracing::info!(cwd = %self.config.cwd, "Working directory set successfully");
        }

        if let Err(error) = self.bootstrap_claude_settings(cancellation).await {
            tracing::warn!(error = %error, "Failed to bootstrap Claude settings");
        }
        ensure_not_cancelled(cancellation)?;
        self.setup_git_config(cancellation).await?;

        let should_run_setup =
            matches!(self.session_mode, SessionMode::New | SessionMode::SetupOnly);
        if !self.config.task_setup_script.is_empty() && should_run_setup {
            tracing::info!("Running task setup script");
            ensure_not_cancelled(cancellation)?;
            if let Err(error) = self
                .run_script(cancellation, "task_setup", &self.config.task_setup_script)
                .await
            {
                return Err(ByocError::TaskSetup(Box::new(error)));
            }
        } else if !self.config.task_setup_script.is_empty() {
            tracing::info!("Fast resume: Skipping task setup script");
        }

        let has_sources = self
            .startup_context
            .as_ref()
            .is_some_and(|context| !context.sources.is_empty());
        let has_outcomes = self
            .startup_context
            .as_ref()
            .is_some_and(|context| !context.outcomes.is_empty());
        if self.session_mode == SessionMode::ResumeCached && has_sources && has_outcomes {
            tracing::info!("Attempting to checkout target branches for BYOC environment");
            if let Err(error) = self
                .handle_branch_checkout(cancellation, activity_recorder)
                .await
            {
                tracing::error!(error = %error, "Failed to checkout branches");
                return Err(ByocError::BranchCheckout(Box::new(error)));
            }
        }

        if self.session_mode == SessionMode::ResumeCached && has_sources {
            match self
                .setup_git_proxy_after_sources_processed(cancellation, activity_recorder)
                .await
            {
                Ok(()) => tracing::info!(
                    source_count = self
                        .startup_context
                        .as_ref()
                        .map_or(0, |context| context.sources.len()),
                    "Git proxy setup completed"
                ),
                Err(SourceProcessorError::CreateManager(error)) => {
                    tracing::warn!(error = %error, "Failed to create source manager for git proxy setup");
                }
                Err(
                    SourceProcessorError::ProcessSources(error)
                    | SourceProcessorError::SetupGitProxy(error),
                ) => {
                    tracing::warn!(error = %error, "Failed to setup git proxy");
                }
            }
        }
        // 把 proxy 阶段（包括其中观察到的取消）视为 best effort，之后直接成功返回。
        tracing::info!("BYOC environment initialization completed");
        Ok(())
    }

    /// 从 Git outcomes 提取仓库到单一 branch 的映射。
    ///
    /// 非 Git outcome 和空 branches 被忽略；同一 repo 重复出现时保留最后一条。
    pub fn extract_repo_branch_mapping(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<BTreeMap<String, String>, ByocError> {
        let mut mapping = BTreeMap::new();
        let Some(context) = &self.startup_context else {
            return Ok(mapping);
        };
        for outcome in &context.outcomes {
            ensure_not_cancelled(cancellation)?;
            if outcome.outcome_type != "git_repository" || outcome.git_info.branches.is_empty() {
                continue;
            }
            if outcome.git_info.branches.len() != 1 {
                return Err(ByocError::MultipleBranches {
                    repo: outcome.git_info.repo.clone(),
                    actual: outcome.git_info.branches.len(),
                });
            }
            let branch = outcome.git_info.branches[0].clone();
            mapping.insert(outcome.git_info.repo.clone(), branch.clone());
            tracing::debug!(repo = %outcome.git_info.repo, branch = %branch, "Mapped repository to branch for checkout");
        }
        Ok(mapping)
    }

    /// 让已注入 sources adapter 按 outcome 切换 branch。
    ///
    /// 未接线 adapter 时 fail closed，避免表面初始化成功但仍在错误 branch 上。
    /// 取消会 drop adapter future；adapter 同时收到 token，必须自行停止已启动的
    /// clone/checkout 进程，已产生的 Git 工作树变化不会回滚。
    pub async fn handle_branch_checkout(
        &self,
        cancellation: &CancellationToken,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), ByocError> {
        tracing::info!("Extracting repository-branch mapping from outcomes");
        let repo_branches = self.extract_repo_branch_mapping(cancellation)?;
        if repo_branches.is_empty() {
            tracing::info!("No specific branches requested for checkout");
            return Ok(());
        }
        tracing::info!(
            branch_count = repo_branches.len(),
            "Found branches to checkout"
        );
        let processor = &self.source_processor;
        let context = self.startup_context.as_ref();
        let request = SourceProcessRequest {
            cwd: self.config.cwd.clone(),
            auth_context: self.auth_context.clone(),
            api_base_url: context
                .map(|value| value.api_base_url.clone())
                .unwrap_or_default(),
            repo_branches,
            sources: context
                .map(|value| value.sources.clone())
                .unwrap_or_default(),
            mode: SourceProcessMode::AllowPrefetched,
            should_validate: true,
        };
        let count = request.repo_branches.len();
        let repository_label = if count == 1 {
            "repository"
        } else {
            "repositories"
        };
        ensure_not_cancelled(cancellation)?;
        if let Some(recorder) = activity_recorder {
            recorder.record_activity(
                ApiLogLevel::Info,
                LogCategory::Init,
                format!("Attempting to checkout branches for {count} {repository_label}"),
                BTreeMap::new(),
            );
        }

        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ByocError::Cancelled),
            result = processor.process_sources(cancellation, &request, activity_recorder) => {
                match result {
                    Ok(()) => {}
                    Err(SourceProcessorError::CreateManager(source)) => {
                        return Err(ByocError::SourceManager(source));
                    }
                    Err(
                        SourceProcessorError::ProcessSources(source)
                        | SourceProcessorError::SetupGitProxy(source),
                    ) => {
                        return Err(ByocError::SourceProcessing(source));
                    }
                }
            }
        }
        ensure_not_cancelled(cancellation)?;
        tracing::info!(branch_count = count, "Branches checked out successfully");
        if let Some(recorder) = activity_recorder {
            recorder.record_activity(
                ApiLogLevel::Info,
                LogCategory::Init,
                format!("Successfully checked out branches for {count} {repository_label}"),
                BTreeMap::new(),
            );
        }
        Ok(())
    }

    /// 构造 Claude Code 环境变量。
    ///
    /// 优先级从低到高为：BYOC 默认值、ambient
    /// `CLAUDE_CODE_EXIT_AFTER_STOP_DELAY`、startup entrypoint、最后是
    /// `config.environment`。StartupContext.environment_variables 属于上层子进程
    /// 合并边界，BYOC 在此不合并，Rust 也保持该行为。
    #[must_use]
    pub fn get_claude_environment_variables(&self) -> BTreeMap<String, String> {
        let entrypoint = self
            .startup_context
            .as_ref()
            .map(|context| context.entrypoint.as_str())
            .filter(|entrypoint| !entrypoint.is_empty())
            .unwrap_or("remote");
        let stop_delay = env::var("CLAUDE_CODE_EXIT_AFTER_STOP_DELAY")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "300000".to_owned());
        let mut variables = BTreeMap::from([
            ("CLAUDE_CODE_REMOTE".to_owned(), "true".to_owned()),
            ("CLAUDE_CODE_DEBUG".to_owned(), "true".to_owned()),
            ("CLAUDE_CODE_ENVIRONMENT_KIND".to_owned(), "byoc".to_owned()),
            ("CLAUDE_CODE_ENTRYPOINT".to_owned(), entrypoint.to_owned()),
            ("CLAUDE_CODE_EXIT_AFTER_STOP_DELAY".to_owned(), stop_delay),
        ]);
        variables.extend(self.config.environment.clone());
        variables
    }

    /// 执行 setup 脚本；缺少 shebang 时补 bash 和 `set -e`。
    ///
    /// 取消时 process 模块只终止 direct child，不扩展到脚本创建的进程组；脚本在
    /// 此前已写入的文件、仍运行的孙进程或外部状态无法通用回滚。
    pub async fn run_script(
        &self,
        cancellation: &CancellationToken,
        name: &str,
        script: &str,
    ) -> Result<(), ByocError> {
        ensure_not_cancelled(cancellation)?;
        tracing::info!(name = %name, script_length = script.len(), "Executing script");
        let content = if script.starts_with("#!") {
            script.to_owned()
        } else {
            format!("#!/bin/bash\nset -e\n{script}")
        };
        let script_name = name.to_owned();
        let streamer = output_streamer(move |stream, data| {
            tracing::debug!(
                script_name = %script_name,
                stream = %stream,
                data = %String::from_utf8_lossy(data),
                "Script output"
            );
            Ok::<(), std::io::Error>(())
        });
        let result = execute_script(cancellation, &content, &format!("{name}-*.sh"), streamer)
            .await
            .map_err(|source| ByocError::ExecuteScript {
                name: name.to_owned(),
                source,
            })?;
        if cancellation.is_cancelled() {
            return Err(ByocError::Cancelled);
        }
        if let Some(source) = result.error {
            return Err(ByocError::ScriptFailed {
                name: name.to_owned(),
                source,
            });
        }
        tracing::info!(
            name = %name,
            exit_code = result.exit_code,
            duration_ms = result.duration.as_millis(),
            "Script completed successfully"
        );
        Ok(())
    }

    /// 在文件缺失时安全写入 Claude settings 和 stop hook。
    ///
    /// `statPath` 预检查已合并进 `create_new` 原子创建路径；已存在的文件永不
    /// 覆盖，也不会在检查与写入之间留下 TOCTOU 窗口。新文件使用 create-new 消除
    /// stat/write 竞争；
    /// stop hook 在完整写入并 sync 之后才设为 0755，避免执行半成品。
    pub async fn bootstrap_claude_settings(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ByocError> {
        let home = env::var_os("HOME").ok_or(ByocError::HomeDirectoryMissing)?;
        let claude_dir = PathBuf::from(home).join(".claude");
        ensure_not_cancelled(cancellation)?;
        create_dir_all_with_mode(claude_dir.clone(), 0o755)
            .await
            .map_err(|source| ByocError::FileSystem {
                operation: "failed to create .claude directory",
                source,
            })?;

        let settings_path = claude_dir.join("settings.json");
        let stop_hook_path = claude_dir.join("stop-hook-git-check.sh");
        // 先 stat 两个路径并记录所有“已存在”事件，然后才开始任何写入；混合
        // 存在/缺失场景的日志顺序可被外部观察，因此不能简化成逐个检查后写入。
        let settings_exists = tokio::fs::metadata(&settings_path).await.is_ok();
        if settings_exists {
            tracing::info!(path = %settings_path.display(), "Claude settings file already exists");
        }
        let stop_hook_exists = tokio::fs::metadata(&stop_hook_path).await.is_ok();
        if stop_hook_exists {
            tracing::info!(path = %stop_hook_path.display(), "Stop hook script already exists");
        }

        if !settings_exists
            && write_if_missing(
                cancellation,
                &settings_path,
                crate::internal::envtype::shared::DEFAULT_SETTINGS_JSON,
                0o600,
                "failed to write settings file",
            )
            .await?
        {
            tracing::info!(path = %settings_path.display(), "Successfully created Claude settings file");
        }
        if !stop_hook_exists
            && write_if_missing(
                cancellation,
                &stop_hook_path,
                crate::internal::envtype::shared::STOP_HOOK_SCRIPT,
                0o755,
                "failed to write stop hook script",
            )
            .await?
        {
            tracing::info!(path = %stop_hook_path.display(), "Successfully created stop hook script");
        }
        ensure_not_cancelled(cancellation)
    }

    /// 顺序写入 Git 全局配置和签名公钥占位文件。
    ///
    /// `SKIP_GIT_CONFIG=true` 在首个副作用前直接返回。其他 Git 命令失败只警告并继续；取消则终止当前 child 并阻止后续命令/文件。
    /// 已成功的 `git config` 是持久化副作用，不在中途失败或取消时回滚。
    pub async fn setup_git_config(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ByocError> {
        if env::var("SKIP_GIT_CONFIG").as_deref() == Ok("true") {
            tracing::info!("Skipping git configuration (SKIP_GIT_CONFIG=true)");
            return Ok(());
        }
        tracing::info!("Setting up git configuration for BYOC environment");
        let configs = [
            ("user.name", "Claude"),
            ("user.email", "noreply@anthropic.com"),
            ("gpg.format", "ssh"),
            ("gpg.ssh.program", "/tmp/code-sign"),
            ("commit.gpgsign", "true"),
            ("http.proxyAuthMethod", "basic"),
        ];
        for (key, value) in configs {
            ensure_not_cancelled(cancellation)?;
            let failure = match run_git_config(cancellation, key, value).await {
                Ok(output) if output.status.success() => {
                    tracing::debug!(key = %key, value = %value, "Set git config");
                    None
                }
                Ok(output) => Some((
                    output.status.to_string(),
                    String::from_utf8_lossy(&output.output.expect_combined()).into_owned(),
                )),
                Err(error) => {
                    let message = error.to_string();
                    Some((
                        message,
                        String::from_utf8_lossy(&error.output.expect_combined()).into_owned(),
                    ))
                }
            };
            if let Some((error, output)) = failure {
                tracing::warn!(key, value, error, output, "Failed to set git config")
            }
        }

        ensure_not_cancelled(cancellation)?;
        let home = env::var_os("HOME").unwrap_or_default();
        let ssh_dir = PathBuf::from(home).join(".ssh");
        if let Err(error) = create_dir_all_with_mode(ssh_dir.clone(), 0o700).await {
            tracing::warn!(error = %error, "Failed to create .ssh directory");
            return Ok(());
        }

        ensure_not_cancelled(cancellation)?;
        let signing_key = ssh_dir.join("commit_signing_key.pub");
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&signing_key)
        {
            Ok(file) => {
                drop(file);
                tracing::debug!(path = %signing_key.display(), "Created empty signing key file");
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                tracing::warn!(error = %error, "Failed to create signing key file")
            }
        }
        ensure_not_cancelled(cancellation)?;
        let key_value = signing_key.to_string_lossy();
        let failure = match run_git_config(cancellation, "user.signingkey", &key_value).await {
            Ok(output) if output.status.success() => {
                tracing::debug!(path = %signing_key.display(), "Set git signing key");
                None
            }
            Ok(output) => Some((
                output.status.to_string(),
                String::from_utf8_lossy(&output.output.expect_combined()).into_owned(),
            )),
            Err(error) => {
                let message = error.to_string();
                Some((
                    message,
                    String::from_utf8_lossy(&error.output.expect_combined()).into_owned(),
                ))
            }
        };
        if let Some((error, output)) = failure {
            tracing::warn!(error, output, "Failed to set git signing key config")
        }
        ensure_not_cancelled(cancellation)?;
        tracing::info!("Git configuration setup completed");
        Ok(())
    }

    /// 在 sources 处理完成后执行 中的 best-effort Git proxy 阶段。
    async fn setup_git_proxy_after_sources_processed(
        &self,
        cancellation: &CancellationToken,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), SourceProcessorError> {
        let processor = &self.source_processor;
        let context = self.startup_context.as_ref();
        let request = SourceProcessRequest {
            cwd: self.config.cwd.clone(),
            auth_context: self.auth_context.clone(),
            api_base_url: context
                .map(|value| value.api_base_url.clone())
                .unwrap_or_default(),
            repo_branches: BTreeMap::new(),
            sources: context
                .map(|value| value.sources.clone())
                .unwrap_or_default(),
            mode: SourceProcessMode::Fresh,
            should_validate: false,
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(SourceProcessorError::SetupGitProxy(
                Box::new(ByocError::Cancelled) as BoxError,
            )),
            result = processor.setup_git_proxy_after_sources_processed(
                cancellation,
                &request,
                activity_recorder,
            ) => result,
        }
    }

    /// 通过已注入 podmonitor factory 构造 lease manager。
    ///
    /// 本模块只负责传递 30 秒间隔和已认证 transport，heartbeat/健康文件
    /// 状态机由 factory 所属的 podmonitor 实现。未注入时明确 fail closed。
    pub fn create_lease_manager(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
        environment_id: &str,
        api_base_url: &str,
        session_ingress_token: &str,
        organization_uuid: &str,
    ) -> Result<Arc<dyn LeaseManagerHandle>, ByocError> {
        ensure_not_cancelled(cancellation)?;
        let factory = self
            .lease_manager_factory
            .as_ref()
            .ok_or(ByocError::LeaseManagerFactoryUnavailable)?;
        let transport = ContainProvideAuthTransport::new(
            session_ingress_token.to_owned(),
            organization_uuid.to_owned(),
        )?;
        tracing::info!(session_id = %session_id, environment_id = %environment_id, "Creating LeaseManager for BYOC environment");
        factory
            .create(LeaseManagerRequest {
                cancellation: cancellation.clone(),
                session_id: session_id.to_owned(),
                environment_id: environment_id.to_owned(),
                api_base_url: api_base_url.to_owned(),
                organization_uuid: organization_uuid.to_owned(),
                transport,
                heartbeat_interval: Duration::from_secs(30),
                safety_window: Duration::from_secs(30),
            })
            .map_err(ByocError::LeaseManagerFactory)
    }
}

#[async_trait]
impl EnvironmentType for ByocEnvironmentType {
    /// 实现统一环境接口的 session mode setter。
    fn set_session_mode(&mut self, mode: SessionMode) {
        ByocEnvironmentType::set_session_mode(self, mode);
    }

    /// 实现统一环境接口的 startup context setter。
    fn set_startup_context(&mut self, context: Option<StartupContext>) {
        ByocEnvironmentType::set_startup_context(self, context);
    }

    /// 实现统一环境接口的 auth context setter。
    fn set_auth_context(&mut self, context: Option<AuthContext>) {
        ByocEnvironmentType::set_auth_context(self, context);
    }

    /// 在动态分发边界执行 BYOC 初始化。
    async fn initialize(
        &mut self,
        cancellation: &CancellationToken,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), BoxError> {
        ByocEnvironmentType::initialize(self, cancellation, activity_recorder)
            .await
            .map_err(|error| Box::new(error) as BoxError)
    }

    /// 在动态分发边界返回 CWD。
    fn get_cwd(&self) -> &str {
        ByocEnvironmentType::get_cwd(self)
    }

    /// 在动态分发边界返回 Claude Code 环境变量。
    fn get_claude_environment_variables(&self) -> BTreeMap<String, String> {
        ByocEnvironmentType::get_claude_environment_variables(self)
    }

    /// 对动态 `EnvironmentType` 暴露 BYOC 的 lease provider 能力。
    fn lease_manager_provider(&self) -> Option<&dyn LeaseManagerProvider> {
        Some(self)
    }
}

impl LeaseManagerProvider for ByocEnvironmentType {
    /// 将通用 provider 调用转发给 BYOC factory adapter。
    fn create_lease_manager(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
        environment_id: &str,
        api_base_url: &str,
        session_ingress_token: &str,
        organization_uuid: &str,
    ) -> Result<Arc<dyn LeaseManagerHandle>, BoxError> {
        ByocEnvironmentType::create_lease_manager(
            self,
            cancellation,
            session_id,
            environment_id,
            api_base_url,
            session_ingress_token,
            organization_uuid,
        )
        .map_err(|error| Box::new(error) as BoxError)
    }
}

/// 取消检查的单一出口，用于在每个新副作用前 fail fast。
fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), ByocError> {
    if cancellation.is_cancelled() {
        Err(ByocError::Cancelled)
    } else {
        Ok(())
    }
}

/// 使用 create-new 写入文件，完整 sync 后再设置最终 mode。
///
/// # 返回
/// `true` 表示本次创建并完成写入；竞争方已先创建时返回 `false`；其他 I/O 或取消
/// 错误返回 `ByocError`。调用方据此只为真实创建动作写成功日志。
async fn write_if_missing(
    cancellation: &CancellationToken,
    path: &Path,
    content: &[u8],
    mode: u32,
    operation: &'static str,
) -> Result<bool, ByocError> {
    ensure_not_cancelled(cancellation)?;
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = match options.open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
        Err(source) => return Err(ByocError::FileSystem { operation, source }),
    };
    file.write_all(content)
        .await
        .map_err(|source| ByocError::FileSystem { operation, source })?;
    file.sync_all()
        .await
        .map_err(|source| ByocError::FileSystem { operation, source })?;
    #[cfg(unix)]
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .await
        .map_err(|source| ByocError::FileSystem { operation, source })?;
    Ok(true)
}

/// 执行单条 `git config --global`，并捕获 stdout/stderr 的真实交错顺序。
///
/// stdout 与 stderr 共享同一个 close-on-exec 管道写端，避免把两个独立缓冲区
/// 人工拼接后改变交错顺序。取消发生在进程运行期间时只终止并回收直接 child；已经
/// 写入 pipe 的内容仍排空后返回，供 warning 日志保留 可观察到的部分输出。
///
/// # 返回
/// 退出成功时返回 `Ok(())`；启动、取消或非零退出时返回错误和已捕获输出，供调用方
/// 记录 `error` 与 `output`。
async fn run_git_config(
    cancellation: &CancellationToken,
    key: &str,
    value: &str,
) -> Result<CommandOutput, CommandFailure> {
    let mut command = Command::new("git");
    command
        .args(["config", "--global", key, value])
        .current_dir("/");
    run_command(command, cancellation, None, None, CaptureMode::Combined).await
}

/// 在 blocking 池中按 递归建目录 语义创建目录。
///
/// mode 只影响新建目录并仍受 umask 限制；已存在的目录不会被 chmod。
async fn create_dir_all_with_mode(path: PathBuf, mode: u32) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(mode);
        }
        #[cfg(not(unix))]
        let _ = mode;
        builder.create(path)
    })
    .await
    .map_err(std::io::Error::other)?
}
