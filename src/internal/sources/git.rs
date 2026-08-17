//! Git source 的外部命令实现。
//!
//! 所有仓库操作启动系统 `git`，不会通过 `git2` 改写协议、凭据或 ref 行为。
//! 子进程统一走 Tokio 执行层；取消只终止并回收直接 child，不遍历 process group。

use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{self, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use super::{
    GitHandler, GitProxyConfig, GitProxyFactory, GitRemoteUpdater, ProcessMode, RepoAuth,
    SourceHandler, SourceValue, SourcesError, SourcesResult,
};
use crate::internal::{
    api::LogCategory,
    auth::{AuthContext, Permission, SourceAuthProvider, new_github_source_auth_provider},
    config::{GitInfo, GitRepositorySource},
    process::{CaptureMode, CommandFailure, CommandOutput, run_command},
    session::ActivityRecorder,
    util::{Stopper, update_remote_tracking_branch},
};

use super::gitproxy_adapter::ProductionGitProxyFactory;
use super::types::SharedAuthProvider;

const GIT_HTTP_PROXY_AUTH_CONFIG: &str = "http.proxyAuthMethod=basic";

/// 保存全部配置并读取 post-clone Hook 环境变量。
///
/// `POST_CLONE_HOOK_PATH` 只在构造时读取一次；之后修改进程环境不会改变已创建
/// handler，这与 recorder 对象生命周期一致。`'recorder` 原样来自调用方；返回的 handler
/// 不能比 recorder 活得更久，因此局部初始化 recorder 无需伪装成 `'static`。
#[allow(clippy::too_many_arguments)]
pub fn new_git_handler<'recorder>(
    base_dir: impl Into<PathBuf>,
    auth_context: Option<Arc<AuthContext>>,
    sdk_base_url: impl Into<String>,
    repo_branches: Option<HashMap<String, String>>,
    activity_recorder: Option<Arc<dyn ActivityRecorder + 'recorder>>,
    process_mode: ProcessMode,
    should_validate: bool,
) -> GitHandler<'recorder> {
    let post_clone_hook_path = std::env::var("POST_CLONE_HOOK_PATH").unwrap_or_default();
    if !post_clone_hook_path.is_empty() {
        tracing::info!(hook_path = %post_clone_hook_path, "Using post-clone hook from environment");
    }

    let git_proxy_factory: Arc<dyn GitProxyFactory> = Arc::new(ProductionGitProxyFactory::new());
    GitHandler {
        base_dir: base_dir.into(),
        auth_context,
        sdk_base_url: sdk_base_url.into(),
        repo_branches,
        last_auth_provider: std::sync::Mutex::new(None),
        activity_recorder,
        process_mode,
        git_proxy_manager: std::sync::RwLock::new(None),
        git_proxy_factory: std::sync::RwLock::new(Some(git_proxy_factory)),
        should_validate,
        post_clone_hook_path,
    }
}

impl GitHandler<'_> {
    /// 只接受精确的 `git_repository` 标签。
    #[must_use]
    pub fn can_handle(&self, source_type: &str) -> bool {
        source_type == "git_repository"
    }

    /// 覆盖生产默认的 Git proxy factory；传 `None` 可在隔离测试中显式禁用。
    ///
    /// 构造函数保持 参数表不变，因此测试 adapter 通过单独 setter 覆盖；替换
    /// factory 不会停止已经启动并存入 handler 的 manager。
    pub fn set_git_proxy_factory(&self, factory: Option<Arc<dyn GitProxyFactory>>) {
        *self
            .git_proxy_factory
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = factory;
    }

    /// 准备目录、认证 URL、clone/fetch 并处理 outcome branch。
    ///
    /// `fresh` 删除现有 clone；`allow-prefetched` 复用 `.git`；其余模式在目录检查上
    /// 都走 resume 分支，但只有精确的 `resume` 会容忍 fetch 失败后继续 branch setup。
    pub async fn process(
        &self,
        cancellation: &CancellationToken,
        source: &dyn SourceValue,
    ) -> SourcesResult<()> {
        let git_source = source
            .as_git_repository()
            .ok_or_else(|| SourcesError::message("source is not a GitRepositorySource"))?;
        let process_started = Instant::now();
        let process_mode = self.process_mode.as_str();
        let operation = if process_mode == "fresh" {
            "clone"
        } else {
            "fetch"
        };

        tracing::info!("source_type" = ?(&git_source.base.source_type), "repo" = ?(&git_source.git_info.repo), "host" = ?(&git_source.git_info.host), "has_auth" = ?(git_source.git_info.auth.is_some()), "process_mode" = ?(process_mode), "{}", format!("Starting repository {operation}"));

        let activity_guard = if process_mode != "allow-prefetched" {
            self.activity_recorder.as_ref().map(|recorder| {
                let message = if process_mode == "resume" {
                    format!("Fetching repository {}", git_source.git_info.repo)
                } else {
                    format!("Cloning repository {}", git_source.git_info.repo)
                };
                ActivityStopGuard::new(Some(
                    recorder.record_long_running_activity(LogCategory::Init, message),
                ))
            })
        } else {
            None
        };

        // Stop 必须覆盖本函数内全部 `return`。Rust 的 Drop 无法安全等待 Tokio
        // callback，因此把业务体包进内部 future，收集结果后再异步停止。
        let process_result = async {
            let git_url = self.git_url(git_source)?;
        if git_source
            .git_info
            .url
            .as_ref()
            .is_some_and(|url| !url.is_empty())
        {
            tracing::info!("repo_url" = ?(&git_url), "repo" = ?(&git_source.git_info.repo), "{}", "Using custom git URL");
        }
        let repo_path = git_source.get_directory(&self.base_dir);
        if repo_path.as_os_str().is_empty() {
            return Err(SourcesError::message(format!(
                "could not determine repository directory for repo: {}",
                git_source.git_info.repo
            )));
        }

        match process_mode {
            "fresh" => {
                create_dir_all_0755(&self.base_dir).await.map_err(|error| {
                    SourcesError::message(format!(
                        "failed to create base directory {}: {error}",
                        self.base_dir.display()
                    ))
                })?;
                if tokio::fs::metadata(&repo_path).await.is_ok() {
                    tracing::warn!("repo_path" = ?(repo_path.to_string_lossy()), "{}", "Repository directory already exists, removing it");
                    remove_all(&repo_path).await.map_err(|error| {
                        SourcesError::message(format!(
                            "failed to remove existing directory: {error}"
                        ))
                    })?;
                }
            }
            "allow-prefetched" => {
                if matches!(
                    tokio::fs::metadata(&repo_path).await,
                    Err(error) if error.kind() == io::ErrorKind::NotFound
                ) {
                    tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "repo" = ?(&git_source.git_info.repo), "{}", "Repository directory does not exist in allow-prefetched mode, will fall back to fresh clone");
                    create_dir_all_0755(&self.base_dir).await.map_err(|error| {
                        SourcesError::message(format!(
                            "failed to create base directory {}: {error}",
                            self.base_dir.display()
                        ))
                    })?;
                }
            }
            _ => {
                if tokio::fs::metadata(&repo_path).await.is_err() {
                    return Err(SourcesError::message(format!(
                        "repository directory does not exist in resume mode: {}",
                        repo_path.display()
                    )));
                }
            }
        }

        let auth_provider = if git_source
            .git_info
            .auth
            .as_ref()
            .is_some_and(|auth| !auth.token.is_empty())
        {
            let provider = self.create_source_auth_provider(&git_source.git_info);
            if let Some(provider) = &provider {
                *self
                    .last_auth_provider
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(provider));
            }
            provider
        } else {
            None
        };

        let (mut authenticated_url, _) = self.get_authenticated_url(
            &git_url,
            auth_provider.as_deref(),
            &Permission::from("read"),
        );
        if authenticated_url.is_empty() {
            authenticated_url.clone_from(&git_url);
        }

        if self.should_validate {
            let validation_guard = if process_mode == "allow-prefetched" {
                self.activity_recorder.as_ref().map(|recorder| {
                    ActivityStopGuard::new(Some(recorder.record_long_running_activity(
                        LogCategory::Init,
                        format!("Validating {} repository access", git_source.git_info.repo),
                    )))
                })
            } else {
                None
            };
            tracing::info!("repo" = ?(&git_source.git_info.repo), "{}", "Validating repository access");
            let validation_result = self
                .validate_repository_access(
                    cancellation,
                    &git_url,
                    &authenticated_url,
                    auth_provider.as_deref(),
                )
                .await;
            if let Some(validation_guard) = validation_guard {
                validation_guard.stop().await;
            }
            if let Err(error) = validation_result {
                return Err(SourcesError::message(format!(
                    "repository access validation failed: {error}"
                )));
            }
            tracing::info!("repo" = ?(&git_source.git_info.repo), "{}", "Repository access validated successfully");
        }

        let clone_error = self
            .clone_repository(
                cancellation,
                &git_url,
                git_source.git_info.git_ref.as_deref(),
                &repo_path,
                &authenticated_url,
                auth_provider.as_deref(),
                &git_source.git_info.repo,
            )
            .await
            .err()
            .map(|error| error.to_string());

        if let Some(error) = &clone_error {
            let duration_ms = elapsed_milliseconds(process_started);
            tracing::error!("repo" = ?(&git_source.git_info.repo), "repo_path" = ?(repo_path.to_string_lossy()), "duration_ms" = ?(duration_ms), "error" = ?(error), "{}", "Failed to clone repository");
            if process_mode != "resume" {
                return Err(SourcesError::message(format!(
                    "failed to clone repository: {error}"
                )));
            }
            tracing::info!("repo" = ?(&git_source.git_info.repo), "{}", "Continuing to branch setup despite clone failure in resume mode");
        }

        if let Err(error) = self
            .setup_branch_from_outcomes(
                cancellation,
                &repo_path,
                &git_url,
                &authenticated_url,
                auth_provider.as_deref(),
                git_source,
            )
            .await
        {
            if let Some(clone_error) = &clone_error {
                return Err(SourcesError::message(format!(
                    "failed to setup branch from outcomes (clone also failed: {clone_error}): {error}"
                )));
            }
            return Err(SourcesError::message(format!(
                "failed to setup branch from outcomes: {error}"
            )));
        }

        let duration_ms = elapsed_milliseconds(process_started);
        if let Some(error) = clone_error {
            tracing::warn!("host" = ?(&git_source.git_info.host), "repo" = ?(&git_source.git_info.repo), "repo_path" = ?(repo_path.to_string_lossy()), "duration_ms" = ?(duration_ms), "error" = ?(&error), "{}", "Repository branch setup succeeded but clone/fetch had errors");
            return Err(SourcesError::message(format!(
                "clone/fetch failed in resume mode: {error}"
            )));
        }
        tracing::info!("host" = ?(&git_source.git_info.host), "repo" = ?(&git_source.git_info.repo), "repo_path" = ?(repo_path.to_string_lossy()), "duration_ms" = ?(duration_ms), "{}", "Git repository cloned successfully");
            Ok(())
        }
        .await;
        if let Some(activity_guard) = activity_guard {
            activity_guard.stop().await;
        }
        process_result
    }

    /// 根据 custom URL 或 host 标签构造公开 Git URL。
    fn git_url(&self, source: &GitRepositorySource) -> SourcesResult<String> {
        if let Some(url) = source.git_info.url.as_ref().filter(|url| !url.is_empty()) {
            return Ok(url.clone());
        }
        match source.git_info.host.as_str() {
            "github" => Ok(format!("https://github.com/{}", source.git_info.repo)),
            "test-file" => Ok(format!("file://{}", source.git_info.repo)),
            "github-ssh" => Ok(format!("git@github.com:{}.git", source.git_info.repo)),
            unsupported => Err(SourcesError::message(format!(
                "unsupported git provider type: {unsupported}"
            ))),
        }
    }

    /// 任何带 auth 的 GitInfo 都创建 GitHub provider。
    fn create_source_auth_provider(&self, git_info: &GitInfo) -> Option<SharedAuthProvider> {
        let auth = git_info.auth.as_ref()?;
        Some(Arc::new(new_github_source_auth_provider(
            auth.token.clone(),
        )))
    }

    /// provider 或 auth context 缺失时返回原 URL。
    #[must_use]
    pub(crate) fn get_authenticated_url(
        &self,
        git_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
        permission: &Permission,
    ) -> (String, bool) {
        match (auth_provider, self.auth_context.as_deref()) {
            (Some(provider), Some(auth_context)) => {
                provider.authenticate_url(auth_context, git_url, permission)
            }
            _ => (git_url.to_owned(), false),
        }
    }

    /// 最多执行五次指数退避 fetch。
    ///
    /// 第四、五次在原参数前加 `-c http.version=HTTP/1.1`；第五次额外设置
    /// `GIT_TRACE_PACKET=1`。退避由 cancellation 打断时保留 文案中的
    /// `cancelled` 双 l 拼写。
    async fn run_git_fetch_with_retry(
        &self,
        cancellation: &CancellationToken,
        dir: &Path,
        git_url: &str,
        authenticated_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
        args: &[String],
    ) -> SourcesResult<()> {
        const MAX_ATTEMPTS: usize = 5;
        let base_delay_seconds = 4 + unix_nanos_mod(5);
        let mut last_error = None;

        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                let multiplier = 1_u64 << (attempt - 1);
                let delay = Duration::from_secs(multiplier * base_delay_seconds);
                tracing::info!("attempt" = ?(attempt + 1), "max_attempts" = ?(MAX_ATTEMPTS), "delay_seconds" = ?(delay.as_secs()), "base_delay_seconds" = ?(base_delay_seconds), "{}", "Retrying git fetch after failure");
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        return Err(SourcesError::message(
                            "retry backoff cancelled",
                        ));
                    }
                    _ = tokio::time::sleep(delay) => {}
                }
            }

            let mut attempt_args = args.to_vec();
            let mut environment = Vec::new();
            if attempt >= 3 {
                tracing::info!("attempt" = ?(attempt + 1), "{}", "Using HTTP/1.1 for this attempt");
                attempt_args.splice(0..0, ["-c".to_owned(), "http.version=HTTP/1.1".to_owned()]);
                if attempt == MAX_ATTEMPTS - 1 {
                    tracing::info!("attempt" = ?(attempt + 1), "{}", "Enabling GIT_TRACE_PACKET for debugging");
                    environment.push(("GIT_TRACE_PACKET".to_owned(), "1".to_owned()));
                }
            }

            match self
                .run_git_command(
                    cancellation,
                    Some(dir),
                    git_url,
                    authenticated_url,
                    auth_provider,
                    &environment,
                    &attempt_args,
                )
                .await
            {
                Ok(()) => {
                    if attempt > 0 {
                        tracing::info!("attempt" = ?(attempt + 1), "max_attempts" = ?(MAX_ATTEMPTS), "{}", "Git fetch succeeded after retry");
                    }
                    return Ok(());
                }
                Err(error) => {
                    last_error = Some(error.to_string());
                    if attempt < MAX_ATTEMPTS - 1 {
                        tracing::warn!("attempt" = ?(attempt + 1), "max_attempts" = ?(MAX_ATTEMPTS), "error" = ?(error.to_string()), "{}", "Git fetch failed, will retry");
                    }
                }
            }
        }

        let last_error = last_error.unwrap_or_default();
        tracing::error!("attempts" = ?(MAX_ATTEMPTS), "error" = ?(&last_error), "{}", "Git fetch failed after all retries");
        Err(SourcesError::message(format!(
            "git fetch failed after {MAX_ATTEMPTS} attempts: {last_error}"
        )))
    }

    /// 用 `git ls-remote` 最多验证三次。
    pub async fn validate_repository_access(
        &self,
        cancellation: &CancellationToken,
        git_url: &str,
        authenticated_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
    ) -> SourcesResult<()> {
        const MAX_ATTEMPTS: usize = 3;
        let base_delay_seconds = 2 + unix_nanos_mod(3);
        let mut last_error = None;

        tracing::info!("repo_url" = ?(git_url), "max_attempts" = ?(MAX_ATTEMPTS), "{}", "Validating repository access via git proxy");

        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                let delay = Duration::from_secs(
                    (1_u64 << (attempt - 1)).saturating_mul(base_delay_seconds),
                );
                tracing::info!("attempt" = ?(attempt + 1), "max_attempts" = ?(MAX_ATTEMPTS), "delay_seconds" = ?(delay.as_secs()), "{}", "Retrying git proxy validation");
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        return Err(SourcesError::message(
                            "validation retry backoff cancelled",
                        ));
                    }
                    _ = tokio::time::sleep(delay) => {}
                }
            }

            let args = vec![
                "ls-remote".to_owned(),
                "--heads".to_owned(),
                authenticated_url.to_owned(),
                "HEAD".to_owned(),
            ];
            match self
                .run_git_command(
                    cancellation,
                    None,
                    git_url,
                    authenticated_url,
                    auth_provider,
                    &[],
                    &args,
                )
                .await
            {
                Ok(()) => {
                    if attempt > 0 {
                        tracing::info!("attempt" = ?(attempt + 1), "repo_url" = ?(git_url), "{}", "Git proxy validation succeeded after retry");
                    } else {
                        tracing::info!("repo_url" = ?(git_url), "{}", "Git proxy validation succeeded");
                    }
                    return Ok(());
                }
                Err(error) => {
                    last_error = Some(error.to_string());
                    if attempt < MAX_ATTEMPTS - 1 {
                        tracing::warn!("attempt" = ?(attempt + 1), "max_attempts" = ?(MAX_ATTEMPTS), "repo_url" = ?(git_url), "error" = ?(error.to_string()), "{}", "Git proxy validation failed, will retry");
                    }
                }
            }
        }

        let last_error = last_error.unwrap_or_default();
        tracing::error!("max_attempts" = ?(MAX_ATTEMPTS), "repo_url" = ?(git_url), "error" = ?(&last_error), "{}", "Git proxy validation failed after all retries");
        Err(SourcesError::message(format!(
            "git proxy validation failed after {MAX_ATTEMPTS} attempts: {}",
            last_error
        )))
    }

    /// 统一追加 proxy auth 配置并捕获合并输出。
    ///
    /// 日志中的命令参数会脱敏；错误输出还会交给 auth provider 二次清理。实际 argv
    /// 始终使用原字符串直接传给 `git`，不会经过 shell 或重新编码。
    #[allow(clippy::too_many_arguments)]
    async fn run_git_command(
        &self,
        cancellation: &CancellationToken,
        dir: Option<&Path>,
        git_url: &str,
        authenticated_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
        environment: &[(String, String)],
        args: &[String],
    ) -> SourcesResult<()> {
        // 在进入 `runGitCommand` 后立即启动计时，参数复制、脱敏和命令构造都属于
        // `duration_ms`；起点不能下移到 spawn，否则短命令的毫秒边界会产生差分。
        let command_started = Instant::now();
        let mut git_args = vec!["-c".to_owned(), GIT_HTTP_PROXY_AUTH_CONFIG.to_owned()];
        git_args.extend_from_slice(args);

        let sanitized_args = git_args
            .iter()
            .map(|argument| {
                if argument == authenticated_url && argument != git_url {
                    return self.sanitize_url(authenticated_url);
                }
                if argument != git_url && argument.contains("://") && argument.contains('@') {
                    return self.sanitize_url(argument);
                }
                argument.clone()
            })
            .collect::<Vec<_>>();
        let command = sanitized_args.join(" ");
        tracing::info!("command" = ?(format!("git {command}")), "dir" = ?(dir.map_or_else(String::new, |path| path.to_string_lossy().into_owned())), "{}", "Executing git command");

        // 两个输出流共享一个 pipe，错误日志和返回错误都能观察真实交错顺序。
        let outcome = run_external_command(
            cancellation,
            "git",
            &git_args,
            dir,
            environment,
            CaptureMode::Combined,
        )
        .await;
        let duration_ms = elapsed_milliseconds(command_started);
        let (output, error) = match outcome {
            Ok(outcome) => {
                let error = (!outcome.status.success()).then(|| outcome.status.to_string());
                (outcome.output.expect_combined(), error)
            }
            Err(outcome) => {
                let error = outcome.to_string();
                (outcome.output.expect_combined(), Some(error))
            }
        };
        if let Some(error) = error {
            let mut output = String::from_utf8_lossy(&output).into_owned();
            if let Some(provider) = auth_provider {
                output = provider.sanitize_output(&output, git_url, authenticated_url);
            }
            tracing::error!("command" = ?(&sanitized_args), error, output, duration_ms, "Git command failed");
            return Err(SourcesError::message(format!(
                "git {command} failed: {error}\nOutput: {output}"
            )));
        }
        tracing::info!(command = %format!("git {command}"), duration_ms, output_size = output.len(), "Git command succeeded");
        Ok(())
    }

    /// 执行 clone/fetch 主流程，并在返回前恢复公开 origin。
    ///
    /// Rust 不能在 async `Drop` 中 await，因此先保存主体结果，再执行恢复命令，
    /// 最后原样返回主体错误。恢复失败只记录 warning。
    #[allow(clippy::too_many_arguments)]
    async fn clone_repository(
        &self,
        cancellation: &CancellationToken,
        git_url: &str,
        git_ref: Option<&str>,
        repo_path: &Path,
        authenticated_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
        repo_name: &str,
    ) -> SourcesResult<()> {
        let using_authenticated_url = git_url != authenticated_url;
        tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "repo_url" = ?(git_url), "repo_name" = ?(repo_name), "authenticated" = ?(using_authenticated_url), "{}", "Cloning repository");
        let result = self
            .clone_repository_inner(
                cancellation,
                git_url,
                git_ref,
                repo_path,
                authenticated_url,
                auth_provider,
                repo_name,
            )
            .await;

        if using_authenticated_url {
            tracing::debug!("{}", "Resetting origin URL to non-authenticated URL");
            let reset_args = vec![
                "remote".to_owned(),
                "set-url".to_owned(),
                "origin".to_owned(),
                git_url.to_owned(),
            ];
            if let Err(error) = self
                .run_git_command(
                    cancellation,
                    Some(repo_path),
                    git_url,
                    authenticated_url,
                    auth_provider,
                    &[],
                    &reset_args,
                )
                .await
            {
                tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to reset origin URL");
            }
        }
        result
    }

    /// 实现 clone/fetch 的主体，使公开 URL 恢复逻辑能覆盖每个提前返回分支。
    #[allow(clippy::too_many_arguments)]
    async fn clone_repository_inner(
        &self,
        cancellation: &CancellationToken,
        git_url: &str,
        git_ref: Option<&str>,
        repo_path: &Path,
        authenticated_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
        repo_name: &str,
    ) -> SourcesResult<()> {
        let clone_started = Instant::now();
        let process_mode = self.process_mode.as_str();
        let allow_prefetched = process_mode == "allow-prefetched";
        let prefetched_repo_exists =
            allow_prefetched && tokio::fs::metadata(repo_path.join(".git")).await.is_ok();
        if prefetched_repo_exists {
            tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "{}", "Repository already exists in allow-prefetched mode, using existing clone");
        }

        match (process_mode, prefetched_repo_exists) {
            ("fresh", _) => {
                self.init_repository(
                    cancellation,
                    repo_path,
                    git_url,
                    authenticated_url,
                    auth_provider,
                )
                .await?;
            }
            (_, true) if allow_prefetched => {
                let args = vec![
                    "remote".to_owned(),
                    "set-url".to_owned(),
                    "origin".to_owned(),
                    authenticated_url.to_owned(),
                ];
                self.run_git_command(
                    cancellation,
                    Some(repo_path),
                    git_url,
                    authenticated_url,
                    auth_provider,
                    &[],
                    &args,
                )
                .await?;
            }
            _ => {
                self.init_repository(
                    cancellation,
                    repo_path,
                    git_url,
                    authenticated_url,
                    auth_provider,
                )
                .await?;
            }
        }

        if let Some(ref_name) = git_ref.filter(|value| !value.is_empty()) {
            if allow_prefetched && prefetched_repo_exists {
                if let Some(target_branch) = self
                    .repo_branches
                    .as_ref()
                    .and_then(|branches| branches.get(repo_name))
                    .filter(|branch| !branch.is_empty())
                {
                    tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "target_branch" = ?(target_branch), "ref" = ?(ref_name), "{}", "BYOC resume: checking if task branch exists on remote");
                    if self
                        .branch_exists_on_remote(
                            cancellation,
                            repo_path,
                            authenticated_url,
                            target_branch,
                        )
                        .await
                    {
                        tracing::info!("target_branch" = ?(target_branch), "{}", "BYOC resume: task branch exists on remote, fetching it");
                        self.fetch_and_checkout_remote_branch(
                            cancellation,
                            repo_path,
                            target_branch,
                            authenticated_url,
                            git_url,
                            auth_provider,
                        )
                        .await
                        .map_err(|error| {
                            SourcesError::message(format!(
                                "failed to fetch/checkout task branch: {error}"
                            ))
                        })?;
                        tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "repo_url" = ?(git_url), "target_branch" = ?(target_branch), "{}", "Repository resumed successfully with task branch");
                        return Ok(());
                    }
                    tracing::info!("target_branch" = ?(target_branch), "ref" = ?(ref_name), "{}", "BYOC resume: task branch not found on remote, falling back to original ref");
                }

                let local_ref_args = vec![
                    "-C".to_owned(),
                    repo_path.display().to_string(),
                    "rev-parse".to_owned(),
                    "--verify".to_owned(),
                    ref_name.to_owned(),
                ];
                if run_external_command(
                    cancellation,
                    "git",
                    &local_ref_args,
                    None,
                    &[],
                    CaptureMode::Separate,
                )
                .await
                .is_ok_and(|output| output.status.success())
                {
                    tracing::info!("ref" = ?(ref_name), "{}", "Ref already exists locally in prefetched repo, skipping fetch");
                    let checkout_args = vec![
                        "-c".to_owned(),
                        "core.hooksPath=/dev/null".to_owned(),
                        "checkout".to_owned(),
                        "--force".to_owned(),
                        ref_name.to_owned(),
                    ];
                    self.run_git_command(
                        cancellation,
                        Some(repo_path),
                        git_url,
                        authenticated_url,
                        auth_provider,
                        &[],
                        &checkout_args,
                    )
                    .await
                    .map_err(|error| {
                        SourcesError::message(format!(
                            "failed to checkout local ref {ref_name}: {error}"
                        ))
                    })?;
                    tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "repo_url" = ?(git_url), "{}", "Repository cloned successfully");
                    return Ok(());
                }
                tracing::info!("ref" = ?(ref_name), "{}", "Ref not found locally, will fetch from remote");
            }

            tracing::info!("ref" = ?(ref_name), "{}", "Fetching specific ref");
            let fetch_args = vec![
                "fetch".to_owned(),
                "--no-progress".to_owned(),
                "--depth".to_owned(),
                "50".to_owned(),
                "origin".to_owned(),
                ref_name.to_owned(),
            ];
            if let Err(error) = self
                .run_git_fetch_with_retry(
                    cancellation,
                    repo_path,
                    git_url,
                    authenticated_url,
                    auth_provider,
                    &fetch_args,
                )
                .await
            {
                if self
                    .is_empty_repository(cancellation, repo_path, authenticated_url)
                    .await
                {
                    tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "repo_url" = ?(git_url), "ref" = ?(ref_name), "{}", "Repository is empty (no commits), skipping checkout");
                    return Ok(());
                }
                return Err(error);
            }
        } else {
            if allow_prefetched && prefetched_repo_exists {
                tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "{}", "Allow-prefetched mode with existing repo - using existing repository state");
                tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "repo_url" = ?(git_url), "{}", "Repository cloned successfully");
                return Ok(());
            }

            let fetch_args = vec![
                "fetch".to_owned(),
                "--depth".to_owned(),
                "50".to_owned(),
                "--no-progress".to_owned(),
                "origin".to_owned(),
                "HEAD".to_owned(),
            ];
            if process_mode == "fresh" || allow_prefetched {
                tracing::info!("{}", "Cloning default branch");
                if let Err(error) = self
                    .run_git_fetch_with_retry(
                        cancellation,
                        repo_path,
                        git_url,
                        authenticated_url,
                        auth_provider,
                        &fetch_args,
                    )
                    .await
                {
                    if self
                        .is_empty_repository(cancellation, repo_path, authenticated_url)
                        .await
                    {
                        tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "repo_url" = ?(git_url), "{}", "Repository is empty (no commits), skipping checkout");
                        return Ok(());
                    }
                    return Err(error);
                }
            } else if process_mode == "resume" {
                tracing::info!("{}", "Fetching latest HEAD for resume");
                if let Err(error) = self
                    .run_git_fetch_with_retry(
                        cancellation,
                        repo_path,
                        git_url,
                        authenticated_url,
                        auth_provider,
                        &fetch_args,
                    )
                    .await
                {
                    tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to fetch latest HEAD, continuing with existing state");
                } else {
                    tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "{}", "Successfully fetched latest HEAD");
                }
            }
        }

        let update_ref_args = vec![
            "update-ref".to_owned(),
            "--no-deref".to_owned(),
            "HEAD".to_owned(),
            "HEAD".to_owned(),
        ];
        let _ = self
            .run_git_command(
                cancellation,
                Some(repo_path),
                git_url,
                authenticated_url,
                auth_provider,
                &[],
                &update_ref_args,
            )
            .await;

        let reset_args = vec![
            "-c".to_owned(),
            "core.hooksPath=/dev/null".to_owned(),
            "reset".to_owned(),
            "--hard".to_owned(),
            "FETCH_HEAD^{commit}".to_owned(),
        ];
        self.run_git_command(
            cancellation,
            Some(repo_path),
            git_url,
            authenticated_url,
            auth_provider,
            &[],
            &reset_args,
        )
        .await?;

        let clone_duration_ms = elapsed_milliseconds(clone_started);
        // 在 后台任务 中计算 pack size，并与 post-clone Hook 并行。这里用
        // spawn_blocking 保持相同时序，也避免同步目录遍历占住 Tokio worker。
        let pack_path = repo_path.to_owned();
        let pack_task = tokio::task::spawn_blocking(move || {
            let pack_started = Instant::now();
            let result = pack_size(&pack_path);
            (result, elapsed_milliseconds(pack_started))
        });
        self.run_post_clone_hook(cancellation, repo_path).await;
        let (mut pack_size_bytes, pack_duration_ms, pack_error) = match pack_task.await {
            Ok((Ok(size), duration_ms)) => (size, duration_ms, None),
            Ok((Err(error), duration_ms)) => (0, duration_ms, Some(error.to_string())),
            Err(error) => (0, 0, Some(error.to_string())),
        };
        if let Some(error) = pack_error {
            // 只有一个 `result.err != `None`` 日志调用点；即便 Tokio worker 自身异常，
            // 也汇入同一调用点，避免 Rust 分支凭空增加一条记录。
            tracing::warn!("error" = ?(error), "{}", "Failed to calculate repo size");
            pack_size_bytes = -1;
        }
        tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "repo_url" = ?(git_url), "duration_ms" = ?(clone_duration_ms), "pack_size_bytes" = ?(pack_size_bytes), "pack_size_duration_ms" = ?(pack_duration_ms), "{}", "Repository cloned successfully");
        Ok(())
    }

    /// 初始化空仓库、best-effort 禁用 auto-gc，并添加 authenticated origin。
    async fn init_repository(
        &self,
        cancellation: &CancellationToken,
        repo_path: &Path,
        git_url: &str,
        authenticated_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
    ) -> SourcesResult<()> {
        let init_args = vec!["init".to_owned(), repo_path.display().to_string()];
        self.run_git_command(
            cancellation,
            None,
            git_url,
            authenticated_url,
            auth_provider,
            &[],
            &init_args,
        )
        .await?;

        let config_args = vec!["config".to_owned(), "gc.auto".to_owned(), "0".to_owned()];
        if let Err(error) = self
            .run_git_command(
                cancellation,
                Some(repo_path),
                git_url,
                authenticated_url,
                auth_provider,
                &[],
                &config_args,
            )
            .await
        {
            tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to disable auto gc");
        }

        let remote_args = vec![
            "remote".to_owned(),
            "add".to_owned(),
            "origin".to_owned(),
            authenticated_url.to_owned(),
        ];
        self.run_git_command(
            cancellation,
            Some(repo_path),
            git_url,
            authenticated_url,
            auth_provider,
            &[],
            &remote_args,
        )
        .await
    }

    /// 执行 `bash <hook> <repo>` 并始终写诊断文件。
    ///
    /// Hook 非零退出、启动失败或取消都只记录 warning，不改变 clone 结果；输出文件
    /// 使用 create/truncate 语义。
    async fn run_post_clone_hook(&self, cancellation: &CancellationToken, repo_path: &Path) {
        if self.post_clone_hook_path.is_empty() {
            return;
        }

        tracing::info!("hook_path" = ?(&self.post_clone_hook_path), "repo_path" = ?(repo_path.to_string_lossy()), "{}", "Running post-clone hook");

        let args = vec![
            self.post_clone_hook_path.clone(),
            repo_path.display().to_string(),
        ];
        let outcome = run_external_command(
            cancellation,
            "bash",
            &args,
            None,
            &[],
            CaptureMode::Combined,
        )
        .await;
        let (output, error) = match outcome {
            Ok(outcome) => {
                let error = (!outcome.status.success()).then(|| outcome.status.to_string());
                (outcome.output.expect_combined(), error)
            }
            Err(outcome) => {
                let error = outcome.to_string();
                (outcome.output.expect_combined(), Some(error))
            }
        };
        if let Err(error) =
            write_file_0644(Path::new("/tmp/post-clone-hook.out"), output.clone()).await
        {
            tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to write post-clone hook output to file");
        }
        if let Some(error) = error {
            tracing::warn!(error, output = %String::from_utf8_lossy(&output), repo_path = %repo_path.display(), hook_path = %self.post_clone_hook_path, "Post-clone hook failed, continuing anyway");
        } else {
            tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "hook_path" = ?(&self.post_clone_hook_path), "{}", "Post-clone hook completed successfully");
        }
    }

    /// 以空的 `ls-remote --symref ... HEAD` 判断空仓库。
    async fn is_empty_repository(
        &self,
        cancellation: &CancellationToken,
        repo_path: &Path,
        authenticated_url: &str,
    ) -> bool {
        tracing::info!("repo_path" = ?(repo_path.to_string_lossy()), "{}", "Checking if repository is empty");
        let args = vec![
            "-c".to_owned(),
            GIT_HTTP_PROXY_AUTH_CONFIG.to_owned(),
            "ls-remote".to_owned(),
            "--symref".to_owned(),
            authenticated_url.to_owned(),
            "HEAD".to_owned(),
        ];
        let outcome = run_external_command(
            cancellation,
            "git",
            &args,
            Some(repo_path),
            &[],
            CaptureMode::Separate,
        )
        .await;
        match outcome {
            Ok(outcome) => {
                let (stdout, _) = outcome.output.expect_separate();
                let trimmed_output = String::from_utf8_lossy(&stdout).trim().to_owned();
                if outcome.status.success() && trimmed_output.is_empty() {
                    tracing::info!(repo_path = %repo_path.display(), "Repository confirmed empty - no commits");
                    return true;
                }
                if !outcome.status.success() {
                    tracing::debug!(repo_path = %repo_path.display(), error = %outcome.status, output = %trimmed_output, "Failed to check if repository is empty");
                }
            }
            Err(error) => {
                tracing::debug!(repo_path = %repo_path.display(), error = %error, "Failed to check if repository is empty");
            }
        }
        false
    }

    /// 把 URL userinfo 固定替换为 `<token>`。
    #[must_use]
    pub(crate) fn sanitize_url(&self, url_string: &str) -> String {
        let Ok(parsed) = url::Url::parse(url_string) else {
            if url_string.contains("://") && url_string.contains('@') {
                let mut parts = url_string.splitn(2, "://");
                if let (Some(scheme), Some(rest)) = (parts.next(), parts.next())
                    && let Some(at_index) = rest.find('@')
                    && at_index > 0
                {
                    return format!("{scheme}://<token>{}", &rest[at_index..]);
                }
            }
            return url_string.to_owned();
        };

        if parsed.username().is_empty() && parsed.password().is_none() {
            return parsed.into();
        }

        // `url::Url::set_username` 会把 `<token>` 编码成 `%3Ctoken%3E`；这里要求日志中
        // 保留字面占位符，因此按解析后的组件重建结果。
        let authority = &parsed[url::Position::BeforeHost..url::Position::AfterPort];
        let mut sanitized = format!("{}://<token>@{authority}{}", parsed.scheme(), parsed.path());
        if let Some(query) = parsed.query() {
            sanitized.push('?');
            sanitized.push_str(query);
        }
        if let Some(fragment) = parsed.fragment() {
            sanitized.push('#');
            sanitized.push_str(fragment);
        }
        sanitized
    }

    /// 只在命令成功且 stdout 非空时返回 true。
    pub(crate) async fn branch_exists_on_remote(
        &self,
        cancellation: &CancellationToken,
        repo_path: &Path,
        authenticated_url: &str,
        branch: &str,
    ) -> bool {
        tracing::debug!("branch" = ?(branch), "{}", "Checking if branch exists on remote");
        let args = vec![
            "-c".to_owned(),
            GIT_HTTP_PROXY_AUTH_CONFIG.to_owned(),
            "ls-remote".to_owned(),
            "--heads".to_owned(),
            authenticated_url.to_owned(),
            branch.to_owned(),
        ];
        run_external_command(
            cancellation,
            "git",
            &args,
            Some(repo_path),
            &[],
            CaptureMode::Separate,
        )
        .await
        .is_ok_and(|outcome| {
            let (stdout, _) = outcome.output.expect_separate();
            outcome.status.success() && !stdout.is_empty()
        })
    }

    /// 若目标 branch 已存在于 remote 则 fetch/checkout，否则从当前 HEAD 新建。
    async fn checkout_branch(
        &self,
        cancellation: &CancellationToken,
        repo_path: &Path,
        target_branch: &str,
        git_url: &str,
        authenticated_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
    ) -> SourcesResult<()> {
        if self
            .branch_exists_on_remote(cancellation, repo_path, authenticated_url, target_branch)
            .await
        {
            self.fetch_and_checkout_remote_branch(
                cancellation,
                repo_path,
                target_branch,
                authenticated_url,
                git_url,
                auth_provider,
            )
            .await
        } else {
            self.create_local_branch(cancellation, repo_path, target_branch)
                .await
        }
    }

    /// fetch `<branch>:<branch>` 后 checkout。
    ///
    /// authenticated URL 与公开 URL 不同时，临时修改 origin；无论 fetch/checkout
    /// 成败都在返回前 best-effort 恢复公开 URL。
    #[allow(clippy::too_many_arguments)]
    async fn fetch_and_checkout_remote_branch(
        &self,
        cancellation: &CancellationToken,
        repo_path: &Path,
        target_branch: &str,
        authenticated_url: &str,
        git_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
    ) -> SourcesResult<()> {
        tracing::info!("target_branch" = ?(target_branch), "{}", "Branch exists on remote, fetching and checking out");
        if authenticated_url != git_url {
            let set_args = vec![
                "remote".to_owned(),
                "set-url".to_owned(),
                "origin".to_owned(),
                authenticated_url.to_owned(),
            ];
            if let Err(error) = self
                .run_git_command(
                    cancellation,
                    Some(repo_path),
                    git_url,
                    authenticated_url,
                    auth_provider,
                    &[],
                    &set_args,
                )
                .await
            {
                tracing::error!("error" = ?(error.to_string()), "{}", "Failed to set authenticated URL for fetch");
            }
        }

        let ref_spec = format!("{target_branch}:{target_branch}");
        let fetch_args = vec!["fetch".to_owned(), "origin".to_owned(), ref_spec];
        let result = match self
            .run_git_command(
                cancellation,
                Some(repo_path),
                git_url,
                authenticated_url,
                auth_provider,
                &[],
                &fetch_args,
            )
            .await
        {
            Ok(()) => {
                let checkout_args = vec![
                    "-c".to_owned(),
                    "core.hooksPath=/dev/null".to_owned(),
                    "checkout".to_owned(),
                    target_branch.to_owned(),
                ];
                self.run_git_command(
                    cancellation,
                    Some(repo_path),
                    git_url,
                    authenticated_url,
                    auth_provider,
                    &[],
                    &checkout_args,
                )
                .await
                .map_err(|error| {
                    SourcesError::message(format!(
                        "failed to checkout branch {target_branch}: {error}"
                    ))
                })
            }
            Err(error) => Err(SourcesError::message(format!(
                "failed to fetch branch {target_branch}: {error}"
            ))),
        };

        if authenticated_url != git_url {
            let reset_args = vec![
                "remote".to_owned(),
                "set-url".to_owned(),
                "origin".to_owned(),
                git_url.to_owned(),
            ];
            if let Err(error) = self
                .run_git_command(
                    cancellation,
                    Some(repo_path),
                    git_url,
                    authenticated_url,
                    auth_provider,
                    &[],
                    &reset_args,
                )
                .await
            {
                tracing::error!("error" = ?(error.to_string()), "{}", "Failed to reset origin URL");
            }
        }
        result
    }

    ///
    /// 从当前 HEAD 创建本地目标 branch，随后通过 `internal/util/git.rs` 的统一封装
    /// 创建 `refs/remotes/origin/<branch>`。checkout 时关闭 hooks，避免实现意外
    /// 执行仓库脚本。
    ///
    /// # 参数
    /// - `cancellation`：上层 context 的取消信号；只作用于当前直接 Git 子进程。
    /// - `repo_path`：要修改的本地 Git 工作区目录。
    /// - `target_branch`：要创建的 branch 名称，不经 shell 解释。
    ///
    /// # 返回
    /// checkout 和 remote-tracking ref 都成功时返回 `Ok(())`；任一步失败时保留 `source chain` 包装文本并返回 [`SourcesError`]。
    ///
    /// # 副作用
    /// 创建 Git 子进程、修改本地 branch/ref，并按 消息、级别和属性顺序记录
    /// 创建前、失败或成功事件。失败 `output` 保留捕获输出的尾随换行。
    async fn create_local_branch(
        &self,
        cancellation: &CancellationToken,
        repo_path: &Path,
        target_branch: &str,
    ) -> SourcesResult<()> {
        tracing::info!(
            target_branch,
            "Branch doesn't exist on remote, creating new branch locally from current HEAD"
        );

        let checkout_args = vec![
            "-c".to_owned(),
            "core.hooksPath=/dev/null".to_owned(),
            "checkout".to_owned(),
            "-b".to_owned(),
            target_branch.to_owned(),
        ];
        let checkout = run_external_command(
            cancellation,
            "git",
            &checkout_args,
            Some(repo_path),
            &[],
            CaptureMode::Combined,
        )
        .await;
        let failure = match checkout {
            Ok(checkout) if checkout.status.success() => None,
            Ok(checkout) => Some((
                checkout.status.to_string(),
                checkout.output.expect_combined(),
            )),
            Err(checkout) => {
                let error = checkout.to_string();
                Some((error, checkout.output.expect_combined()))
            }
        };
        if let Some((error, output)) = failure {
            tracing::error!(
                target_branch,
                error,
                output = %String::from_utf8_lossy(&output),
                "Failed to create new branch"
            );
            return Err(SourcesError::message(format!(
                "failed to create branch {target_branch}: {error}"
            )));
        }

        update_remote_tracking_branch(cancellation, repo_path, "origin", target_branch, "HEAD")
            .await
            .map_err(|error| SourcesError::message(error.to_string()))?;

        tracing::info!(target_branch, "Successfully created new branch locally");
        Ok(())
    }

    /// 按 repo 名查找唯一目标 branch。
    #[allow(clippy::too_many_arguments)]
    async fn setup_branch_from_outcomes(
        &self,
        cancellation: &CancellationToken,
        repo_path: &Path,
        git_url: &str,
        authenticated_url: &str,
        auth_provider: Option<&(dyn SourceAuthProvider + Send + Sync)>,
        git_source: &GitRepositorySource,
    ) -> SourcesResult<()> {
        let Some(target_branch) = self
            .repo_branches
            .as_ref()
            .and_then(|branches| branches.get(&git_source.git_info.repo))
            .filter(|branch| !branch.is_empty())
        else {
            return Ok(());
        };

        tracing::info!("repo" = ?(&git_source.git_info.repo), "target_branch" = ?(target_branch), "process_mode" = ?(self.process_mode.as_str()), "{}", "Processing branch from outcomes");

        let current_args = vec!["branch".to_owned(), "--show-current".to_owned()];
        let current = run_external_command(
            cancellation,
            "git",
            &current_args,
            Some(repo_path),
            &[],
            CaptureMode::Separate,
        )
        .await;
        let already_on_target = current.is_ok_and(|current| {
            let (stdout, _) = current.output.expect_separate();
            current.status.success() && String::from_utf8_lossy(&stdout).trim() == target_branch
        });
        if already_on_target {
            tracing::info!("target_branch" = ?(target_branch), "{}", "Already on target branch, skipping checkout");
            return Ok(());
        }

        if self.process_mode.as_str() == "resume" {
            tracing::info!("target_branch" = ?(target_branch), "{}", "Resume mode: creating local branch directly");
            self.create_local_branch(cancellation, repo_path, target_branch)
                .await
                .map_err(|error| {
                    SourcesError::message(format!(
                        "failed to create local branch {target_branch}: {error}"
                    ))
                })?;
        } else {
            self.checkout_branch(
                cancellation,
                repo_path,
                target_branch,
                git_url,
                authenticated_url,
                auth_provider,
            )
            .await
            .map_err(|error| {
                SourcesError::message(format!(
                    "failed to checkout branch {target_branch}: {error}"
                ))
            })?;
        }
        tracing::info!("target_branch" = ?(target_branch), "{}", "Successfully processed branch");
        Ok(())
    }

    /// 无自定义 URL 或 repo 不存在时成功 no-op。
    pub async fn update_remote_url(
        &self,
        cancellation: &CancellationToken,
        source: &dyn SourceValue,
    ) -> SourcesResult<()> {
        let git_source = source
            .as_git_repository()
            .ok_or_else(|| SourcesError::message("source is not a GitRepositorySource"))?;
        let Some(custom_url) = git_source
            .git_info
            .url
            .as_ref()
            .filter(|url| !url.is_empty())
        else {
            tracing::debug!("repo" = ?(&git_source.git_info.repo), "{}", "No custom URL specified, skipping remote URL update");
            return Ok(());
        };

        let repo_path = git_source.get_directory(&self.base_dir);
        if repo_path.as_os_str().is_empty() {
            return Err(SourcesError::message(format!(
                "could not determine repository directory for repo: {}",
                git_source.git_info.repo
            )));
        }
        if tokio::fs::metadata(&repo_path).await.is_err() {
            tracing::debug!("repo_path" = ?(repo_path.to_string_lossy()), "repo" = ?(&git_source.git_info.repo), "{}", "Repository directory does not exist, skipping remote URL update");
            return Ok(());
        }

        tracing::info!("repo" = ?(&git_source.git_info.repo), "custom_url" = ?(custom_url), "{}", "Updating to custom git URL");

        let args = vec![
            "remote".to_owned(),
            "set-url".to_owned(),
            "origin".to_owned(),
            custom_url.clone(),
        ];
        let outcome = run_external_command(
            cancellation,
            "git",
            &args,
            Some(&repo_path),
            &[],
            CaptureMode::Combined,
        )
        .await;
        let failure = match outcome {
            Ok(outcome) if outcome.status.success() => None,
            Ok(outcome) => Some((outcome.status.to_string(), outcome.output.expect_combined())),
            Err(outcome) => {
                let error = outcome.to_string();
                Some((error, outcome.output.expect_combined()))
            }
        };
        if let Some((error, output)) = failure {
            tracing::error!(repo = %git_source.git_info.repo, repo_path = %repo_path.display(), error, output = %String::from_utf8_lossy(&output), "Failed to update remote URL");
            return Err(SourcesError::message(format!(
                "failed to update remote URL for {}: {error}",
                git_source.git_info.repo
            )));
        }
        tracing::info!("repo" = ?(&git_source.git_info.repo), "repo_path" = ?(repo_path.to_string_lossy()), "custom_url" = ?(custom_url), "{}", "Successfully updated git remote URL");
        Ok(())
    }

    /// 收集有 token 的 Git source 并启动 proxy。
    ///
    /// 没有带认证仓库时成功 no-op。确实需要 proxy 却未注入 factory 时返回带
    /// `failed to create git proxy manager` 前缀的错误，保持原调用层包装语义。
    pub async fn setup_git_proxy_after_sources_processed(
        &self,
        cancellation: &CancellationToken,
        sources: &[Option<&dyn SourceValue>],
    ) -> SourcesResult<()> {
        let mut repo_auths = Vec::new();
        for source in sources.iter().flatten() {
            if source.get_type() != "git_repository" {
                continue;
            }
            let Some(git_source) = source.as_git_repository() else {
                continue;
            };
            let Some(auth) = git_source
                .git_info
                .auth
                .as_ref()
                .filter(|auth| !auth.token.is_empty())
            else {
                tracing::debug!("repo" = ?(&git_source.git_info.repo), "{}", "Skipping repo without auth");
                continue;
            };
            repo_auths.push(RepoAuth {
                repo: git_source.git_info.repo.clone(),
                host: git_source.git_info.host.clone(),
                auth_token: auth.token.clone(),
            });
        }

        if repo_auths.is_empty() {
            tracing::info!("{}", "No repositories configured for git proxy");
            return Ok(());
        }
        let session_id = self
            .auth_context
            .as_deref()
            .map(AuthContext::get_session_id)
            .unwrap_or_default();
        if session_id.is_empty() {
            return Err(SourcesError::message("session ID not available"));
        }

        let token_backend = if std::env::var("ENVRUNNER_SKIP_CLAUDE_CODE").as_deref() == Ok("true")
        {
            "tvs".to_owned()
        } else {
            String::new()
        };
        let config = GitProxyConfig {
            session_ingress_url: self.sdk_base_url.clone(),
            session_id: session_id.to_owned(),
            repo_auths,
            token_backend,
        };
        let repo_count = config.repo_auths.len();
        let factory = self
            .git_proxy_factory
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or_else(|| {
                SourcesError::message(
                    "failed to create git proxy manager: git proxy manager factory is not configured",
                )
            })?;
        let manager = factory.create(config).map_err(|error| {
            SourcesError::message(format!("failed to create git proxy manager: {error}"))
        })?;
        let proxy_url = manager.start(cancellation).await.map_err(|error| {
            SourcesError::message(format!("failed to start git proxy: {error}"))
        })?;
        *self
            .git_proxy_manager
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&manager));
        tracing::info!("proxy_url" = ?(&proxy_url), "repo_count" = ?(repo_count), "{}", "Local git proxy started for all repositories");

        for source in sources.iter().flatten() {
            if source.get_type() != "git_repository" {
                continue;
            }
            let Some(git_source) = source.as_git_repository() else {
                continue;
            };
            if !git_source
                .git_info
                .auth
                .as_ref()
                .is_some_and(|auth| !auth.token.is_empty())
            {
                continue;
            }
            let repo_path = git_source.get_directory(&self.base_dir);
            if repo_path.as_os_str().is_empty() {
                tracing::warn!("repo" = ?(&git_source.git_info.repo), "{}", "Could not determine repo path for proxy setup");
                continue;
            }
            if let Err(error) = self
                .setup_local_git_proxy(cancellation, git_source, &repo_path)
                .await
            {
                tracing::warn!("repo" = ?(&git_source.git_info.repo), "error" = ?(error.to_string()), "{}", "Failed to setup local git proxy for repo");
            }
        }
        Ok(())
    }

    /// 把单个 origin 改为 manager 生成的本地 URL。
    async fn setup_local_git_proxy(
        &self,
        cancellation: &CancellationToken,
        git_source: &GitRepositorySource,
        repo_path: &Path,
    ) -> SourcesResult<()> {
        let manager = self
            .git_proxy_manager
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .filter(|manager| manager.is_running())
            .ok_or_else(|| SourcesError::message("git proxy is not running"))?;
        let proxy_url = manager.get_proxy_url(&git_source.git_info.repo);
        let args = vec![
            "remote".to_owned(),
            "set-url".to_owned(),
            "origin".to_owned(),
            proxy_url.clone(),
        ];
        let outcome = run_external_command(
            cancellation,
            "git",
            &args,
            Some(repo_path),
            &[],
            CaptureMode::Combined,
        )
        .await;
        let failure = match outcome {
            Ok(outcome) if outcome.status.success() => None,
            Ok(outcome) => Some((outcome.status.to_string(), outcome.output.expect_combined())),
            Err(outcome) => {
                let error = outcome.to_string();
                Some((error, outcome.output.expect_combined()))
            }
        };
        if let Some((error, output)) = failure {
            let output = String::from_utf8_lossy(&output);
            return Err(SourcesError::message(format!(
                "failed to update git remote to use proxy: {error}, output: {output}"
            )));
        }
        tracing::info!("repo" = ?(&git_source.git_info.repo), "proxy_url" = ?(&proxy_url), "{}", "Git remote updated to use local proxy");
        Ok(())
    }
}

#[async_trait]
impl SourceHandler for GitHandler<'_> {
    /// 转发到同名固有方法。
    fn can_handle(&self, source_type: &str) -> bool {
        GitHandler::can_handle(self, source_type)
    }

    /// 转发到同名异步固有方法。
    async fn process(
        &self,
        cancellation: &CancellationToken,
        source: &dyn SourceValue,
    ) -> SourcesResult<()> {
        GitHandler::process(self, cancellation, source).await
    }

    /// Git handler 自身实现 remote 更新能力。
    fn as_git_remote_updater(&self) -> Option<&dyn GitRemoteUpdater> {
        Some(self)
    }

    /// Git handler 返回自身供 proxy 后处理使用。
    fn as_git_handler(&self) -> Option<&GitHandler<'_>> {
        Some(self)
    }
}

#[async_trait]
impl GitRemoteUpdater for GitHandler<'_> {
    /// 转发到 Git handler 的公开更新方法。
    async fn update_remote_url(
        &self,
        cancellation: &CancellationToken,
        source: &dyn SourceValue,
    ) -> SourcesResult<()> {
        GitHandler::update_remote_url(self, cancellation, source).await
    }
}

/// 只累计 `.git/objects/pack` 下的普通 `.pack` 文件。
pub(crate) fn pack_size(repo_path: &Path) -> SourcesResult<i64> {
    let pack_dir = repo_path.join(".git").join("objects").join("pack");
    let entries = std::fs::read_dir(&pack_dir).map_err(|error| {
        SourcesError::message(format!("failed to read pack directory: {error}"))
    })?;
    let mut total = 0_i64;
    for entry in entries {
        let entry = entry.map_err(|error| {
            SourcesError::message(format!("failed to read pack directory: {error}"))
        })?;
        let file_type = entry.file_type().map_err(|error| {
            SourcesError::message(format!(
                "failed to stat pack file {}: {error}",
                entry.file_name().to_string_lossy()
            ))
        })?;
        let name = entry.file_name();
        if file_type.is_dir() || !name.to_string_lossy().ends_with(".pack") {
            continue;
        }
        let metadata = entry.metadata().map_err(|error| {
            SourcesError::message(format!(
                "failed to stat pack file {}: {error}",
                name.to_string_lossy()
            ))
        })?;
        total = total.wrapping_add(metadata.len() as i64);
    }
    Ok(total)
}

/// 保存长任务 stopper，在正常返回时等待、future 被丢弃时至少同步请求停止。
pub(super) struct ActivityStopGuard {
    stopper: Option<Arc<dyn Stopper>>,
}

impl ActivityStopGuard {
    /// 包装可选 stopper；`None` 保持 no-op 语义。
    pub(super) fn new(stopper: Option<Arc<dyn Stopper>>) -> Self {
        Self { stopper }
    }

    /// 正常控制流返回前取消 stopper 并等待在途 callback。
    pub(super) async fn stop(mut self) {
        if let Some(stopper) = self.stopper.take() {
            stopper.stop().await;
        }
    }
}

impl Drop for ActivityStopGuard {
    /// future 被 abort 或 panic 展开时无法异步等待，但必须立即传播取消请求。
    ///
    /// 正常 [`Self::stop`] 会先取走 stopper，因此不会重复请求；若 `stop().await`
    /// 本身被丢弃，`Stopper::stop` 的契约保证它已在首次 poll 时同步发出取消。
    fn drop(&mut self) {
        if let Some(stopper) = self.stopper.take() {
            stopper.request_stop();
        }
    }
}

/// 启动外部命令、并发排空 stdout/stderr，并响应上层取消。
///
/// 共享执行器只终止并回收直接子进程，不设置 process group，也不遍历后代。
async fn run_external_command(
    cancellation: &CancellationToken,
    program: &str,
    args: &[String],
    dir: Option<&Path>,
    environment: &[(String, String)],
    capture: CaptureMode,
) -> Result<CommandOutput, CommandFailure> {
    let mut command = Command::new(program);
    command.args(args);
    if let Some(dir) = dir {
        command.current_dir(dir);
    }
    for (name, value) in environment {
        command.env(name, value);
    }

    run_command(command, cancellation, None, None, capture).await
}

/// 用 0755 创建目录树，并保留递归创建时的 umask 行为。
async fn create_dir_all_0755(path: &Path) -> io::Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true).mode(0o755).create(path)
    })
    .await
    .map_err(io::Error::other)?
}

/// 删除目录、普通文件或符号链接。
async fn remove_all(path: &Path) -> io::Result<()> {
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if metadata.file_type().is_dir() {
        tokio::fs::remove_dir_all(path).await
    } else {
        tokio::fs::remove_file(path).await
    }
}

/// 以 create/truncate 和 0644 mode 写文件。
async fn write_file_0644(path: &Path, contents: Vec<u8>) -> io::Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(path)?;
        file.write_all(&contents)
    })
    .await
    .map_err(io::Error::other)?
}

/// 计算当前 Unix 纳秒时间对小整数的余数，实现retry jitter。
fn unix_nanos_mod(modulus: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u64::from(duration.subsec_nanos()) % modulus)
}

/// 把单调时钟经过的时长转换成截断的 `i64` 毫秒数。
///
/// 正常 Git 操作不可能接近 `i64::MAX` 毫秒；饱和转换仅避免 Rust `u128 -> i64`
/// 截断在异常长进程上生成负数，并不改变实际可观察范围内的数值。
#[must_use]
fn elapsed_milliseconds(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}
