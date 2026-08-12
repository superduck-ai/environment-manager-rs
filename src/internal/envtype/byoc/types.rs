//! BYOC 配置、认证 transport 以及外部子系统适配边界。
//!
//! 对应 Go：`internal/envtype/byoc/byoc.go` 中的配置、环境、认证 RoundTripper 和
//! lease 创建参数。`SourceProcessor` 与 `LeaseManagerFactory` 是 Rust 为隔离测试
//! 增加的最小接口；生产仍调用真实 clone/proxy 和 heartbeat 状态机。

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, Request, Response};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::internal::auth::AuthContext;
use crate::internal::config::{SessionMode, Source, StartupContext};
use crate::internal::envtype::LeaseManagerHandle;
use crate::internal::session::ActivityRecorder;
use crate::internal::util::BoxError;

/// BYOC 环境专属的 JSON 配置。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ByocConfig {
    /// 必须为 `byoc` 的稳定类型标签。
    #[serde(default, deserialize_with = "deserialize_nullable_string")]
    pub environment_type: String,
    /// 环境工作目录；非空时必须是已归一化的绝对路径。
    #[serde(default, deserialize_with = "deserialize_nullable_string")]
    pub cwd: String,
    /// 新建或 setup-only session 中执行的一次性脚本。
    #[serde(default, deserialize_with = "deserialize_nullable_string")]
    pub task_setup_script: String,
    /// 追加到 Claude Code 环境的变量；同名键覆盖 BYOC 默认值。
    #[serde(default, deserialize_with = "deserialize_nullable_environment")]
    pub environment: BTreeMap<String, String>,
}

/// 将 Go `encoding/json` 对 null string 的“保留零值”语义映射到 Rust。
fn deserialize_nullable_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// 接受 null map 和 null string value，与 Go `map[string]string` 反序列化零值一致。
fn deserialize_nullable_environment<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let values = Option::<BTreeMap<String, Option<String>>>::deserialize(deserializer)?;
    Ok(values
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| (key, value.unwrap_or_default()))
        .collect())
}

/// BYOC 构造和初始化的类型化错误。
#[derive(Debug, Error)]
pub enum ByocError {
    /// JSON 无法解析。
    #[error("failed to unmarshal byoc config: {0}")]
    Unmarshal(#[from] serde_json::Error),
    /// `environment_type` 不是 BYOC。
    #[error("invalid environment_type: expected 'byoc', got '{actual}'")]
    InvalidEnvironmentType {
        /// 配置中的原始值。
        actual: String,
    },
    /// CWD 不是绝对路径。
    #[error("cwd must be an absolute path, got: {cwd}")]
    CwdNotAbsolute {
        /// 被拒绝的路径。
        cwd: String,
    },
    /// CWD 包含 `.`、`..`、重复分隔符或尾随分隔符。
    #[error("cwd contains path traversal elements: {cwd}")]
    CwdNotClean {
        /// 被拒绝的路径。
        cwd: String,
    },
    /// setup 脚本超过字节上限。
    #[error("task_setup_script too large: {actual} bytes (max {maximum})")]
    SetupScriptTooLarge {
        /// UTF-8 序列化后的字节数。
        actual: usize,
        /// 允许的最大字节数。
        maximum: usize,
    },
    /// 操作在启动新副作用前被取消。
    #[error("BYOC environment initialization cancelled")]
    Cancelled,
    /// 工作目录无法创建。
    #[error("failed to create working directory {cwd}: {source}")]
    CreateWorkingDirectory {
        /// 目标目录。
        cwd: String,
        /// 文件系统错误。
        #[source]
        source: std::io::Error,
    },
    /// 进程全局 CWD 无法切换。
    #[error("failed to change to working directory {cwd}: {source}")]
    ChangeWorkingDirectory {
        /// 目标目录。
        cwd: String,
        /// 文件系统错误。
        #[source]
        source: std::io::Error,
    },
    /// 临时脚本无法创建或启动。
    #[error("failed to execute {name} script: {source}")]
    ExecuteScript {
        /// 脚本逻辑名。
        name: String,
        /// process 层错误。
        #[source]
        source: crate::internal::process::ScriptError,
    },
    /// 脚本已启动，但以非零状态结束。
    #[error("{name} script failed: {source}")]
    ScriptFailed {
        /// 脚本逻辑名。
        name: String,
        /// 子进程退出错误。
        #[source]
        source: crate::internal::process::ProcessExitError,
    },
    /// initialize 在 task setup 边界追加的 Go 兼容错误上下文。
    #[error("task setup script failed: {0}")]
    TaskSetup(#[source] BoxError),
    /// 用户 home 目录不可用。
    #[error("failed to get home directory: HOME is not set")]
    HomeDirectoryMissing,
    /// Claude 设置目录或文件无法准备。
    #[error("{operation}: {source}")]
    FileSystem {
        /// 兼容 Go 错误前缀的操作文本。
        operation: &'static str,
        /// 原始 I/O 错误。
        #[source]
        source: std::io::Error,
    },
    /// outcome 指定了多个 branch。
    #[error("outcome for repo {repo} has {actual} branches, expected 0 or 1")]
    MultipleBranches {
        /// 逻辑仓库名。
        repo: String,
        /// 观察到的 branch 数。
        actual: usize,
    },
    /// source 处理失败。
    #[error("failed to checkout branches: {0}")]
    SourceProcessing(#[source] BoxError),
    /// sources manager 无法按 BYOC 参数构造。
    #[error("failed to create source handler manager: {0}")]
    SourceManager(#[source] BoxError),
    /// BYOC 初始化封装 branch checkout 错误。
    #[error("failed to checkout branches in BYOC environment: {0}")]
    BranchCheckout(#[source] BoxError),
    /// HTTP client 无法构造。
    #[error("failed to create authenticated HTTP client: {0}")]
    HttpClient(#[source] reqwest::Error),
    /// token 或 organization UUID 不是合法 header value。
    #[error("invalid {header} header value: {source}")]
    InvalidHeader {
        /// 失败的 header 名。
        header: &'static str,
        /// header 编码错误。
        #[source]
        source: reqwest::header::InvalidHeaderValue,
    },
    /// 认证 HTTP 请求失败。
    #[error("failed to execute HTTP request: {0}")]
    Http(#[source] reqwest::Error),
    /// podmonitor factory 尚未接线。
    #[error("lease manager factory is not configured for BYOC")]
    LeaseManagerFactoryUnavailable,
    /// 已注入 factory 拒绝构造 lease manager。
    #[error("failed to create BYOC lease manager: {0}")]
    LeaseManagerFactory(#[source] BoxError),
}

/// source 处理时允许的缓存策略。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceProcessMode {
    /// 按常规方式处理 source。
    Fresh,
    /// 恢复缓存 session 时允许复用预取仓库。
    AllowPrefetched,
}

/// 传给 sources 子系统的完整 BYOC 请求。
#[derive(Clone)]
pub struct SourceProcessRequest {
    /// source 工作目录。
    pub cwd: String,
    /// source 可选使用的认证上下文。
    pub auth_context: Option<AuthContext>,
    /// Environment API base URL。
    pub api_base_url: String,
    /// outcome 仓库到目标 branch 的唯一映射。
    pub repo_branches: BTreeMap<String, String>,
    /// 要处理的 source 列表。
    pub sources: Vec<Source>,
    /// 预取缓存策略。
    pub mode: SourceProcessMode,
    /// 是否在处理 source 前校验 repository access。
    pub should_validate: bool,
}

impl fmt::Debug for SourceProcessRequest {
    /// 输出 source 请求元数据，但不展开认证凭据。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceProcessRequest")
            .field("cwd", &self.cwd)
            .field("has_auth_context", &self.auth_context.is_some())
            .field("api_base_url", &self.api_base_url)
            .field("repo_branches", &self.repo_branches)
            .field("sources", &self.sources)
            .field("mode", &self.mode)
            .field("should_validate", &self.should_validate)
            .finish()
    }
}

/// source adapter 用于让 BYOC 保留 Go 的分阶段错误边界。
#[derive(Debug, Error)]
pub enum SourceProcessorError {
    /// `SourceHandlerManager` 构造失败，尚未开始 source 或 proxy 副作用。
    #[error("{0}")]
    CreateManager(#[source] BoxError),
    /// manager 已构造，但 `ProcessSources` 失败。
    #[error("{0}")]
    ProcessSources(#[source] BoxError),
    /// manager 已构造，但 Git proxy 后处理失败。
    #[error("{0}")]
    SetupGitProxy(#[source] BoxError),
}

/// BYOC 与独立 sources 子系统之间的可替换边界。
#[async_trait]
pub trait SourceProcessor: Send + Sync {
    /// clone/复用 source 并按 outcome 切换 branch。
    async fn process_sources(
        &self,
        cancellation: &CancellationToken,
        request: &SourceProcessRequest,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), SourceProcessorError>;

    /// 所有 sources 已就绪后安装 Git proxy。
    async fn setup_git_proxy_after_sources_processed(
        &self,
        cancellation: &CancellationToken,
        request: &SourceProcessRequest,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), SourceProcessorError>;
}

/// 为 Contain/Provide API 添加 ingress 认证的 Reqwest round tripper。
#[derive(Clone)]
pub struct ContainProvideAuthRoundTripper {
    pub(crate) client: Client,
    pub(crate) token: String,
    pub(crate) organization_uuid: String,
}

impl fmt::Debug for ContainProvideAuthRoundTripper {
    /// 仅输出 organization 和 token 是否存在，不泄漏 Bearer 凭据。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContainProvideAuthRoundTripper")
            .field("organization_uuid", &self.organization_uuid)
            .field("has_token", &!self.token.is_empty())
            .finish_non_exhaustive()
    }
}

/// 早期 Rust 调用方使用的兼容别名。
///
/// 新名称与 Go 的 `containProvideAuthRoundTripper` 对齐；别名保留已经采用
/// `Transport` 术语的内部调用面。
pub type ContainProvideAuthTransport = ContainProvideAuthRoundTripper;

/// podmonitor factory 创建 lease manager 所需的全部输入。
#[derive(Clone)]
pub struct LeaseManagerRequest {
    /// lease 生命周期使用的取消 token。
    pub cancellation: CancellationToken,
    /// session 标识。
    pub session_id: String,
    /// environment 标识。
    pub environment_id: String,
    /// heartbeat API base URL。
    pub api_base_url: String,
    /// organization 标识。
    pub organization_uuid: String,
    /// 已配置 BYOC headers 的 transport。
    pub transport: ContainProvideAuthTransport,
    /// 常规 heartbeat 间隔。
    pub heartbeat_interval: Duration,
    /// lease 到期前的安全窗口。
    pub safety_window: Duration,
}

impl fmt::Debug for LeaseManagerRequest {
    /// 输出 lease 元数据，但不展开认证 transport 或 token。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseManagerRequest")
            .field("session_id", &self.session_id)
            .field("environment_id", &self.environment_id)
            .field("api_base_url", &self.api_base_url)
            .field("organization_uuid", &self.organization_uuid)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("safety_window", &self.safety_window)
            .finish_non_exhaustive()
    }
}

/// podmonitor 迁移后接入的 lease manager 工厂。
pub trait LeaseManagerFactory: Send + Sync {
    /// 使用 BYOC 已认证 transport 构造句柄，不在本模块重复 heartbeat 状态机。
    fn create(&self, request: LeaseManagerRequest)
    -> Result<Arc<dyn LeaseManagerHandle>, BoxError>;
}

/// BYOC 环境的可变生命周期状态。
pub struct ByocEnvironmentType {
    /// 后端下发并通过构造校验的 BYOC 配置。
    pub(crate) config: ByocConfig,
    /// 控制 setup 脚本与缓存仓库分支的 session 模式。
    pub(crate) session_mode: SessionMode,
    /// stdin 解码出的 source、outcome、API 地址和 entrypoint 上下文。
    pub(crate) startup_context: Option<StartupContext>,
    /// sources clone/proxy 使用的认证上下文；Debug 输出不得展开其中凭据。
    pub(crate) auth_context: Option<AuthContext>,
    /// 生产使用真实 sources adapter，测试可替换以隔离 Git/网络副作用。
    pub(crate) source_processor: Arc<dyn SourceProcessor>,
    /// 生产使用真实 podmonitor adapter，测试可替换以捕获构造参数和失败。
    pub(crate) lease_manager_factory: Option<Arc<dyn LeaseManagerFactory>>,
}

impl fmt::Debug for ByocEnvironmentType {
    /// 输出可用于诊断的配置，但隐藏 setup 脚本正文和认证上下文。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ByocEnvironmentType")
            .field("environment_type", &self.config.environment_type)
            .field("cwd", &self.config.cwd)
            .field(
                "has_task_setup_script",
                &!self.config.task_setup_script.is_empty(),
            )
            .field("session_mode", &self.session_mode)
            .field("has_startup_context", &self.startup_context.is_some())
            .field("has_auth_context", &self.auth_context.is_some())
            .field("source_processor", &"configured")
            .field(
                "has_lease_manager_factory",
                &self.lease_manager_factory.is_some(),
            )
            .finish()
    }
}

impl ContainProvideAuthRoundTripper {
    /// 构造带 30 秒超时的 Reqwest client，保留 Go `http.Client` 语义。
    pub fn new(
        token: impl Into<String>,
        organization_uuid: impl Into<String>,
    ) -> Result<Self, ByocError> {
        let token = token.into();
        let organization_uuid = organization_uuid.into();
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .default_headers(authentication_headers(&token, &organization_uuid)?)
            .build()
            .map_err(ByocError::HttpClient)?;
        Ok(Self {
            client,
            token,
            organization_uuid,
        })
    }

    /// 返回复用连接池的已认证 client clone。
    ///
    /// 该 client 的 default headers 已包含 Bearer、organization、版本和 beta，
    /// 便于 podmonitor adapter 直接作为底层 HTTP client 使用。
    #[must_use]
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// 在要发送的请求上设置 BYOC 认证和版本 headers。
    pub fn authorize_request(&self, request: &mut Request) -> Result<(), ByocError> {
        let headers = authentication_headers(&self.token, &self.organization_uuid)?;
        for (name, value) in &headers {
            // `insert` 而不是 append，保留 Go Header.Set 覆盖调用方同名值的语义。
            request.headers_mut().insert(name.clone(), value.clone());
        }
        Ok(())
    }

    /// 添加 headers 并发送请求。
    ///
    /// `biased` 优先观察取消，因此入口已取消时不会把请求交给网络栈。
    pub async fn round_trip(
        &self,
        cancellation: &CancellationToken,
        mut request: Request,
    ) -> Result<Response, ByocError> {
        self.authorize_request(&mut request)?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ByocError::Cancelled),
            result = self.client.execute(request) => result.map_err(ByocError::Http),
        }
    }
}

/// 一次性校验并构造 BYOC transport 的全部默认 headers。
fn authentication_headers(
    token: &str,
    organization_uuid: &str,
) -> Result<reqwest::header::HeaderMap, ByocError> {
    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};

    let mut headers = HeaderMap::new();
    let authorization = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|source| {
        ByocError::InvalidHeader {
            header: "Authorization",
            source,
        }
    })?;
    let organization =
        HeaderValue::from_str(organization_uuid).map_err(|source| ByocError::InvalidHeader {
            header: "x-organization-uuid",
            source,
        })?;
    let runner = HeaderValue::from_str(crate::internal::util::version()).map_err(|source| {
        ByocError::InvalidHeader {
            header: "x-environment-runner-version",
            source,
        }
    })?;
    headers.insert(AUTHORIZATION, authorization);
    headers.insert(HeaderName::from_static("x-organization-uuid"), organization);
    headers.insert(
        HeaderName::from_static("x-environment-runner-version"),
        runner,
    );
    if let Some(version) = crate::internal::util::claude_code_version() {
        let version =
            HeaderValue::from_str(version).map_err(|source| ByocError::InvalidHeader {
                header: "x-claude-code-version",
                source,
            })?;
        headers.insert(HeaderName::from_static("x-claude-code-version"), version);
    }
    headers.insert(
        HeaderName::from_static("anthropic-beta"),
        HeaderValue::from_static("environments-2025-11-01"),
    );
    Ok(headers)
}
