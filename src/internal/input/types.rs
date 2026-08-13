//! 输入解析子系统的数据声明与跨模块适配边界。
//!
//! `EnvironmentConfigResponse`、`SessionContext` 与 `Outcomes` 位于输入边界，并通过
//! `From` 或 type alias 与 API/Claude 模块接线，而不改变 wire 格式。

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::internal::auth::{AuthConfig, AuthContext};
use crate::internal::config::{McpConfigFile, Source, StartupContext};

/// 统一输入 parser 接口。
///
/// boxed future 让 V0 的纯解析和 V1 的异步 session-context 获取可由同一上层调度；
/// 具体 parser 仍保留更精确的错误类型。
pub trait InputParser {
    /// 具体 parser 返回的错误类型。
    type Error;

    /// 解析一份完整 stdin/work payload。
    fn parse_input<'a>(
        &'a self,
        data: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ParsedContext, Self::Error>> + Send + 'a>>;
}

/// 输入解析完成后交给 session manager 的上下文。
#[derive(Clone, Debug)]
pub struct ParsedContext {
    /// 已验证并完成 URL 归一化的启动上下文。
    pub startup_context: StartupContext,
    /// healthcheck 不创建环境，因此该字段可为空。
    pub environment: Option<EnvironmentConfigResponse>,
    /// 解析后的认证上下文。
    pub auth_context: AuthContext,
    /// 已过滤和校验的仓库 outcomes。
    pub outcomes: Outcomes,
    /// 实际 session ID；healthcheck 为空。
    pub session_id: String,
    /// V1 work ID；V0 为空。
    pub work_id: String,
    /// V1 environment ID；V0 为空。
    pub environment_id: String,
    /// 是否为只检查 worker 健康状态的 work。
    pub is_healthcheck: bool,
}

/// 环境配置响应的本地 wire DTO。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct EnvironmentConfigResponse {
    /// 环境后端类型，例如 `anthropic` 或 `byoc`。
    pub environment_type: String,
    /// 保留未知字段的环境后端配置对象。
    pub config: Value,
    /// 旧版环境级允许工具字符串。
    #[serde(default)]
    pub allowed_tools: String,
    /// 旧版环境级禁止工具字符串。
    #[serde(default)]
    pub disallowed_tools: String,
}

/// V1 `/v2/sessions/{id}` 返回的 session context wire DTO。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct SessionContext {
    /// session 指定的工作目录。
    #[serde(default)]
    pub cwd: String,
    /// 原始 outcomes JSON；空值表示没有 outcomes。
    #[serde(default, deserialize_with = "deserialize_present_json_value")]
    pub outcomes: Option<Value>,
    /// 替换默认 system prompt 的文本。
    #[serde(default)]
    pub custom_system_prompt: String,
    /// 追加 system prompt 的文本。
    #[serde(default)]
    pub append_system_prompt: String,
    /// session 指定的 model。
    #[serde(default)]
    pub model: String,
    /// 原始 inline MCP 配置。
    #[serde(default, deserialize_with = "deserialize_present_json_value")]
    pub mcp_config: Option<Value>,
}

/// 保留 session context 中 原始 JSON 的 missing/null 差别。
fn deserialize_present_json_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

/// 仓库名到分支列表的映射。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Outcomes {
    repositories: HashMap<String, Vec<String>>,
}

impl Outcomes {
    /// 创建空 outcomes。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 添加或覆盖一个仓库，并复制分支列表。
    pub fn add(&mut self, repository: impl Into<String>, branches: &[String]) {
        self.repositories
            .insert(repository.into(), branches.to_vec());
    }

    /// 返回仓库的完整分支 slice；未知仓库返回空 slice。
    #[must_use]
    pub fn get(&self, repository: &str) -> &[String] {
        self.repositories.get(repository).map_or(&[], Vec::as_slice)
    }

    /// 返回仓库的第一个分支；不存在时返回空字符串。
    #[must_use]
    pub fn get_first(&self, repository: &str) -> &str {
        self.get(repository).first().map_or("", String::as_str)
    }

    /// 返回仓库数量。
    #[must_use]
    pub fn len(&self) -> usize {
        self.repositories.len()
    }

    /// 判断是否没有任何仓库 outcome。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.repositories.is_empty()
    }

    /// 返回映射只读引用，供后续 Claude 层转换。
    #[must_use]
    pub fn as_map(&self) -> &HashMap<String, Vec<String>> {
        &self.repositories
    }

    /// 将每个仓库的多个分支截断为第一个分支。
    ///
    /// 空分支列表会被保留。
    #[must_use]
    pub fn validate(&self) -> Self {
        let repositories = self
            .repositories
            .iter()
            .map(|(repository, branches)| {
                let kept = branches.first().cloned().into_iter().collect();
                (repository.clone(), kept)
            })
            .collect();
        Self { repositories }
    }
}

/// V0 stdin parser 的运行时配置。
#[derive(Clone, Debug, Default)]
pub struct V0Parser {
    /// CLI 提供的 session ID。
    pub session_id: String,
}

/// V1 session-context 获取请求。
#[derive(Clone, Copy, Debug)]
pub struct SessionContextRequest<'a> {
    /// work secret 中的 API base URL。
    pub api_base_url: &'a str,
    /// work secret 中的 session ingress token。
    pub session_ingress_token: &'a str,
    /// work data 中的实际 session ID。
    pub session_id: &'a str,
}

/// V1 parser 获取 session context 的异步适配器。
///
/// API 模块可在实现中使用 Reqwest/Tokio，并负责添加 Anthropic headers；输入层只
/// 依赖稳定 DTO，便于单元测试在无网络条件下注入 fixture。
pub trait SessionContextProvider: Send + Sync {
    /// 异步获取指定 session 的上下文。
    fn get_session_context<'a>(
        &'a self,
        request: SessionContextRequest<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<SessionContext, String>> + Send + 'a>>;
}

/// 未接入 API client 时的占位 provider。
#[derive(Clone, Copy, Debug, Default)]
pub struct NoSessionContextProvider;

impl SessionContextProvider for NoSessionContextProvider {
    /// 返回明确错误，防止非 code-session work 被静默按空 context 执行。
    fn get_session_context<'a>(
        &'a self,
        _request: SessionContextRequest<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<SessionContext, String>> + Send + 'a>> {
        Box::pin(async { Err("session context provider is not configured".to_owned()) })
    }
}

/// V1 work parser；provider 以泛型注入，避免在输入层绑定具体 HTTP client。
#[derive(Clone, Debug)]
pub struct V1Parser<P = NoSessionContextProvider> {
    /// session context 未指定 cwd 时的回退目录。
    pub working_directory: String,
    /// 异步 session context provider。
    pub session_context_provider: P,
}

impl V1Parser<NoSessionContextProvider> {
    /// 创建尚未接入 session API 的 V1 parser。
    #[must_use]
    pub fn without_session_context_provider(working_directory: impl Into<String>) -> Self {
        Self {
            working_directory: working_directory.into(),
            session_context_provider: NoSessionContextProvider,
        }
    }
}

impl<P> V1Parser<P> {
    /// 使用指定异步 provider 创建 V1 parser。
    #[must_use]
    pub fn new(working_directory: impl Into<String>, session_context_provider: P) -> Self {
        Self {
            working_directory: working_directory.into(),
            session_context_provider,
        }
    }
}

/// V1 work data 联合体中的公共字段。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct V1WorkData {
    /// `session` 或 `healthcheck`。
    #[serde(rename = "type", default)]
    pub data_type: String,
    /// session work 的 session ID；healthcheck 可为空。
    #[serde(default)]
    pub id: String,
}

/// orchestrator poll 返回的一条 V1 work。
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct V1WorkResponse {
    /// work ID。
    #[serde(default)]
    pub id: String,
    /// 顶层固定类型 `work`。
    #[serde(rename = "type", default)]
    pub work_type: String,
    /// 环境 ID。
    #[serde(default)]
    pub environment_id: String,
    /// work 状态。
    #[serde(default)]
    pub state: String,
    /// session/healthcheck 详情。
    #[serde(default)]
    pub data: V1WorkData,
    /// padded URL-safe Base64 work secret。
    #[serde(default)]
    pub secret: String,
    /// 后端提供的创建时间字符串。
    #[serde(default)]
    pub created_at: String,
}

impl fmt::Debug for V1WorkResponse {
    /// 输出 work 元数据并固定脱敏 secret。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("V1WorkResponse")
            .field("id", &self.id)
            .field("work_type", &self.work_type)
            .field("environment_id", &self.environment_id)
            .field("state", &self.state)
            .field("data", &self.data)
            .field("secret", &"[REDACTED]")
            .field("created_at", &self.created_at)
            .finish()
    }
}

impl V1WorkResponse {
    /// 返回 data 中的 session ID。
    #[must_use]
    pub fn get_session_id(&self) -> &str {
        &self.data.id
    }

    /// 判断该 work 是否为 healthcheck。
    #[must_use]
    pub fn is_healthcheck(&self) -> bool {
        self.data.data_type == "healthcheck"
    }
}

/// 解码后的 V1 work secret。
#[derive(Clone, Serialize)]
pub struct WorkSecret {
    /// secret schema 版本；当前只接受 1。
    pub version: i64,
    /// session ingress token。
    pub session_ingress_token: String,
    /// Environment API base URL。
    pub api_base_url: String,
    /// source 列表。
    pub sources: Vec<Source>,
    /// provider 认证配置。
    pub auth: Vec<AuthConfig>,
    /// 可选 sandbox gateway Claude 参数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claude_code_args: Option<HashMap<String, String>>,
    /// 可选预计算 MCP 文件。
    #[serde(rename = "mcp_config", skip_serializing_if = "Option::is_none")]
    pub mcp_config: Option<McpConfigFile>,
    /// 是否使用 sandbox gateway 配置。
    pub use_sandbox_gateway_config: bool,
    /// 可选子进程环境变量。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment_variables: Option<HashMap<String, String>>,
    /// 是否使用 code sessions v2。
    pub use_code_sessions: bool,
}

impl fmt::Debug for WorkSecret {
    /// 输出非敏感结构元数据，隐藏 ingress token 和 auth token。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkSecret")
            .field("version", &self.version)
            .field("session_ingress_token", &"[REDACTED]")
            .field("api_base_url", &self.api_base_url)
            .field("source_count", &self.sources.len())
            .field("auth_count", &self.auth.len())
            .field("has_claude_code_args", &self.claude_code_args.is_some())
            .field("has_mcp_config", &self.mcp_config.is_some())
            .field(
                "use_sandbox_gateway_config",
                &self.use_sandbox_gateway_config,
            )
            .field(
                "has_environment_variables",
                &self.environment_variables.is_some(),
            )
            .field("use_code_sessions", &self.use_code_sessions)
            .finish()
    }
}
