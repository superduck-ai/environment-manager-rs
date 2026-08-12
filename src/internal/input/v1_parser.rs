//! V1 work response 到运行时上下文的异步 parser。
//!
//! 网络访问通过 `SessionContextProvider` 注入。code-session 与 healthcheck 不调用
//! provider；旧 session 工作流会异步获取 session context 后再组装启动配置。

use std::{pin::Pin, time::Instant};

use serde_json::{Value, json};
use thiserror::Error;

use super::secret::{SecretError, decode_work_secret, parse_v1_work_response};
use super::types::{
    EnvironmentConfigResponse, InputParser, Outcomes, ParsedContext, SessionContext,
    SessionContextProvider, SessionContextRequest, V1Parser, V1WorkResponse, WorkSecret,
};
use crate::internal::auth::{AuthContext, AuthError, new_auth_context_with_session_id};
use crate::internal::config::{ConfigError, McpConfig, OutcomeField, StartupContext};

/// V1 work parser 失败。
#[derive(Debug, Error)]
pub enum V1ParseError {
    /// V1 envelope 无效。
    #[error("failed to parse V1 work response: {0}")]
    WorkResponse(#[source] SecretError),
    /// work secret 无法解码或版本不支持。
    #[error("failed to decode secret: {0}")]
    Secret(#[source] SecretError),
    /// auth 数组无法编码为认证子系统输入。
    #[error("failed to build auth context: failed to marshal auth configs: {0}")]
    AuthEncoding(#[source] serde_json::Error),
    /// auth provider 配置无效。
    #[error("failed to build auth context: failed to create auth context: {0}")]
    Auth(#[source] AuthError),
    /// session API/provider 获取失败。
    #[error("failed to fetch session context: {0}")]
    SessionContextFetch(String),
    /// session context 的 outcomes JSON 无效。
    #[error("failed to build startup context: failed to parse outcomes from session: {0}")]
    Outcomes(#[source] serde_json::Error),
    /// session context 的 MCP JSON 无效。
    #[error("failed to build startup context: failed to decode mcp_config: {0}")]
    McpConfig(#[source] serde_json::Error),
    /// 合成的 startup context 未通过业务校验。
    #[error("failed to build startup context: invalid startup context: {0}")]
    StartupContext(#[source] ConfigError),
}

impl<P> V1Parser<P>
where
    P: SessionContextProvider,
{
    /// 解析 V1 work，按 secret 模式选择是否获取 session context。
    pub async fn parse(&self, data: &[u8]) -> Result<ParsedContext, V1ParseError> {
        self.parse_internal(data).await
    }

    /// 执行 V1 共享解析状态机。
    async fn parse_internal(&self, data: &[u8]) -> Result<ParsedContext, V1ParseError> {
        let started = Instant::now();
        tracing::debug!("data_size_bytes" = ?(data.len()), "{}", "Parsing V1 input format");
        let work_response = parse_v1_work_response(data).map_err(V1ParseError::WorkResponse)?;
        tracing::debug!("id" = ?(&work_response.id), "type" = ?(&work_response.work_type), "environment_id" = ?(&work_response.environment_id), "state" = ?(&work_response.state), "data_type" = ?(&work_response.data.data_type), "{}", "Parsed V1 work response");
        let secret = decode_work_secret(&work_response.secret).map_err(V1ParseError::Secret)?;
        let auth_context = self.build_auth_context(&secret, work_response.get_session_id())?;

        if work_response.is_healthcheck() {
            return Ok(self.build_healthcheck_result(
                &work_response,
                &secret,
                auth_context,
                started,
            ));
        }
        self.build_session_result(&work_response, &secret, auth_context, started)
            .await
    }

    /// 构建无需环境配置与 session ID 的 healthcheck 结果。
    fn build_healthcheck_result(
        &self,
        work_response: &V1WorkResponse,
        secret: &WorkSecret,
        auth_context: AuthContext,
        started: Instant,
    ) -> ParsedContext {
        tracing::info!("work_id" = ?(&work_response.id), "environment_id" = ?(&work_response.environment_id), "{}", "Processing healthcheck work type");
        tracing::debug!("parse_duration_ms" = ?(duration_milliseconds(started)), "{}", "Completed parsing V1 healthcheck input");
        ParsedContext {
            startup_context: StartupContext {
                api_base_url: normalize_api_base_url(&secret.api_base_url),
                ..StartupContext::default()
            },
            environment: None,
            auth_context,
            outcomes: Outcomes::new(),
            session_id: String::new(),
            work_id: work_response.id.clone(),
            environment_id: work_response.environment_id.clone(),
            is_healthcheck: true,
        }
    }

    /// 获取必要的 session context 并构建普通 session 结果。
    async fn build_session_result(
        &self,
        work_response: &V1WorkResponse,
        secret: &WorkSecret,
        auth_context: AuthContext,
        started: Instant,
    ) -> Result<ParsedContext, V1ParseError> {
        let session_id = work_response.get_session_id();
        tracing::debug!("version" = ?(secret.version), "api_base_url" = ?(&secret.api_base_url), "source_count" = ?(secret.sources.len()), "auth_count" = ?(secret.auth.len()), "has_claude_code_args" = ?(secret.claude_code_args.is_some()), "has_mcp_config" = ?(secret.mcp_config.is_some()), "has_environment_variables" = ?(secret.environment_variables.is_some()), "use_code_sessions" = ?(secret.use_code_sessions), "{}", "Decoded work secret");
        let session_context = if secret.use_code_sessions {
            SessionContext::default()
        } else {
            self.session_context_provider
                .get_session_context(SessionContextRequest {
                    api_base_url: &secret.api_base_url,
                    session_ingress_token: auth_context.get_session_ingress_token(),
                    session_id,
                })
                .await
                .map_err(V1ParseError::SessionContextFetch)?
        };

        let startup_context = self.build_startup_context(secret, &session_context)?;
        let environment = self.build_environment_response(&session_context);
        let outcomes = self.build_outcomes(&session_context);

        tracing::debug!("parse_duration_ms" = ?(duration_milliseconds(started)), "{}", "Completed parsing V1 input");

        Ok(ParsedContext {
            startup_context,
            environment: Some(environment),
            auth_context,
            outcomes,
            session_id: session_id.to_owned(),
            work_id: work_response.id.clone(),
            environment_id: work_response.environment_id.clone(),
            is_healthcheck: false,
        })
    }

    /// 将 secret 与 session context 合并成可校验的启动上下文。
    fn build_startup_context(
        &self,
        secret: &WorkSecret,
        session_context: &SessionContext,
    ) -> Result<StartupContext, V1ParseError> {
        let outcomes = match session_context.outcomes.as_ref() {
            None | Some(Value::Null) => Vec::new(),
            Some(value) => serde_json::from_value::<Vec<OutcomeField>>(value.clone())
                .map_err(V1ParseError::Outcomes)?,
        };
        let mcp_config = match session_context.mcp_config.as_ref() {
            None => None,
            Some(Value::Null) => Some(McpConfig::default()),
            Some(value) => Some(
                serde_json::from_value::<McpConfig>(value.clone())
                    .map_err(V1ParseError::McpConfig)?,
            ),
        };

        let startup_context = StartupContext {
            sources: secret.sources.clone(),
            api_base_url: normalize_api_base_url(&secret.api_base_url),
            outcomes,
            custom_system_prompt: session_context.custom_system_prompt.clone(),
            append_system_prompt: session_context.append_system_prompt.clone(),
            model: session_context.model.clone(),
            mcp_config,
            claude_code_args: secret.claude_code_args.clone().unwrap_or_default(),
            mcp_config_file: secret.mcp_config.clone(),
            use_sandbox_gateway_config: secret.use_sandbox_gateway_config,
            environment_variables: secret.environment_variables.clone().unwrap_or_default(),
            use_code_sessions: secret.use_code_sessions,
            ..StartupContext::default()
        };
        startup_context
            .validate()
            .map_err(V1ParseError::StartupContext)?;
        Ok(startup_context)
    }

    /// 根据 session cwd 或 parser 回退目录构建 BYOC 环境配置。
    fn build_environment_response(
        &self,
        session_context: &SessionContext,
    ) -> EnvironmentConfigResponse {
        let cwd = if session_context.cwd.is_empty() {
            &self.working_directory
        } else {
            &session_context.cwd
        };
        EnvironmentConfigResponse {
            environment_type: "byoc".to_owned(),
            config: json!({
                "environment_type": "byoc",
                "cwd": cwd,
            }),
            allowed_tools: String::new(),
            disallowed_tools: String::new(),
        }
    }

    /// 将 secret auth 数组解析为上下文，并用顶层 ingress token 覆盖旧值。
    fn build_auth_context(
        &self,
        secret: &WorkSecret,
        session_id: &str,
    ) -> Result<AuthContext, V1ParseError> {
        let auth_json = serde_json::to_vec(&secret.auth).map_err(V1ParseError::AuthEncoding)?;
        let mut auth_context =
            new_auth_context_with_session_id(&auth_json, session_id).map_err(V1ParseError::Auth)?;
        auth_context.set_session_ingress_token(secret.session_ingress_token.clone());
        Ok(auth_context)
    }

    /// 将有效单分支 git_repository outcomes 转成运行时映射。
    fn build_outcomes(&self, session_context: &SessionContext) -> Outcomes {
        let Some(value) = session_context.outcomes.as_ref() else {
            return Outcomes::new();
        };
        let fields = if value.is_null() {
            Vec::new()
        } else {
            match serde_json::from_value::<Vec<OutcomeField>>(value.clone()) {
                Ok(fields) => fields,
                Err(error) => {
                    tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to parse outcomes for git push info");
                    return Outcomes::new();
                }
            }
        };

        let mut outcomes = Outcomes::new();
        for outcome in fields {
            if outcome.outcome_type != "git_repository" || outcome.git_info.repo.is_empty() {
                continue;
            }
            if outcome.git_info.branches.len() != 1 {
                tracing::warn!("repo" = ?(&outcome.git_info.repo), "branch_count" = ?(outcome.git_info.branches.len()), "{}", "Outcome skipped: exactly one branch must be specified");
                continue;
            }
            outcomes.add(outcome.git_info.repo, &outcome.git_info.branches);
        }
        let validated = outcomes.validate();
        let outcome_count = value.as_array().map_or(0, Vec::len);
        tracing::info!("outcome_count" = ?(outcome_count), "repo_count" = ?(validated.len()), "{}", "Parsed outcomes from V1 session context");
        validated
    }
}

/// 将 parser 单调耗时转换为 Go `Duration.Milliseconds()` 的截断毫秒数。
fn duration_milliseconds(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}

impl<P> InputParser for V1Parser<P>
where
    P: SessionContextProvider,
{
    type Error = V1ParseError;

    /// 把 V1 parser 的原生异步流程装箱给统一 parser 接口。
    fn parse_input<'a>(
        &'a self,
        data: &'a [u8],
    ) -> Pin<Box<dyn std::future::Future<Output = Result<ParsedContext, Self::Error>> + Send + 'a>>
    {
        Box::pin(async move { self.parse(data).await })
    }
}

/// 为缺少 scheme 的 API base URL 补 `https://`。
///
/// 与 Go 实现一致，仅做字符串前缀判断，不在此处验证 URL、去除尾斜杠或改写大小写。
#[must_use]
pub fn normalize_api_base_url(api_base_url: &str) -> String {
    if api_base_url.starts_with("http://") || api_base_url.starts_with("https://") {
        api_base_url.to_owned()
    } else {
        format!("https://{api_base_url}")
    }
}
