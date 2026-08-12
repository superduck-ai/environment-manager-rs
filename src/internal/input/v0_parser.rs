//! 旧版 V0 stdin JSON parser。
//!
//! parser 保留环境配置中的未知字段，并在成功后统一补全 API URL scheme。为保持
//! 当前 Go 工作树的库内行为，缺失 `startup_context` 的普通入口仍触发同文本 panic；
//! CLI 专用入口则把它转换成类型化错误。显式 JSON `null` 仍先构造零值 context，
//! 再走正常的 API base URL 校验分支。

use std::{pin::Pin, time::Instant};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use thiserror::Error;

use super::types::{EnvironmentConfigResponse, InputParser, Outcomes, ParsedContext, V0Parser};
use super::v1_parser::normalize_api_base_url;
use crate::internal::auth::{AuthError, new_auth_context_with_session_id};
use crate::internal::config::{ConfigError, StartupContext};

/// V0 stdin 解析失败。
#[derive(Debug, Error)]
pub enum V0ParseError {
    /// stdin 不是合法 V0 JSON object。
    #[error("failed to parse V0 stdin JSON: {0}")]
    InvalidStdin(#[source] serde_json::Error),
    /// 缺少 startup_context；CLI 专用入口返回该值，普通 parser 入口仍会 panic。
    #[error("failed to parse startup_context: startup context is required")]
    StartupContextRequired,
    /// startup_context JSON 形状无效。
    #[error("failed to parse startup_context: failed to unmarshal startup_context: {0}")]
    InvalidStartupContextJson(#[source] serde_json::Error),
    /// startup_context 未通过领域校验。
    #[error("failed to parse startup_context: invalid startup_context: {0}")]
    InvalidStartupContext(#[source] ConfigError),
    /// environment 字段缺失。
    #[error("failed to parse environment: environment configuration is required")]
    EnvironmentRequired,
    /// environment JSON 不是预期对象。
    #[error("failed to parse environment: failed to unmarshal environment: {0}")]
    InvalidEnvironmentJson(#[source] serde_json::Error),
    /// environment_type 为空。
    #[error(
        "failed to parse environment: environment configuration missing required 'environment_type' field"
    )]
    EnvironmentTypeRequired,
    /// anthropic 环境缺少 cwd。
    #[error("failed to parse environment: cwd field is required in environment configuration")]
    AnthropicCwdRequired,
    /// auth Value 无法重新编码成 JSON。
    #[error("failed to parse auth: failed to encode auth configuration: {0}")]
    AuthEncoding(#[source] serde_json::Error),
    /// auth provider 配置无效。
    #[error("failed to parse auth: failed to create auth context from JSON: {0}")]
    InvalidAuth(#[source] AuthError),
}

/// 缺失 `startup_context` 时共享解析流程采用的行为。
///
/// Go parser 本身返回 nil context 后由调用方解引用并 panic。Rust 的库内 parser 保留
/// 这一语义，CLI 则返回可诊断的普通错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissingStartupContextBehavior {
    /// 复刻 parser 被直接调用时发生的 panic，供单元测试和差分 probe 使用。
    PanicLikeGoParser,
    /// 返回专用错误，供 CLI 正常报告无效输入。
    ReturnErrorForCli,
}

#[derive(Default, Deserialize)]
struct V0EnvironmentConfig {
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    environment_type: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    cwd: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    allowed_tools: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    disallowed_tools: String,
}

/// 将环境配置中的显式 JSON `null` 解为 Go string 字段的空字符串零值。
///
/// Go `encoding/json` 对 string 字段收到 `null` 时不报错且保留零值；Serde 默认拒绝
/// 该输入。这里仅放宽 V0 environment 的四个固定字符串，其他类型错误仍按原顺序返回。
fn deserialize_string_or_null<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Option::unwrap_or_default)
}

impl V0Parser {
    /// 解析完整 V0 stdin payload。
    pub fn parse(&self, data: &[u8]) -> Result<ParsedContext, V0ParseError> {
        self.parse_internal(data, MissingStartupContextBehavior::PanicLikeGoParser)
    }

    /// 解析 CLI 收到的完整 V0 stdin payload。
    ///
    /// 除缺失 `startup_context` 外，本方法与 [`Self::parse`] 的阶段、日志和错误完全
    /// 一致；该唯一分支返回 [`V0ParseError::StartupContextRequired`]。
    ///
    /// # 参数
    /// - `data`：已经从 stdin 读到 EOF 的原始 JSON 字节，仅在调用期间借用。
    ///
    /// # 返回
    /// 成功时返回完整解析上下文；输入缺字段或其他阶段失败时返回类型化解析错误。
    pub(crate) fn parse_for_cli(&self, data: &[u8]) -> Result<ParsedContext, V0ParseError> {
        self.parse_internal(data, MissingStartupContextBehavior::ReturnErrorForCli)
    }

    /// 执行 V0 共享解析流程，并按调用入口决定日志及缺字段行为。
    ///
    /// `missing_startup_context` 只改变 Go nil 解引用的承载方式，不能改变它之前的
    /// JSON 解码和日志，也不能让它之后的 environment/auth 阶段继续执行。
    fn parse_internal(
        &self,
        data: &[u8],
        missing_startup_context: MissingStartupContextBehavior,
    ) -> Result<ParsedContext, V0ParseError> {
        let started = Instant::now();
        tracing::info!("data_size_bytes" = ?(data.len()), "{}", "Parsing V0 input format");
        let input: serde_json::Map<String, Value> =
            serde_json::from_slice(data).map_err(V0ParseError::InvalidStdin)?;
        let startup_context =
            self.parse_startup_context(input.get("startup_context"), missing_startup_context)?;
        self.log_startup_context(&startup_context);
        let environment = self.parse_environment(input.get("environment"))?;
        tracing::info!("environment_type" = ?(&environment.environment_type), "has_allowed_tools" = ?(!environment.allowed_tools.is_empty()), "has_disallowed_tools" = ?(!environment.disallowed_tools.is_empty()), "{}", "Parsed environment from V0 input");
        let auth_context = self.parse_auth(input.get("auth"))?;
        tracing::info!("has_auth_data" = ?(input.contains_key("auth")), "{}", "Parsed auth from V0 input");
        let outcomes = self.parse_outcomes(&startup_context);

        tracing::info!("parse_duration_ms" = ?(duration_milliseconds(started)), "{}", "Completed parsing V0 input");

        Ok(ParsedContext {
            startup_context,
            environment: Some(environment),
            auth_context,
            outcomes,
            session_id: self.session_id.clone(),
            work_id: String::new(),
            environment_id: String::new(),
            is_healthcheck: false,
        })
    }

    /// 记录 V0 startup context 摘要以及可选的预计算 sandbox gateway 配置。
    fn log_startup_context(&self, startup_context: &StartupContext) {
        tracing::info!("has_api_base_url" = ?(!startup_context.api_base_url.is_empty()), "api_base_url" = ?(&startup_context.api_base_url), "sources_count" = ?(startup_context.sources.len()), "allowed_tools_count" = ?(startup_context.allowed_tools.len()), "disallowed_tools_count" = ?(startup_context.disallowed_tools.len()), "{}", "Parsed startup context from V0 input");

        if startup_context.claude_code_args.is_empty() && startup_context.mcp_config_file.is_none()
        {
            return;
        }
        tracing::info!("claude_code_args_count" = ?(startup_context.claude_code_args.len()), "has_mcp_config_file" = ?(startup_context.mcp_config_file.is_some()), "{}", "Received pre-computed args from sandbox-gateway");
        if let Some(config_file) = &startup_context.mcp_config_file
            && let Ok(content) = STANDARD.decode(&config_file.content)
        {
            tracing::debug!("path" = ?(&config_file.path), "content" = ?(String::from_utf8_lossy(&content)), "{}", "Pre-computed MCP config file");
        }
    }

    /// 反序列化、校验并归一化 startup_context。
    ///
    /// `missing_behavior` 仅用于区分库内 panic 与 CLI 普通错误；显式 `null`、非法
    /// JSON 形状和领域校验错误都走原有路径。
    fn parse_startup_context(
        &self,
        value: Option<&Value>,
        missing_behavior: MissingStartupContextBehavior,
    ) -> Result<StartupContext, V0ParseError> {
        // Go 的 parseStartupContext 对 missing RawMessage 返回 nil、nil，调用方随后立即
        // 解引用并产生固定 runtime panic。显式 null 则会构造零值 context，不能与
        // missing 合并。
        let Some(value) = value else {
            return match missing_behavior {
                MissingStartupContextBehavior::PanicLikeGoParser => {
                    panic!("runtime error: invalid memory address or nil pointer dereference");
                }
                MissingStartupContextBehavior::ReturnErrorForCli => {
                    Err(V0ParseError::StartupContextRequired)
                }
            };
        };
        let mut startup_context = if value.is_null() {
            StartupContext::default()
        } else {
            serde_json::from_value(value.clone())
                .map_err(V0ParseError::InvalidStartupContextJson)?
        };
        startup_context
            .validate()
            .map_err(V0ParseError::InvalidStartupContext)?;
        startup_context.api_base_url = normalize_api_base_url(&startup_context.api_base_url);
        Ok(startup_context)
    }

    /// 解析环境摘要并保留完整原始配置 object。
    pub(crate) fn parse_environment(
        &self,
        value: Option<&Value>,
    ) -> Result<EnvironmentConfigResponse, V0ParseError> {
        let value = value.ok_or(V0ParseError::EnvironmentRequired)?;
        let environment: V0EnvironmentConfig = if value.is_null() {
            V0EnvironmentConfig::default()
        } else {
            serde_json::from_value(value.clone()).map_err(V0ParseError::InvalidEnvironmentJson)?
        };
        if environment.environment_type.is_empty() {
            return Err(V0ParseError::EnvironmentTypeRequired);
        }
        if environment.environment_type == "anthropic" && environment.cwd.is_empty() {
            return Err(V0ParseError::AnthropicCwdRequired);
        }
        Ok(EnvironmentConfigResponse {
            environment_type: environment.environment_type,
            config: value.clone(),
            allowed_tools: environment.allowed_tools,
            disallowed_tools: environment.disallowed_tools,
        })
    }

    /// 将可选 auth Value 交给认证子系统解析。
    fn parse_auth(
        &self,
        value: Option<&Value>,
    ) -> Result<crate::internal::auth::AuthContext, V0ParseError> {
        let encoded = match value {
            None => Vec::new(),
            Some(value) => serde_json::to_vec(value).map_err(V0ParseError::AuthEncoding)?,
        };
        new_auth_context_with_session_id(&encoded, self.session_id.clone())
            .map_err(V0ParseError::InvalidAuth)
    }

    /// 把有效的单分支 git_repository outcomes 转成运行时映射。
    fn parse_outcomes(&self, startup_context: &StartupContext) -> Outcomes {
        let mut outcomes = Outcomes::new();
        if startup_context.outcomes.is_empty() {
            return outcomes;
        }
        for outcome in &startup_context.outcomes {
            if outcome.outcome_type != "git_repository" || outcome.git_info.repo.is_empty() {
                continue;
            }
            if outcome.git_info.branches.len() != 1 {
                tracing::warn!("repo" = ?(&outcome.git_info.repo), "branch_count" = ?(outcome.git_info.branches.len()), "{}", "Outcome skipped: exactly one branch must be specified");
                continue;
            }
            outcomes.add(&outcome.git_info.repo, &outcome.git_info.branches);
        }
        let validated = outcomes.validate();
        tracing::info!("outcome_count" = ?(startup_context.outcomes.len()), "repo_count" = ?(validated.len()), "{}", "Parsed outcomes from V0 input");
        for outcome in &startup_context.outcomes {
            tracing::debug!("type" = ?(&outcome.outcome_type), "repo" = ?(&outcome.git_info.repo), "branches" = ?(&outcome.git_info.branches), "{}", "Repository outcome");
        }
        validated
    }
}

/// 将单调时钟耗时转换为 Go `Duration.Milliseconds()` 的截断毫秒值。
fn duration_milliseconds(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}

impl InputParser for V0Parser {
    type Error = V0ParseError;

    /// 将同步 V0 解析包装成立即完成的 future，供统一异步调度层使用。
    fn parse_input<'a>(
        &'a self,
        data: &'a [u8],
    ) -> Pin<Box<dyn std::future::Future<Output = Result<ParsedContext, Self::Error>> + Send + 'a>>
    {
        Box::pin(async move { self.parse(data) })
    }
}
