//! 将 wire auth 数组解析为运行时认证上下文。
//!
//! 解析规则保持 Go 实现的“按数组顺序覆盖”语义：同一提供方出现多次时，后一个
//! 值胜出；未知提供方和空 token 会立即终止解析。

use serde::Deserialize;
use serde_json::json;
use thiserror::Error;

use crate::internal::logger::Logger;

use super::types::{AuthConfig, AuthContext, deserialize_string_or_null};

/// 认证配置解析失败。
#[derive(Debug, Error)]
pub enum AuthError {
    /// 顶层 auth 值不是合法的 JSON 数组。
    #[error("failed to parse auth configuration: {0}")]
    InvalidConfiguration(#[source] serde_json::Error),
    /// 已知提供方缺少必填 token。
    #[error("{0} auth configuration missing required 'token' field")]
    MissingToken(&'static str),
    /// Antspace token 中的嵌套 JSON 无法解析。
    #[error("failed to parse antspace deploy credentials: {0}")]
    InvalidAntspaceCredentials(#[source] serde_json::Error),
    /// Supabase token 中的嵌套 JSON 无法解析。
    #[error("failed to parse supabase credentials: {0}")]
    InvalidSupabaseCredentials(#[source] serde_json::Error),
    /// wire 数据包含当前版本不认识的认证提供方。
    #[error("unknown auth provider type: {0}")]
    UnknownProvider(String),
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct AntspaceCredentials {
    #[serde(deserialize_with = "deserialize_string_or_null")]
    url: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    auth_token: String,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct SupabaseCredentials {
    #[serde(deserialize_with = "deserialize_string_or_null")]
    project_ref: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    anon_key: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    db_pass: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pat: String,
}

/// 解析 auth JSON，并附加调用方已知的会话 ID。
///
/// 空切片表示没有认证配置并返回一个只含 session ID 的上下文。函数刻意不检查
/// Antspace/Supabase 嵌套对象中的单个字段是否为空，以保持恢复后的 Go 行为。
pub fn new_auth_context_with_session_id(
    auth_json: &[u8],
    session_id: impl Into<String>,
) -> Result<AuthContext, AuthError> {
    parse_auth_context(auth_json, session_id.into(), None)
}

/// 使用生产 logger 解析 auth JSON，并复刻 Go 每个 provider 配置完成后的日志。
///
/// 对应 Go：`auth.NewAuthContextWithSessionID`。日志只包含 provider 类型以及
/// Antspace URL、Supabase project ref 这两个 Go 原实现会记录的非 secret 字段；
/// token、密码和匿名 key 永远不会进入日志。某一项校验失败时，该项不会产生
/// “Configured” 日志，之前已经成功的项则不会回滚。
pub fn new_auth_context_with_session_id_and_logger(
    auth_json: &[u8],
    session_id: impl Into<String>,
    logger: &Logger,
) -> Result<AuthContext, AuthError> {
    parse_auth_context(auth_json, session_id.into(), Some(logger))
}

/// 执行共享 auth 解析流程，并按入口决定是否发出生产日志。
fn parse_auth_context(
    auth_json: &[u8],
    session_id: String,
    logger: Option<&Logger>,
) -> Result<AuthContext, AuthError> {
    let mut context = AuthContext {
        session_id,
        ..AuthContext::default()
    };

    if auth_json.is_empty() {
        return Ok(context);
    }

    // Go 把顶层 `null` 解为 nil slice，也允许数组元素 `null` 先落成 AuthConfig
    // 零值；后者随后按既有顺序触发 unknown provider，而不是 JSON 类型错误。
    let configurations: Option<Vec<Option<AuthConfig>>> =
        serde_json::from_slice(auth_json).map_err(AuthError::InvalidConfiguration)?;

    for configuration in configurations.unwrap_or_default() {
        let configuration = configuration.unwrap_or_default();
        // provider 日志需要在 token 被移动进上下文后继续引用类型名，因此先复制
        // 这个非敏感小字符串，避免让 match 借用扩大到整个分支。
        let auth_type = configuration.auth_type.clone();
        match auth_type.as_str() {
            "session_ingress" => {
                require_token("session_ingress", &configuration.token)?;
                context.session_ingress_token = configuration.token;
                log_configured(logger, "Configured session ingress token", &auth_type, None);
            }
            "anthropic_api" => {
                require_token("anthropic_api", &configuration.token)?;
                context.anthropic_api_token = configuration.token;
                log_configured(logger, "Configured Anthropic API token", &auth_type, None);
            }
            "anthropic_oauth" => {
                require_token("anthropic_oauth", &configuration.token)?;
                context.anthropic_oauth_token = configuration.token;
                log_configured(logger, "Configured Anthropic OAuth token", &auth_type, None);
            }
            "vercel_deploy" => {
                require_token("vercel_deploy", &configuration.token)?;
                context.vercel_deploy_token = configuration.token;
                log_configured(logger, "Configured Vercel deploy token", &auth_type, None);
            }
            "antspace_deploy" => {
                require_token("antspace_deploy", &configuration.token)?;
                let credentials: Option<AntspaceCredentials> =
                    serde_json::from_str(&configuration.token)
                        .map_err(AuthError::InvalidAntspaceCredentials)?;
                let credentials = credentials.unwrap_or_default();
                context.antspace_control_plane_url = credentials.url;
                context.antspace_auth_token = credentials.auth_token;
                log_configured(
                    logger,
                    "Configured Antspace deploy credentials",
                    &auth_type,
                    Some(("url", json!(&context.antspace_control_plane_url))),
                );
            }
            "supabase" => {
                require_token("supabase", &configuration.token)?;
                let credentials: Option<SupabaseCredentials> =
                    serde_json::from_str(&configuration.token)
                        .map_err(AuthError::InvalidSupabaseCredentials)?;
                let credentials = credentials.unwrap_or_default();
                context.supabase_project_ref = credentials.project_ref;
                context.supabase_anon_key = credentials.anon_key;
                context.supabase_db_pass = credentials.db_pass;
                context.supabase_pat = credentials.pat;
                log_configured(
                    logger,
                    "Configured Supabase credentials",
                    &auth_type,
                    Some(("project_ref", json!(&context.supabase_project_ref))),
                );
            }
            unknown => return Err(AuthError::UnknownProvider(unknown.to_owned())),
        }
    }

    Ok(context)
}

/// 写入一条 Go auth provider 配置完成日志，并保持属性插入顺序。
fn log_configured(
    logger: Option<&Logger>,
    message: &str,
    auth_type: &str,
    extra: Option<(&str, serde_json::Value)>,
) {
    let Some(logger) = logger else {
        return;
    };
    let mut attributes = serde_json::Map::new();
    attributes.insert("auth_type".to_owned(), json!(auth_type));
    if let Some((key, value)) = extra {
        attributes.insert(key.to_owned(), value);
    }
    logger.info(message, attributes);
}

/// 验证已知提供方的 token 非空，并生成与 Go 兼容的错误文本。
fn require_token(provider: &'static str, token: &str) -> Result<(), AuthError> {
    if token.is_empty() {
        return Err(AuthError::MissingToken(provider));
    }
    Ok(())
}
