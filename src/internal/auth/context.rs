//! 将 wire auth 数组解析为运行时认证上下文。
//!
//! 解析规则保持 的“按数组顺序覆盖”语义：同一提供方出现多次时，后一个
//! 值胜出；未知提供方和空 token 会立即终止解析。

use serde::Deserialize;
use thiserror::Error;

use super::types::{AuthConfig, AuthContext};

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
    url: String,
    auth_token: String,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct SupabaseCredentials {
    project_ref: String,
    anon_key: String,
    db_pass: String,
    pat: String,
}

/// 解析 auth JSON，并附加调用方已知的会话 ID。
///
/// 空切片表示没有认证配置并返回一个只含 session ID 的上下文。函数刻意不检查
/// Antspace/Supabase 嵌套对象中的单个字段是否为空，以保持恢复后的。
pub fn new_auth_context_with_session_id(
    auth_json: &[u8],
    session_id: impl Into<String>,
) -> Result<AuthContext, AuthError> {
    parse_auth_context(auth_json, session_id.into())
}

/// 执行共享 auth 解析流程，并按入口决定是否发出生产日志。
fn parse_auth_context(auth_json: &[u8], session_id: String) -> Result<AuthContext, AuthError> {
    let mut context = AuthContext {
        session_id,
        ..AuthContext::default()
    };

    if auth_json.is_empty() {
        return Ok(context);
    }

    let configurations: Vec<AuthConfig> =
        serde_json::from_slice(auth_json).map_err(AuthError::InvalidConfiguration)?;

    for configuration in configurations {
        // provider 日志需要在 token 被移动进上下文后继续引用类型名，因此先复制
        // 这个非敏感小字符串，避免让 match 借用扩大到整个分支。
        let auth_type = configuration.auth_type.clone();
        match auth_type.as_str() {
            "session_ingress" => {
                require_token("session_ingress", &configuration.token)?;
                context.session_ingress_token = configuration.token;
                tracing::info!(auth_type, "Configured session ingress token");
            }
            "anthropic_api" => {
                require_token("anthropic_api", &configuration.token)?;
                context.anthropic_api_token = configuration.token;
                tracing::info!(auth_type, "Configured Anthropic API token");
            }
            "anthropic_oauth" => {
                require_token("anthropic_oauth", &configuration.token)?;
                context.anthropic_oauth_token = configuration.token;
                tracing::info!(auth_type, "Configured Anthropic OAuth token");
            }
            "vercel_deploy" => {
                require_token("vercel_deploy", &configuration.token)?;
                context.vercel_deploy_token = configuration.token;
                tracing::info!(auth_type, "Configured Vercel deploy token");
            }
            "antspace_deploy" => {
                require_token("antspace_deploy", &configuration.token)?;
                let credentials: AntspaceCredentials =
                    serde_json::from_str(&configuration.token)
                        .map_err(AuthError::InvalidAntspaceCredentials)?;
                context.antspace_control_plane_url = credentials.url;
                context.antspace_auth_token = credentials.auth_token;
                tracing::info!(
                    auth_type,
                    url = %context.antspace_control_plane_url,
                    "Configured Antspace deploy credentials"
                );
            }
            "supabase" => {
                require_token("supabase", &configuration.token)?;
                let credentials: SupabaseCredentials =
                    serde_json::from_str(&configuration.token)
                        .map_err(AuthError::InvalidSupabaseCredentials)?;
                context.supabase_project_ref = credentials.project_ref;
                context.supabase_anon_key = credentials.anon_key;
                context.supabase_db_pass = credentials.db_pass;
                context.supabase_pat = credentials.pat;
                tracing::info!(
                    auth_type,
                    project_ref = %context.supabase_project_ref,
                    "Configured Supabase credentials"
                );
            }
            unknown => return Err(AuthError::UnknownProvider(unknown.to_owned())),
        }
    }

    Ok(context)
}

/// 验证已知提供方的 token 非空，并生成稳定错误文本。
fn require_token(provider: &'static str, token: &str) -> Result<(), AuthError> {
    if token.is_empty() {
        return Err(AuthError::MissingToken(provider));
    }
    Ok(())
}
