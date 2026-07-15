//! 认证子系统的数据类型。
//!
//! 文件名与恢复后的 Go `zz_recovered_types.go` 对应。敏感字段不会通过 `Debug`
//! 输出，避免错误链或诊断日志意外泄漏令牌。

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

use crate::internal::logger::Logger;

/// 一条来自 stdin 或 work secret 的认证配置。
#[derive(Clone, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthConfig {
    /// 认证提供方的稳定 wire 名称。
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub auth_type: String,
    /// 提供方令牌；部分提供方把一段 JSON 凭据编码在此字符串中。
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub token: String,
}

/// 把缺失或 JSON `null` 字符串字段还原为 Go struct 的空字符串零值。
///
/// `encoding/json` 把 `null` 解到已有 string 字段时不会报错；Serde 的 `String`
/// 默认会拒绝 null。认证配置和嵌套部署凭据共享该 helper，以保持错误分支顺序。
pub(crate) fn deserialize_string_or_null<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Option::unwrap_or_default)
}

impl fmt::Debug for AuthConfig {
    /// 只输出提供方类型，并将令牌固定脱敏。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthConfig")
            .field("auth_type", &self.auth_type)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

/// 解析后的会话认证上下文。
///
/// 该结构集中保存下游 API、部署工具和 MCP 所需的凭据。字段保持私有，调用方
/// 只能通过显式 getter 取得值，从而降低把整份上下文打印到日志中的风险。
#[derive(Clone, Default)]
pub struct AuthContext {
    pub(crate) session_ingress_token: String,
    pub(crate) anthropic_api_token: String,
    pub(crate) anthropic_oauth_token: String,
    pub(crate) vercel_deploy_token: String,
    pub(crate) antspace_control_plane_url: String,
    pub(crate) antspace_auth_token: String,
    pub(crate) supabase_project_ref: String,
    pub(crate) supabase_anon_key: String,
    pub(crate) supabase_db_pass: String,
    pub(crate) supabase_pat: String,
    pub(crate) session_id: String,
}

impl fmt::Debug for AuthContext {
    /// 输出可用于定位配置问题的元数据，但绝不输出任何凭据正文。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthContext")
            .field("session_id", &self.session_id)
            .field(
                "has_session_ingress_token",
                &!self.session_ingress_token.is_empty(),
            )
            .field(
                "has_anthropic_api_token",
                &!self.anthropic_api_token.is_empty(),
            )
            .field(
                "has_anthropic_oauth_token",
                &!self.anthropic_oauth_token.is_empty(),
            )
            .field(
                "has_vercel_deploy_token",
                &!self.vercel_deploy_token.is_empty(),
            )
            .field("has_antspace", &!self.antspace_control_plane_url.is_empty())
            .field("has_supabase", &!self.supabase_project_ref.is_empty())
            .finish()
    }
}

impl AuthContext {
    /// 返回 session ingress Bearer token。
    #[must_use]
    pub fn get_session_ingress_token(&self) -> &str {
        &self.session_ingress_token
    }

    /// 返回 Anthropic API token。
    #[must_use]
    pub fn get_anthropic_api_token(&self) -> &str {
        &self.anthropic_api_token
    }

    /// 返回 Anthropic OAuth token。
    ///
    /// Go 名称 `GetAnthropicOAuthToken` 中的 OAuth 缩写在 Rust 中按一个语义词
    /// 写作 `oauth`；两者读取的是同一私有凭据字段，不做格式转换。
    #[must_use]
    pub fn get_anthropic_oauth_token(&self) -> &str {
        &self.anthropic_oauth_token
    }

    /// 返回 Vercel 部署 token。
    #[must_use]
    pub fn get_vercel_deploy_token(&self) -> &str {
        &self.vercel_deploy_token
    }

    /// 返回当前会话 ID。
    #[must_use]
    pub fn get_session_id(&self) -> &str {
        &self.session_id
    }

    /// 更新当前会话 ID，供 V1 work 中的实际 session 覆盖启动参数。
    pub fn set_session_id(&mut self, session_id: impl Into<String>) {
        self.session_id = session_id.into();
    }

    /// 更新 session ingress token，供 work secret 覆盖 auth 数组中的旧值。
    pub fn set_session_ingress_token(&mut self, token: impl Into<String>) {
        self.session_ingress_token = token.into();
    }

    /// 返回 Supabase project reference。
    #[must_use]
    pub fn get_supabase_project_ref(&self) -> &str {
        &self.supabase_project_ref
    }

    /// 返回 Supabase anon key。
    #[must_use]
    pub fn get_supabase_anon_key(&self) -> &str {
        &self.supabase_anon_key
    }

    /// 返回 Supabase 数据库密码。
    #[must_use]
    pub fn get_supabase_db_pass(&self) -> &str {
        &self.supabase_db_pass
    }

    /// 返回 Supabase personal access token。
    #[must_use]
    pub fn get_supabase_pat(&self) -> &str {
        &self.supabase_pat
    }

    /// 返回 Antspace control plane URL。
    #[must_use]
    pub fn get_antspace_control_plane_url(&self) -> &str {
        &self.antspace_control_plane_url
    }

    /// 返回 Antspace 部署认证 token。
    #[must_use]
    pub fn get_antspace_auth_token(&self) -> &str {
        &self.antspace_auth_token
    }

    /// 判断是否配置了可识别的 Supabase 项目。
    ///
    /// 与 Go 行为一致，只检查 `project_ref`，不会要求其余三个字段同时非空。
    #[must_use]
    pub fn has_supabase(&self) -> bool {
        !self.supabase_project_ref.is_empty()
    }
}

/// GitHub URL 认证所使用的凭据类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GitHubAuthType {
    /// 普通 GitHub token，可作为 username 或 username/password 使用。
    Token,
    /// 由 git proxy 签发、以 `sk-ant-ccsr-` 开头的 JWT。
    GitProxyJwt,
}

/// GitHub source URL 的认证适配器。
#[derive(Clone)]
pub struct GitHubSourceAuthProvider {
    pub(crate) auth_type: GitHubAuthType,
    pub(crate) token: String,
    pub(crate) logger: Logger,
}

impl fmt::Debug for GitHubSourceAuthProvider {
    /// 输出认证类型和是否存在 token，避免泄露 token 内容。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GitHubSourceAuthProvider")
            .field("auth_type", &self.auth_type)
            .field("has_token", &!self.token.is_empty())
            .finish()
    }
}

/// 仓库访问权限标签。
///
/// 目前标签只用于日志与策略传递，URL 认证本身不会按权限改变 token。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Permission(pub String);

impl From<&str> for Permission {
    /// 从静态或借用字符串构造权限标签。
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// 源码仓库 URL 认证提供方的统一接口。
pub trait SourceAuthProvider {
    /// 尝试把认证信息写入 HTTPS URL；不支持或无凭据时返回原 URL 与 `false`。
    fn authenticate_url(
        &self,
        auth_context: &AuthContext,
        source_url: &str,
        permission: &Permission,
    ) -> (String, bool);

    /// 从外部命令输出中移除已认证 URL 和原始 token。
    fn sanitize_output(&self, output: &str, git_url: &str, authenticated_url: &str) -> String;
}
