//! GitHub HTTPS URL 的凭据注入与命令输出脱敏。
//!
//! SSH、非 HTTPS URL 和无 token 场景保持原 URL 不变。URL 的解析与转义交给
//! `url` crate，避免手工拼接凭据导致非法 URL 或注入问题。

use url::{Position, Url};

use super::types::{
    AuthContext, GitHubAuthType, GitHubSourceAuthProvider, Permission, SourceAuthProvider,
};

const GIT_PROXY_JWT_PREFIX: &str = "sk-ant-ccsr-";

/// 根据 token 前缀创建 GitHub 源码认证提供方。
///
/// `sk-ant-ccsr-` 前缀被视为 git proxy JWT，其余 token 使用普通 GitHub token
/// 语义。空 token 仍会创建 provider，但认证尝试会返回 `false`。
#[must_use]
pub fn new_github_source_auth_provider(token: impl Into<String>) -> GitHubSourceAuthProvider {
    GitHubSourceAuthProvider::new(token)
}

impl GitHubSourceAuthProvider {
    /// 根据 token 前缀构造认证提供方。
    ///
    /// 这是 Go `NewGitHubSourceAuthProvider` 的现代关联构造器；普通 token 与 git
    /// proxy JWT 的判定仍只依赖固定前缀，空 token 也保留为可构造状态。
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        let token = token.into();
        let auth_type = if token.starts_with(GIT_PROXY_JWT_PREFIX) {
            tracing::info!(
                "{}",
                "GitHub source auth provider initialized with git proxy JWT"
            );
            GitHubAuthType::GitProxyJwt
        } else {
            GitHubAuthType::Token
        };
        Self { auth_type, token }
    }

    /// 返回构造时识别出的认证类型，主要供策略选择和兼容性测试使用。
    #[must_use]
    pub fn auth_type(&self) -> GitHubAuthType {
        self.auth_type
    }
}

impl SourceAuthProvider for GitHubSourceAuthProvider {
    /// 给 HTTPS URL 注入 token，并返回是否成功应用了认证。
    ///
    /// 普通 token 若含第一个冒号，则冒号前后分别作为 username/password；git
    /// proxy JWT 固定使用 `unused` 作为 username。`auth_context` 和 `permission`
    /// 当前不参与凭据选择，但保留在接口中以兼容上层调用模型。
    fn authenticate_url(
        &self,
        _auth_context: &AuthContext,
        source_url: &str,
        permission: &Permission,
    ) -> (String, bool) {
        if source_url.starts_with("git@") || source_url.starts_with("ssh://") {
            tracing::debug!("url" = ?(source_url), "{}", "SSH URL detected, cannot use token authentication");
            return (source_url.to_owned(), false);
        }

        let mut parsed_url = match Url::parse(source_url) {
            Ok(parsed_url) => parsed_url,
            Err(error) => {
                tracing::warn!("url" = ?(source_url), "error" = ?(error.to_string()), "{}", "Failed to parse GitHub URL");
                return (source_url.to_owned(), false);
            }
        };
        if parsed_url.scheme() != "https" {
            return (source_url.to_owned(), false);
        }

        let credential_result = match self.auth_type {
            GitHubAuthType::Token => {
                if self.token.is_empty() {
                    return (source_url.to_owned(), false);
                }
                if let Some((username, password)) = self.token.split_once(':') {
                    parsed_url
                        .set_username(username)
                        .and_then(|()| parsed_url.set_password(Some(password)))
                } else {
                    parsed_url
                        .set_username(&self.token)
                        .and_then(|()| parsed_url.set_password(None))
                }
            }
            GitHubAuthType::GitProxyJwt => {
                if self.token.is_empty() {
                    tracing::warn!("No git proxy JWT available");
                    return (source_url.to_owned(), false);
                }
                tracing::info!("url" = ?(source_url), "{}", "Using git proxy authentication");
                if parsed_url.set_username("unused").is_err()
                    || parsed_url.set_password(Some(&self.token)).is_err()
                {
                    return (source_url.to_owned(), false);
                }
                Ok(())
            }
        };

        if credential_result.is_err() {
            return (source_url.to_owned(), false);
        }
        let authenticated_url = format_authenticated_url(parsed_url, source_url);
        tracing::debug!("url" = ?(source_url), "auth_type" = ?(match self.auth_type {
                        GitHubAuthType::Token => 0,
                        GitHubAuthType::GitProxyJwt => 1,
                    }), "permission" = ?(&permission.0), "success" = ?(true), "{}", "GitHub authentication applied");
        (authenticated_url, true)
    }

    /// 将认证 URL 和 token 从 git 子进程输出中移除。
    ///
    /// 先恢复公开 URL，再替换任何残余 token；这样既保留可诊断的仓库地址，也
    /// 避免错误信息中通过 URL 编码或 proxy username 泄密。
    fn sanitize_output(&self, output: &str, git_url: &str, authenticated_url: &str) -> String {
        let mut sanitized = output.to_owned();
        if !authenticated_url.is_empty() && authenticated_url != git_url {
            sanitized = sanitized.replace(authenticated_url, git_url);
        }
        if !self.token.is_empty() {
            sanitized = sanitized.replace(&self.token, "***");
            sanitized = sanitized.replace(&format!("unused:{}", self.token), "unused:***");
        }
        sanitized
    }
}

/// 输出注入凭据后的 URL，同时撤销 WHATWG parser 自动补出的根路径 `/`。
///
/// Go `net/url.URL.String` 会区分 `https://host` 与 `https://host/`；`url::Url` 内部
/// 则把前者规范化为后者。Git 会在错误输出中回显该 URL，额外 `/` 会同时改变可见
/// 文本和 `SanitizeOutput` 的精确替换，因此只有原 URL 明确带 path 时才保留根斜杠。
fn format_authenticated_url(parsed_url: Url, source_url: &str) -> String {
    let explicit_path = source_url
        .split_once("://")
        .map(|(_, remainder)| {
            remainder
                .find(['/', '?', '#'])
                .is_some_and(|index| remainder.as_bytes()[index] == b'/')
        })
        .unwrap_or(false);
    let path_start = parsed_url[..Position::BeforePath].len();
    let implicit_root = !explicit_path && parsed_url.path() == "/";
    let mut formatted = parsed_url.to_string();
    if implicit_root && formatted.as_bytes().get(path_start) == Some(&b'/') {
        formatted.remove(path_start);
    }
    formatted
}
