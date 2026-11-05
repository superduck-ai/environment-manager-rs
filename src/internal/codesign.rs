//! Git commit 签名命令与 Claude 子进程之间共享的非敏感上下文。
//!
//! ingress token 继续通过专用的 0600 文件传递，不进入本结构。source 在写入环境变量
//! 前缩减为签名 API 接受的仓库标识字段，避免传播 clone URL 或 provider 凭据。

use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::config::{Source, StartupContext};

/// Claude 子进程携带的 CodeSign 上下文环境变量。
pub const CONTEXT_ENV: &str = "ENVIRONMENT_MANAGER_CODESIGN_CONTEXT";

/// 直接请求签名接口所需的非敏感会话上下文。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct CodeSignContext {
    /// 远端 session ID。
    pub session_id: String,
    /// 签名 API 根 URL。
    pub signing_server_url: String,
    /// 是否使用 `/v1/code/sessions` 端点。
    pub use_code_sessions: bool,
    /// 仓库目录与脱敏 source 的映射。
    pub sources: Vec<CodeSignSource>,
}

/// 一份仓库的本地目录和签名 API source。
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CodeSignSource {
    /// source clone 后的绝对目录。
    pub directory: String,
    /// 只含 type、git provider、repo 和 ref 的 source。
    pub source: Value,
}

impl CodeSignContext {
    /// 从执行器已验证的 session/startup 配置构造子进程上下文。
    #[must_use]
    pub fn from_runtime(
        session_id: impl Into<String>,
        startup_context: &StartupContext,
        working_directory: &Path,
    ) -> Self {
        let configured_url = std::env::var("SIGNING_SERVER_URL")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| startup_context.api_base_url.clone());
        let signing_server_url = normalize_server_url(&configured_url);
        let sources = startup_context
            .sources
            .iter()
            .map(|source| CodeSignSource {
                directory: canonical_or_absolute(&source.get_directory(working_directory))
                    .to_string_lossy()
                    .into_owned(),
                source: sanitize_source(source),
            })
            .collect();
        Self {
            session_id: session_id.into(),
            signing_server_url,
            use_code_sessions: startup_context.use_code_sessions,
            sources,
        }
    }

    /// 返回 repo 根目录或其子目录对应的脱敏 source。
    #[must_use]
    pub fn source_for_repository(&self, repository: &Path) -> Option<Value> {
        let repository = canonical_or_absolute(repository);
        self.sources.iter().find_map(|candidate| {
            let source_directory = canonical_or_absolute(Path::new(&candidate.directory));
            (repository == source_directory || repository.starts_with(&source_directory))
                .then(|| candidate.source.clone())
        })
    }

    /// 生成当前模式对应的远端签名 URL。
    pub fn endpoint(&self) -> Result<String, String> {
        if self.session_id.is_empty() {
            return Err("session ID is required for code signing API".to_owned());
        }
        if self.signing_server_url.is_empty() {
            return Err("signing server URL is required for code signing API".to_owned());
        }
        if self.use_code_sessions {
            Ok(format!(
                "{}/v1/code/sessions/{}/sign-commit",
                self.signing_server_url, self.session_id
            ))
        } else {
            Ok(format!(
                "{}/v1/session_ingress/sources/sign-commit/{}",
                self.signing_server_url, self.session_id
            ))
        }
    }
}

/// 把 source 缩减为 signing API 允许的最小字段集合。
#[must_use]
pub fn sanitize_source(source: &Source) -> Value {
    match source {
        Source::GitRepository(repository) => {
            let mut git_info = Map::new();
            git_info.insert("type".to_owned(), json!(repository.git_info.git_type));
            git_info.insert("repo".to_owned(), json!(repository.git_info.repo));
            if let Some(git_ref) = &repository.git_info.git_ref {
                git_info.insert("ref".to_owned(), json!(git_ref));
            }
            json!({
                "type": repository.base.source_type,
                "git_info": git_info,
            })
        }
    }
}

/// 保持已有签名 URL 规则：显式 scheme 原样保留，否则补本地兼容的 `http://`。
fn normalize_server_url(value: &str) -> String {
    if value.is_empty() {
        return String::new();
    }
    let value = if value.starts_with("http://") || value.starts_with("https://") {
        value.to_owned()
    } else {
        format!("http://{value}")
    };
    value.trim_end_matches('/').to_owned()
}

/// symlink 解析失败时退回绝对、去除 `.`/可消解 `..` 的 lexical path。
fn canonical_or_absolute(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let mut clean = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                clean.pop();
            }
            other => clean.push(other.as_os_str()),
        }
    }
    clean
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;
    use tempfile::tempdir;

    use crate::internal::{
        auth::AuthConfig,
        config::{BaseSource, GitInfo, GitRepositorySource, Source, StartupContext},
    };

    use super::CodeSignContext;

    #[test]
    fn context_preserves_endpoint_and_sanitized_source_semantics() {
        let workspace = tempdir().expect("workspace");
        let repository = workspace.path().join("demo");
        std::fs::create_dir_all(repository.join("nested")).expect("repository");
        let startup = StartupContext {
            api_base_url: "https://api.example.test/".to_owned(),
            use_code_sessions: true,
            sources: vec![Source::GitRepository(GitRepositorySource {
                base: BaseSource {
                    source_type: "git_repository".to_owned(),
                },
                git_info: GitInfo {
                    git_type: "github".to_owned(),
                    repo: "anthropic/demo".to_owned(),
                    git_ref: Some("main".to_owned()),
                    url: Some("https://token@example.test/demo.git".to_owned()),
                    auth: Some(AuthConfig {
                        auth_type: "github".to_owned(),
                        token: "secret-token".to_owned(),
                    }),
                    ..GitInfo::default()
                },
                ..GitRepositorySource::default()
            })],
            ..StartupContext::default()
        };
        let context = CodeSignContext::from_runtime("session-1", &startup, workspace.path());
        assert_eq!(
            context.endpoint().unwrap(),
            "https://api.example.test/v1/code/sessions/session-1/sign-commit"
        );
        assert_eq!(
            context.source_for_repository(&repository.join("nested")),
            Some(json!({
                "type": "git_repository",
                "git_info": {
                    "type": "github",
                    "repo": "anthropic/demo",
                    "ref": "main"
                }
            }))
        );
        let serialized = serde_json::to_string(&context).unwrap();
        for secret in [
            "secret-token",
            "token@example.test",
            "\"auth\"",
            "\"url\"",
            "allow_unrestricted_git_push",
        ] {
            assert!(!serialized.contains(secret), "context leaked {secret}");
        }
        assert!(
            context
                .source_for_repository(Path::new("/unrelated"))
                .is_none()
        );
    }
}
