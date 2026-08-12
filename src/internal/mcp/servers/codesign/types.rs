//! code-sign server 的数据类型。
//!
//! wire 字段严格区分本地 [`SignRequest::content`] 与远端
//! [`RemoteSignRequest::contents`]；source 只携带签名 API 所需的仓库标识，绝不复制
//! Git URL、auth 或 unrestricted-push 等敏感/策略字段。

use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::internal::{
    auth::AuthContext,
    config::{Source, StartupContext},
    mcp::BaseServer,
};

/// code-sign MCP 业务 server。
#[derive(Debug)]
pub struct CodeSignMCPServer {
    /// loopback HTTP server 共享实现。
    pub base_server: Arc<BaseServer>,
    /// session ID 和 ingress token 来源。
    pub(crate) auth_context: Option<Arc<AuthContext>>,
    /// source directory 解析基准。
    pub(crate) working_dir: PathBuf,
    /// 远程签名 API 根 URL。
    pub(crate) signing_server_url: String,
    /// sources 和 code-session feature flag。
    pub(crate) startup_context: Option<Arc<StartupContext>>,
}

impl CodeSignMCPServer {
    /// 从已经验证的配置组装 server，不执行网络或文件操作。
    #[must_use]
    pub fn new(
        base_server: Arc<BaseServer>,
        auth_context: Option<Arc<AuthContext>>,
        working_dir: PathBuf,
        signing_server_url: impl Into<String>,
        startup_context: Option<Arc<StartupContext>>,
    ) -> Self {
        Self {
            base_server,
            auth_context,
            working_dir,
            signing_server_url: signing_server_url.into(),
            startup_context,
        }
    }

    /// 暴露规范化后的签名 API 根 URL，供启动诊断和测试读取。
    #[must_use]
    pub fn signing_server_url(&self) -> &str {
        &self.signing_server_url
    }
}

/// 本地签名请求。
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SignRequest {
    /// 待签名文本。
    pub content: String,
    /// 已脱敏 source JSON。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Value>,
    /// Git object format，例如 sha1 或 sha256。
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub git_object_format: String,
}

/// 对业务调用方返回的签名结果。
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SignResponse {
    /// SSH-style Ed25519 signature。
    pub signature: String,
}

/// 文件目录映射到 startup source 后的结果。
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct SourceInfo {
    /// 便于日志读取的 `repo (provider)` 或 `unknown`。
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// 匹配到的原始 source；发送前仍会经过 sanitize。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Source>,
}

/// 签名服务接收的请求；历史 API 使用复数 `contents`。
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RemoteSignRequest {
    /// 待签名文本。
    pub contents: String,
    /// 已脱敏 source JSON。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Value>,
    /// Git object format。
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub git_object_format: String,
}

/// 签名服务的成功响应。
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemoteSignResponse {
    /// SSH-style Ed25519 signature。
    #[serde(default)]
    pub signature: String,
}
