//! code-sign 文件读取、source 脱敏与远程签名操作。
//!
//! 网络请求使用 session ingress Bearer token，发送固定 Anthropic/version headers；
//! 仅等待 response headers 的阶段有 30 秒窗口，读取响应正文和上层调用仍受请求
//! cancellation 控制。任何日志路径都不会输出 ingress token 或 source auth。

use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    config::Source,
    mcp::{McpError, McpResult},
    util::{claude_code_version, version},
};

use super::{
    CodeSignMCPServer, RemoteSignRequest, RemoteSignResponse, SignRequest, SignResponse, SourceInfo,
};

/// 空内容错误的稳定 文案。
pub const ERR_EMPTY_CONTENT: &str = "content to sign cannot be empty";
/// 远程签名失败包装层的稳定 文案。
pub const ERR_SIGNING_FAILED: &str = "signing operation failed";

impl CodeSignMCPServer {
    /// 调用远程 signing API 并验证其成功响应。
    pub async fn execute_sign(
        &self,
        cancellation: &CancellationToken,
        request: &SignRequest,
    ) -> McpResult<SignResponse> {
        tracing::info!("content_length" = ?(request.content.len()), "has_source" = ?(request.source.is_some()), "signing_server_url" = ?(&self.signing_server_url), "working_dir" = ?(self.working_dir.to_string_lossy()), "{}", "Executing sign operation via remote server");
        let session_id = self
            .auth_context
            .as_deref()
            .map(|context| context.get_session_id())
            .unwrap_or_default();
        if session_id.is_empty() {
            return Err(McpError::message(
                "session ID is required for code signing API",
            ));
        }
        let session_ingress_token = self
            .auth_context
            .as_deref()
            .map(|context| context.get_session_ingress_token())
            .unwrap_or_default();
        if session_ingress_token.is_empty() {
            return Err(McpError::message(
                "session ingress token is required for authentication",
            ));
        }

        let endpoint = if self
            .startup_context
            .as_deref()
            .is_some_and(|context| context.use_code_sessions)
        {
            format!(
                "{}/v1/code/sessions/{session_id}/sign-commit",
                self.signing_server_url
            )
        } else {
            format!(
                "{}/v1/session_ingress/sources/sign-commit/{session_id}",
                self.signing_server_url
            )
        };
        let remote_request = RemoteSignRequest {
            contents: request.content.clone(),
            source: request.source.clone(),
            git_object_format: request.git_object_format.clone(),
        };

        let client = reqwest::Client::new();
        let mut builder = client
            .post(&endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("anthropic-version", "2023-06-01")
            .header("x-environment-runner-version", version())
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {session_ingress_token}"),
            )
            .json(&remote_request);
        if let Some(claude_version) = claude_code_version().filter(|value| !value.is_empty()) {
            builder = builder.header("x-claude-code-version", claude_version);
        }

        let response = tokio::select! {
            _ = cancellation.cancelled() => {
                Err("context canceled".to_owned())
            }
            result = tokio::time::timeout(Duration::from_secs(30), builder.send()) => {
                match result {
                    Ok(Ok(response)) => Ok(response),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(_) => Err("response header timeout".to_owned()),
                }
            }
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                tracing::error!("error" = ?(&error), "url" = ?(&endpoint), "{}", "Failed to call remote signing server");
                return Err(McpError::message(format!(
                    "failed to call signing server: {error}"
                )));
            }
        };
        let status = response.status();
        let response_body = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(McpError::message("failed to read response: context canceled"));
            }
            result = response.bytes() => result.map_err(|error| {
                McpError::message(format!("failed to read response: {error}"))
            })?,
        };
        if status != reqwest::StatusCode::OK {
            tracing::error!("status_code" = ?(status.as_u16()), "response_body" = ?(String::from_utf8_lossy(&response_body)), "{}", "Remote signing server returned error");
            return Err(McpError::message(format!(
                "signing server returned status {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&response_body)
            )));
        }

        let remote_response: RemoteSignResponse = match serde_json::from_slice(&response_body) {
            Ok(response) => response,
            Err(error) => {
                tracing::error!("error" = ?(error.to_string()), "response_body" = ?(String::from_utf8_lossy(&response_body)), "{}", "Failed to parse signing server response");
                return Err(McpError::message(format!(
                    "failed to parse response: {error}"
                )));
            }
        };
        if remote_response.signature.is_empty() {
            return Err(McpError::message("no signature returned from server"));
        }
        tracing::info!("signature_length" = ?(remote_response.signature.len()), "{}", "Successfully received signature from remote server");
        Ok(SignResponse {
            signature: remote_response.signature,
        })
    }

    /// 验证非空内容、脱敏 source 并执行远程签名。
    pub async fn sign_content(
        &self,
        cancellation: &CancellationToken,
        content: String,
        source_info: &SourceInfo,
        git_object_format: String,
    ) -> McpResult<String> {
        let source_type = source_info.source.as_ref().map_or("", Source::get_type);
        tracing::info!("source_name" = ?(&source_info.name), "source_type" = ?(source_type), "content_length" = ?(content.len()), "{}", "Signing content for source");
        if content.is_empty() {
            return Err(McpError::message(ERR_EMPTY_CONTENT));
        }
        let request = SignRequest {
            content,
            source: source_info.source.as_ref().map(sanitize_source),
            git_object_format,
        };
        self.execute_sign(cancellation, &request)
            .await
            .map(|response| response.signature)
            .map_err(|error| McpError::message(format!("{ERR_SIGNING_FAILED}: {error}")))
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

/// 读取完整文件内容；无效 UTF-8 按替换字符语义处理。
pub async fn read_file_content(path: &std::path::Path) -> McpResult<String> {
    let bytes = tokio::fs::read(path).await.map_err(|error| {
        McpError::message(format!("cannot open file {}: {error}", path.display()))
    })?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}
