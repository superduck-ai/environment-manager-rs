//! `/v1/environments/whoami` 身份发现客户端。
//!
//! 该请求在 orchestrator 启动阶段把 service key 解析为 organization/environment
//! 身份。请求头与 work poller 一致，响应类型必须为 `environment_whoami`。

use std::time::Duration;

use reqwest::header::AUTHORIZATION;
use tokio_util::sync::CancellationToken;

use super::{OrchestratorError, OrchestratorResult, WhoamiClient, WhoamiResponse};
use crate::{
    internal::api::read_response_body,
    internal::util::{claude_code_version, version},
};

const ENVIRONMENTS_BETA: &str = "environments-2025-11-01";
const WHOAMI_TIMEOUT: Duration = Duration::from_secs(30);

/// 创建身份发现客户端。
pub fn new_whoami_client(
    api_base_url: impl Into<String>,
    auth_token: impl Into<String>,
) -> WhoamiClient {
    let mut api_base_url = api_base_url.into();
    if !api_base_url.starts_with("http://") && !api_base_url.starts_with("https://") {
        api_base_url = format!("https://{api_base_url}");
    }
    WhoamiClient {
        api_base_url: api_base_url.trim_end_matches('/').to_owned(),
        auth_token: auth_token.into(),
        http_client: reqwest::Client::builder()
            .build()
            .expect("Whoami HTTP client configuration is valid"),
    }
}

impl WhoamiClient {
    /// 获取 service key 对应身份。
    ///
    /// 取消令牌同时覆盖发送与 body 读取，避免只取消到“收到响应头”为止。非 200 响应
    /// 保留 body 文本，便于启动失败时展示服务端解释。
    pub async fn get_identity(
        &self,
        cancellation: &CancellationToken,
    ) -> OrchestratorResult<WhoamiResponse> {
        let url = format!("{}/v1/environments/whoami", self.api_base_url);
        tracing::info!("url" = ?(url), "{}", "calling whoami endpoint");

        let mut request = self
            .http_client
            .get(&url)
            .timeout(WHOAMI_TIMEOUT)
            .header(AUTHORIZATION, format!("Bearer {}", self.auth_token))
            .header("anthropic-beta", ENVIRONMENTS_BETA)
            .header("x-environment-runner-version", version());
        if let Some(claude_version) = claude_code_version() {
            request = request.header("x-claude-code-version", claude_version);
        }
        let request = request.build().map_err(|error| {
            OrchestratorError::message(format!("failed to create whoami request: {error}"))
        })?;

        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(None),
            response = self.http_client.execute(request) => response.map_err(Some),
        };
        let response = match response {
            Ok(response) => response,
            Err(Some(error)) => {
                return Err(OrchestratorError::message(format!(
                    "whoami request failed: {}",
                    error.without_url()
                )));
            }
            Err(None) => {
                return Err(OrchestratorError::message("whoami request canceled"));
            }
        };

        let status = response.status();
        let body = read_response_body(response, cancellation)
            .await
            .map_err(|error| {
                OrchestratorError::message(format!("failed to read whoami response body: {error}"))
            })?;

        if status != reqwest::StatusCode::OK {
            return Err(OrchestratorError::message(format!(
                "whoami request returned status {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body)
            )));
        }

        let response: WhoamiResponse = serde_json::from_slice(&body).map_err(|error| {
            OrchestratorError::message(format!("failed to deserialize whoami response: {error}"))
        })?;
        if response.r#type != "environment_whoami" {
            return Err(OrchestratorError::message(format!(
                "unexpected whoami response type: {}",
                response.r#type
            )));
        }

        tracing::info!("organization_uuid" = ?(response.organization_uuid), "environment_id" = ?(response.environment_id), "{}", "whoami response received");
        Ok(response)
    }
}
