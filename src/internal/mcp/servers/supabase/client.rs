//! Supabase Management API client。
//!
//! 路径、JSON keys、Bearer 行为和错误包装构成稳定的 Management API 边界。响应最多读取
//! 10 MiB，并到达上限时静默截断，而不是额外报告 oversized。

use futures_util::StreamExt as _;
use reqwest::Method;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::internal::mcp::{McpError, McpResult};

use super::{Client, MigrationResponse, QueryErrorResponse, TypesResponse};

/// 生产 Management API 根 URL。
pub const DEFAULT_MANAGEMENT_API_BASE_URL: &str = "https://api.supabase.com/v1";
/// 普通 API 响应的 -compatible 读取上限。
pub const MANAGEMENT_API_RESPONSE_LIMIT: usize = 10 * 1024 * 1024;

impl Client {
    /// POST 一条 SQL migration。
    pub async fn apply_migration(
        &self,
        cancellation: &CancellationToken,
        name: &str,
        sql: &str,
    ) -> McpResult<()> {
        let body = serde_json::to_string(&json!({"name": name, "query": sql}))
            .map_err(|error| McpError::message(format!("serialize migration: {error}")))?;
        let path = format!("/projects/{}/database/migrations", self.project_ref);
        self.do_request(cancellation, Method::POST, &path, &body)
            .await
            .map_err(|error| McpError::message(format!("apply migration: {error}")))?;
        Ok(())
    }

    /// POST 任意 SQL query，并原样返回 API JSON 文本。
    pub async fn run_query(
        &self,
        cancellation: &CancellationToken,
        sql: &str,
    ) -> McpResult<String> {
        let body = serde_json::to_string(&json!({"query": sql}))
            .map_err(|error| McpError::message(format!("serialize query: {error}")))?;
        let path = format!("/projects/{}/database/query", self.project_ref);
        self.do_request(cancellation, Method::POST, &path, &body)
            .await
            .map_err(|error| McpError::message(format!("run query: {error}")))
    }

    /// GET 并解析已应用 migration 列表。
    pub async fn list_migrations(
        &self,
        cancellation: &CancellationToken,
    ) -> McpResult<Vec<MigrationResponse>> {
        let path = format!("/projects/{}/database/migrations", self.project_ref);
        let body = self
            .do_request(cancellation, Method::GET, &path, "")
            .await
            .map_err(|error| McpError::message(format!("list migrations: {error}")))?;
        serde_json::from_str::<Vec<MigrationResponse>>(&body)
            .map_err(|error| McpError::message(format!("parse migrations: {error}")))
    }

    /// GET 当前 schema 的 TypeScript types。
    pub async fn generate_types(&self, cancellation: &CancellationToken) -> McpResult<String> {
        let path = format!("/projects/{}/types/typescript", self.project_ref);
        let body = self
            .do_request(cancellation, Method::GET, &path, "")
            .await
            .map_err(|error| McpError::message(format!("generate types: {error}")))?;
        let response: TypesResponse = serde_json::from_str(&body)
            .map_err(|error| McpError::message(format!("parse types response: {error}")))?;
        Ok(response.types)
    }

    /// 发送单个 Management API 请求并应用统一错误策略。
    pub async fn do_request(
        &self,
        cancellation: &CancellationToken,
        method: Method,
        path: &str,
        body: &str,
    ) -> McpResult<String> {
        let url = format!("{}{path}", self.base_url);
        let mut request = self.http_client.request(method, &url).header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", self.pat),
        );
        if !body.is_empty() {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_owned());
        }

        let response = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(McpError::message("http request: operation cancelled"));
            }
            result = request.send() => result.map_err(|error| {
                McpError::message(format!("http request: {error}"))
            })?,
        };
        let status = response.status();
        let bytes = read_limited_response(cancellation, response, MANAGEMENT_API_RESPONSE_LIMIT)
            .await
            .map_err(|error| McpError::message(format!("read response: {error}")))?;
        let text = String::from_utf8_lossy(&bytes).into_owned();
        if status.as_u16() >= 400
            && let Ok(error_response) = serde_json::from_slice::<QueryErrorResponse>(&bytes)
            && !error_response.message.is_empty()
        {
            return Err(McpError::message(format!(
                "API error {}: {}",
                status.as_u16(),
                error_response.message
            )));
        }
        if status.as_u16() >= 400 {
            return Err(McpError::message(format!(
                "API error {}: {text}",
                status.as_u16()
            )));
        }
        Ok(text)
    }
}

/// 从 response stream 最多复制 limit 字节，达到 limit 后立即停止读取。
pub(crate) async fn read_limited_response(
    cancellation: &CancellationToken,
    response: reqwest::Response,
    limit: usize,
) -> McpResult<Vec<u8>> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while body.len() < limit {
        let next = tokio::select! {
            _ = cancellation.cancelled() => return Err(McpError::message("operation cancelled")),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(|error| McpError::message(error.to_string()))?;
        let remaining = limit - body.len();
        body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    Ok(body)
}
