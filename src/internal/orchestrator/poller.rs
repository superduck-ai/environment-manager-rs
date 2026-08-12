//! 后端 `/work/poll` 的非阻塞 HTTP poller。
//!
//! 每次请求独立设置 30 秒超时，并复用 Reqwest 连接池。认证、beta、runner 版本、
//! Claude Code 版本和 worker header 与 Go 版本保持一致。

use std::time::{Duration, Instant};

use async_trait::async_trait;
use rand::Rng;
use reqwest::header::AUTHORIZATION;
use tokio_util::sync::CancellationToken;

use super::{
    OrchestratorError, OrchestratorResult, Poller, PollerInterface, RawMessage, WorkResponse,
};
use crate::internal::util::{claude_code_version, duration_nanoseconds, version};

const ENVIRONMENTS_BETA: &str = "environments-2025-11-01";
const POLL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// 对应 Go `NewPollerWithWorkerID`，创建后端 HTTP poller。
///
/// base URL 没有 scheme 时补 `https://`。空 worker ID 在 Linux 上优先读取 kernel
/// hostname，随后回退 `HOSTNAME`；读取失败仍允许不发送 worker header。
pub fn new_poller_with_worker_id(
    api_base_url: impl Into<String>,
    environment_id: impl Into<String>,
    auth_token: impl Into<String>,
    worker_id: impl Into<String>,
    reclaim_older_than_ms: i64,
) -> Poller {
    let mut api_base_url = api_base_url.into();
    if !api_base_url.starts_with("http://") && !api_base_url.starts_with("https://") {
        api_base_url = format!("https://{api_base_url}");
    }
    let api_base_url = api_base_url.trim_end_matches('/').to_owned();
    let environment_id = environment_id.into();
    let mut worker_id = worker_id.into();
    if worker_id.is_empty() {
        worker_id = local_hostname().unwrap_or_default();
    }

    Poller {
        api_base_url,
        environment_id,
        auth_token: auth_token.into(),
        worker_id,
        reclaim_older_than_ms,
        http_client: reqwest::Client::new(),
    }
}

impl Poller {
    /// 对应 Go `(*Poller).Poll`，非阻塞查询一次 work 队列。
    ///
    /// HTTP 200 的空 body 或精确 `null` 返回 `None`。非空响应先保留原始字节，再解析
    /// envelope 验证 `type == "work"`，成功时原样返回，避免反序列化/重序列化改变 secret。
    pub async fn poll(
        &self,
        cancellation: &CancellationToken,
    ) -> OrchestratorResult<Option<RawMessage>> {
        tracing::debug!(
            environment_id = %self.environment_id,
            worker_id = %self.worker_id,
            "sending poll request (non-blocking)"
        );

        let mut poll_url = format!(
            "{}/v1/environments/{}/work/poll",
            self.api_base_url, self.environment_id
        );
        if self.reclaim_older_than_ms > 0 {
            poll_url.push_str(&format!(
                "?reclaim_older_than_ms={}",
                self.reclaim_older_than_ms
            ));
        }

        let mut request = self
            .http_client
            .get(&poll_url)
            .timeout(POLL_REQUEST_TIMEOUT)
            .header(AUTHORIZATION, format!("Bearer {}", self.auth_token))
            .header("anthropic-beta", ENVIRONMENTS_BETA)
            .header("x-environment-runner-version", version());
        if let Some(claude_version) = claude_code_version() {
            request = request.header("x-claude-code-version", claude_version);
        }
        if !self.worker_id.is_empty() {
            request = request.header("Anthropic-Worker-ID", &self.worker_id);
        }
        let request = request.build().map_err(|error| {
            OrchestratorError::message(format!("failed to create poll request: {error}"))
        })?;

        let started = Instant::now();
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(OrchestratorError::message(
                    "poll request failed: context canceled",
                ));
            }
            response = self.http_client.execute(request) => response,
        };
        let poll_duration = started.elapsed();
        let response = response.map_err(|error| {
            tracing::error!("duration" = ?(duration_nanoseconds(poll_duration)), "error" = ?(error.to_string()), "{}", "poll request failed");
            OrchestratorError::message(format!("poll request failed: {error}"))
        })?;

        let status = response.status();
        let request_id = response
            .headers()
            .get("request-id")
            .or_else(|| response.headers().get("x-request-id"))
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(OrchestratorError::message(
                    "failed to read poll response body: context canceled",
                ));
            }
            body = response.bytes() => body,
        }
        .map_err(|error| {
            OrchestratorError::message(format!("failed to read poll response body: {error}"))
        })?;

        if status == reqwest::StatusCode::OK && (body.is_empty() || body.as_ref() == b"null") {
            tracing::debug!("poll_duration" = ?(duration_nanoseconds(poll_duration)), "request_id" = ?(request_id), "{}", "queue empty");
            return Ok(None);
        }

        if status != reqwest::StatusCode::OK {
            tracing::error!("status_code" = ?(status.as_u16()), "duration" = ?(duration_nanoseconds(poll_duration)), "request_id" = ?(request_id), "response_body" = ?(String::from_utf8_lossy(&body)), "{}", "poll request returned non-OK status");
            return Err(OrchestratorError::message(format!(
                "poll request returned status {} (request-id: {request_id})",
                status.as_u16()
            )));
        }

        let work_response: WorkResponse = serde_json::from_slice(&body).map_err(|error| {
            OrchestratorError::message(format!("failed to unmarshal work response: {error}"))
        })?;
        if work_response.r#type != "work" {
            tracing::warn!("poll_duration" = ?(duration_nanoseconds(poll_duration)), "type" = ?(work_response.r#type), "{}", "received unexpected response type");
            return Err(OrchestratorError::message(format!(
                "unexpected response type: {}",
                work_response.r#type
            )));
        }

        tracing::info!("poll_duration" = ?(duration_nanoseconds(poll_duration)), "request_id" = ?(request_id), "work_id" = ?(work_response.id), "work_type" = ?(work_response.data.r#type), "session_id" = ?(work_response.data.id), "{}", "received work task");
        Ok(Some(body.to_vec()))
    }

    /// 对应 Go `(*Poller).SleepWithJitter`，空队列后随机等待 1–3 秒。
    pub async fn sleep_with_jitter(
        &self,
        cancellation: &CancellationToken,
    ) -> OrchestratorResult<()> {
        let seconds = {
            let mut rng = rand::rng();
            rng.random_range(1_u64..=3)
        };
        let sleep_duration = Duration::from_secs(seconds);
        tracing::debug!("sleep_duration" = ?(duration_nanoseconds(sleep_duration)), "{}", "sleeping with jitter after empty queue");
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(OrchestratorError::message(
                "jitter sleep interrupted: context canceled",
            )),
            _ = tokio::time::sleep(sleep_duration) => Ok(()),
        }
    }
}

#[async_trait]
impl PollerInterface for Poller {
    /// 通过统一 poller trait 转发到 HTTP Poller 的固有实现。
    async fn poll(
        &self,
        cancellation: &CancellationToken,
    ) -> OrchestratorResult<Option<RawMessage>> {
        Poller::poll(self, cancellation).await
    }

    /// 通过统一 poller trait 转发可取消 jitter。
    async fn sleep_with_jitter(&self, cancellation: &CancellationToken) -> OrchestratorResult<()> {
        Poller::sleep_with_jitter(self, cancellation).await
    }
}

/// 读取 Linux kernel hostname；环境变量只作为跨平台测试回退。
fn local_hostname() -> Option<String> {
    ["/proc/sys/kernel/hostname", "/etc/hostname"]
        .into_iter()
        .find_map(|path| std::fs::read_to_string(path).ok())
        .map(|hostname| hostname.trim().to_owned())
        .filter(|hostname| !hostname.is_empty())
        .or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .filter(|value| !value.is_empty())
        })
}
